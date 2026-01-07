/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.Mapping;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.TemplateGenerator;
import org.elasticsearch.datageneration.fields.PredefinedField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class TSDataGenerationHelper {

    private static Object randomDimensionValue(String dimensionName) {
        // We use dimensionName to determine the type of the value.
        var isNumeric = dimensionName.hashCode() % 5 == 0;
        var isIP = dimensionName.hashCode() % 5 == 1;
        if (isNumeric) {
            // Numeric values are sometimes passed as integers and sometimes as strings.
            return ESTestCase.randomIntBetween(1, 1000);
        } else if (isIP) {
            // TODO: Make sure the schema ingests this as an IP address.
            return NetworkAddress.format(ESTestCase.randomIp(ESTestCase.randomBoolean()));
        } else {
            return ESTestCase.randomAlphaOfLengthBetween(1, 20);
        }
    }

    TSDataGenerationHelper(long numDocs, long timeRangeSeconds) {
        // Metrics coming into our system have a pre-set group of attributes.
        // Making a list-to-set-to-list to ensure uniqueness.
        this.numDocs = numDocs;
        var maxAttributes = (int) Math.sqrt(numDocs);
        List<String> tempAttributeSet = ESTestCase.randomList(1, maxAttributes, () -> ESTestCase.randomAlphaOfLengthBetween(3, 30));
        var maxTimeSeries = (int) Math.sqrt(numDocs);
        var minTimeSeries = Math.max(1, maxTimeSeries / 4);
        numTimeSeries = ESTestCase.randomIntBetween(minTimeSeries, maxTimeSeries);
        Set<String> usedAttributeNames = new HashSet<>();
        // allTimeSeries contains the list of dimension-values for each time series.
        List<List<Tuple<String, Object>>> allTimeSeries = IntStream.range(0, numTimeSeries).mapToObj(tsIdx -> {
            List<String> dimensionsInMetric = ESTestCase.randomNonEmptySubsetOf(tempAttributeSet);
            // TODO: How do we handle the case when there are no dimensions? (i.e. regular randomSubsetof(...)
            usedAttributeNames.addAll(dimensionsInMetric);
            return dimensionsInMetric.stream().map(attr -> new Tuple<>(attr, randomDimensionValue(attr))).collect(Collectors.toList());
        }).toList();
        attributesForMetrics = List.copyOf(usedAttributeNames);

        // We want to ensure that all documents have different timestamps.
        var timeRangeMs = timeRangeSeconds * 1000;
        var timeRangeEnd = Instant.parse("2025-07-31T10:00:00Z").toEpochMilli();
        var timeRangeStart = timeRangeEnd - timeRangeMs;
        var timestampSet = new HashSet<Instant>();
        var regens = 0;
        for (int i = 0; i < numDocs; i++) {
            // Random timestamps within the last 90 days.
            while (true) {
                var randomIns = Instant.ofEpochMilli(ESTestCase.randomLongBetween(timeRangeStart, timeRangeEnd));
                if (timestampSet.add(randomIns)) {
                    break;
                }
                regens++;
                if (regens > numDocs) {
                    throw new IllegalStateException("Too many collisions when generating timestamps");
                }
            }
        }
        // Timestampset should have exactly numDocs entries - this works as long as the random number generator
        // does not cycle early.
        assert timestampSet.size() == numDocs : "Expected [" + numDocs + "] timestamps but got [" + timestampSet.size() + "]";
        var timestampsIter = timestampSet.iterator();
        spec = DataGeneratorSpecification.builder()
            .withMaxFieldCountPerLevel(0)
            .withPredefinedFields(
                List.of(
                    new PredefinedField.WithGenerator(
                        "@timestamp",
                        FieldType.DATE,
                        Map.of("type", "date"),
                        fieldMapping -> timestampsIter.next()
                    ),
                    new PredefinedField.WithGenerator(
                        "attributes",
                        FieldType.PASSTHROUGH,
                        Map.of("type", "passthrough", "time_series_dimension", true, "dynamic", true, "priority", 1),
                        (ignored) -> {
                            var tsDimensions = ESTestCase.randomFrom(allTimeSeries);
                            return tsDimensions.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2));
                        }
                    ),
                    new PredefinedField.WithGenerator(
                        "metrics",
                        FieldType.PASSTHROUGH,
                        Map.of("type", "passthrough", "dynamic", true, "priority", 10),

                        (ignored) -> {
                            var res = new HashMap<String, Object>();
                            res.put("counterl_hdd.bytes.read", Randomness.get().nextLong(0, 1000L));
                            res.put("gaugel_hdd.bytes.used", Randomness.get().nextLong(0, 1000000L));
                            // Counter metrics
                            switch (ESTestCase.randomIntBetween(0, 1)) {
                                case 0 -> res.put("counterd_kwh.consumed", Randomness.get().nextDouble(0, 1000000));
                                case 1 -> res.put("gauged_cpu.percent", Randomness.get().nextDouble(0, 100));
                            }
                            return res;
                        }
                    )
                )
            )
            .build();

        documentGenerator = new DocumentGenerator(spec);

        template = new TemplateGenerator(spec).generate();
        mapping = new MappingGenerator(spec).generate(template);
        var doc = mapping.raw().get("_doc");
        @SuppressWarnings("unchecked")
        Map<String, Object> docMap = ((Map<String, Object>) doc);
        // Add dynamic templates to the mapping
        docMap.put(
            "dynamic_templates",
            List.of(
                Map.of(
                    "counter_long",
                    Map.of("path_match", "metrics.counterl_*", "mapping", Map.of("type", "long", "time_series_metric", "counter"))
                ),
                Map.of(
                    "counter_double",
                    Map.of("path_match", "metrics.counterd_*", "mapping", Map.of("type", "double", "time_series_metric", "counter"))
                ),
                Map.of(
                    "gauge_long",
                    Map.of("path_match", "metrics.gaugel_*", "mapping", Map.of("type", "long", "time_series_metric", "gauge"))
                ),
                Map.of(
                    "gauge_double",
                    Map.of("path_match", "metrics.gauged_*", "mapping", Map.of("type", "double", "time_series_metric", "gauge"))
                )
            )
        );
    }

    final DataGeneratorSpecification spec;
    final DocumentGenerator documentGenerator;
    final Template template;
    final Mapping mapping;
    final int numTimeSeries;
    final long numDocs;
    final List<String> attributesForMetrics;

    XContentBuilder generateDocument(Map<String, Object> additionalFields) throws IOException {
        var doc = XContentFactory.jsonBuilder();
        var generated = documentGenerator.generate(template, mapping);
        generated.putAll(additionalFields);

        doc.map(generated);
        return doc;
    }
}
