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

    TSDataGenerationHelper(long numDocs) {
        // Metrics coming into our system have a pre-set group of attributes.
        // Making a list-to-set-to-list to ensure uniqueness.
        this.numDocs = numDocs;
        var maxAttributes = (int) Math.sqrt(numDocs);
        attributesForMetrics = List.copyOf(
            Set.copyOf(ESTestCase.randomList(1, maxAttributes, () -> ESTestCase.randomAlphaOfLengthBetween(2, 30)))
        );
        var maxTimeSeries = (int) Math.sqrt(numDocs);
        var minTimeSeries = Math.max(1, maxTimeSeries / 4);
        numTimeSeries = ESTestCase.randomIntBetween(minTimeSeries, maxTimeSeries);
        // allTimeSeries contains the list of dimension-values for each time series.
        List<List<Tuple<String, Object>>> allTimeSeries = IntStream.range(0, numTimeSeries).mapToObj(tsIdx -> {
            List<String> dimensionsInMetric = ESTestCase.randomNonEmptySubsetOf(attributesForMetrics);
            // TODO: How do we handle the case when there are no dimensions? (i.e. regular randomSubsetof(...)
            return dimensionsInMetric.stream().map(attr -> new Tuple<>(attr, randomDimensionValue(attr))).collect(Collectors.toList());
        }).toList();

        spec = DataGeneratorSpecification.builder()
            .withMaxFieldCountPerLevel(0)
            .withPredefinedFields(
                List.of(
                    new PredefinedField.WithGenerator(
                        "@timestamp",
                        FieldType.DATE,
                        Map.of("type", "date"),
                        fieldMapping -> ESTestCase.randomInstantBetween(Instant.now().minusSeconds(2 * 60 * 60), Instant.now())
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
                        (ignored) -> Map.of("gauge_hdd.bytes.used", Randomness.get().nextLong(0, 1000000000L))
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
                    Map.of("path_match", "metrics.counter_*", "mapping", Map.of("type", "long", "time_series_metric", "counter"))
                ),
                Map.of(
                    "gauge_long",
                    Map.of("path_match", "metrics.gauge_*", "mapping", Map.of("type", "long", "time_series_metric", "gauge"))
                )
                // TODO: Add double and other metric types
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
