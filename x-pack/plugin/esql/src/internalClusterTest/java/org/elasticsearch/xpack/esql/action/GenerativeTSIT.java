/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.Mapping;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.TemplateGenerator;
import org.elasticsearch.datageneration.fields.PredefinedField;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class GenerativeTSIT extends AbstractEsqlIntegTestCase {

    private static final Long NUM_DOCS = 1000L;
    private static final String DATASTREAM_NAME = "tsit_ds";
    private List<XContentBuilder> documents = null;
    private DataGenerationHelper dataGenerationHelper;

    private static final class DataGenerationHelper {

        private static Object randomDimensionValue(String dimensionName) {
            // We use dimensionName to determine the type of the value.
            var isNumeric = dimensionName.hashCode() % 5 == 0;
            if (isNumeric) {
                // Numeric values are sometimes passed as integers and sometimes as strings.
                return ESTestCase.randomBoolean()
                    ? ESTestCase.randomIntBetween(1, 1000)
                    : Integer.toString(ESTestCase.randomIntBetween(1, 1000));
            } else {
                return ESTestCase.randomAlphaOfLengthBetween(1, 20);
            }
        }

        DataGenerationHelper(long numDocs) {
            // Metrics coming into our system have a pre-set group of attributes.
            // Making a list-to-set-to-list to ensure uniqueness.
            this.numDocs = numDocs;
            attributesForMetrics = List.copyOf(
                Set.copyOf(ESTestCase.randomList(1, 300, () -> ESTestCase.randomAlphaOfLengthBetween(2, 30)))
            );
            numTimeSeries = ESTestCase.randomIntBetween(10, (int) Math.sqrt(numDocs));
            // System.out.println("Total of time series: " + numTimeSeries);
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
                    ),
                    Map.of(
                        "counter_double",
                        Map.of("path_match", "metrics.counter_*", "mapping", Map.of("type", "double", "time_series_metric", "counter"))
                    ),
                    Map.of(
                        "gauge_double",
                        Map.of("path_match", "metrics.gauge_*", "mapping", Map.of("type", "double", "time_series_metric", "gauge"))
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

    Map<List<String>, List<Map<String, Object>>> groupedRows(
        List<XContentBuilder> docs,
        List<String> groupingAttributes,
        int secondsInWindow
    ) {
        Map<List<String>, List<Map<String, Object>>> groupedMap = new HashMap<>();
        for (XContentBuilder doc : docs) {
            Map<String, Object> docMap = XContentHelper.convertToMap(BytesReference.bytes(doc), false, XContentType.JSON).v2();
            @SuppressWarnings("unchecked")
            List<String> groupingPairs = groupingAttributes.stream()
                .map(
                    attr -> Tuple.tuple(
                        attr,
                        ((Map<String, Object>) docMap.getOrDefault("attributes", Map.of())).getOrDefault(attr, "").toString()
                    )
                )
                .filter(val -> val.v2().isEmpty() == false) // Filter out empty values
                .map(tup -> tup.v1() + ":" + tup.v2())
                .toList();
            long timeBucketStart = windowStart(docMap.get("@timestamp"), secondsInWindow);
            var keyList = new ArrayList<>(groupingPairs);
            keyList.add(Long.toString(timeBucketStart));
            groupedMap.computeIfAbsent(keyList, k -> new ArrayList<>()).add(docMap);
        }
        return groupedMap;
    }

    static Long windowStart(Object timestampCell, int secondsInWindow) {
        // The timestamp is in the 4th column (index 3)
        return Instant.parse((String) timestampCell).toEpochMilli() / 1000 / secondsInWindow * secondsInWindow;
    }

    static List<String> getRowKey(List<Object> row, List<String> groupingAttributes) {
        List<String> rowKey = new ArrayList<>();
        for (int i = 0; i < groupingAttributes.size(); i++) {
            Object value = row.get(i + 4); // Skip the first four columns
            if (value != null) {
                rowKey.add(groupingAttributes.get(i) + ":" + value);
            }
        }
        rowKey.add(Long.toString(Instant.parse((String) row.get(3)).toEpochMilli() / 1000));
        return rowKey;
    }

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request) {
        assumeTrue("time series available in snapshot builds only", Build.current().isSnapshot());
        return super.run(request);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            DataStreamsPlugin.class,
            LocalStateCompositeXPackPlugin.class,
            // Downsample.class, // TODO(pabloem): What are these
            AggregateMetricMapperPlugin.class,
            EsqlPlugin.class
        );
    }

    void putTSDBIndexTemplate(List<String> patterns, @Nullable String mappingString) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        // Ensure it will be a TSDB data stream
        settingsBuilder.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES);
        settingsBuilder.putList("index.routing_path", List.of("attributes.*"));
        CompressedXContent mappings = mappingString == null ? null : CompressedXContent.fromJSON(mappingString);
        // print the mapping
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            GenerativeTSIT.DATASTREAM_NAME
        );
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(org.elasticsearch.cluster.metadata.Template.builder().settings(settingsBuilder).mappings(mappings))
                .metadata(null)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
    }

    @Before
    public void populateIndex() throws IOException {
        dataGenerationHelper = new DataGenerationHelper(NUM_DOCS);
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.map(dataGenerationHelper.mapping.raw());
        final String jsonMappings = Strings.toString(builder);

        putTSDBIndexTemplate(List.of(DATASTREAM_NAME + "*"), jsonMappings);
        // Now we can push data into the data stream.
        for (int i = 0; i < NUM_DOCS; i++) {
            var document = dataGenerationHelper.generateDocument(Map.of());
            if (documents == null) {
                documents = new ArrayList<>();
            }
            documents.add(document);
            var indexRequest = client().prepareIndex(DATASTREAM_NAME).setOpType(DocWriteRequest.OpType.CREATE).setSource(document);
            indexRequest.setRefreshPolicy(org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE);
            indexRequest.get();
        }
    }

    public void testGroupBySubset() {
        var dimensions = ESTestCase.randomNonEmptySubsetOf(dataGenerationHelper.attributesForMetrics);
        var dimensionsStr = dimensions.stream().map(d -> "attributes." + d).collect(Collectors.joining(", "));
        try (var resp = run(String.format(Locale.ROOT, """
            TS %s
            | STATS max(max_over_time(metrics.gauge_hdd.bytes.used)),
                min(min_over_time(metrics.gauge_hdd.bytes.used)),
                avg(avg_over_time(metrics.gauge_hdd.bytes.used))
                BY tbucket=bucket(@timestamp, 1 minute), %s
            | SORT tbucket
            | LIMIT 1000""", DATASTREAM_NAME, dimensionsStr))) {
            var groups = groupedRows(documents, dimensions, 60);
            List<List<Object>> rows = new ArrayList<>();
            resp.rows().forEach(rowIter -> {
                List<Object> row = new ArrayList<>();
                rowIter.forEach(row::add);
                rows.add(row);
            });
            for (List<Object> row : rows) {
                var rowKey = getRowKey(row, dimensions);
                var pointsInGroup = groups.get(rowKey);
                @SuppressWarnings("unchecked")
                var docValues = pointsInGroup.stream()
                    .map(doc -> ((Map<String, Integer>) doc.get("metrics")).get("gauge_hdd.bytes.used"))
                    .toList();
                docValues.stream().max(Integer::compareTo).ifPresentOrElse(maxValue -> {
                    var res = ((Long) row.getFirst()).intValue();
                    assertThat(res, equalTo(maxValue));
                }, () -> { throw new AssertionError("No values found for group: " + rowKey); });
                docValues.stream().min(Integer::compareTo).ifPresentOrElse(minValue -> {
                    var res = ((Long) row.get(1)).intValue();
                    assertThat(res, equalTo(minValue));
                }, () -> { throw new AssertionError("No values found for group: " + rowKey); });
                docValues.stream().mapToDouble(Integer::doubleValue).average().ifPresentOrElse(avgValue -> {
                    var res = (Double) row.get(2);
                    assertThat(res, closeTo(avgValue, res * 0.5));
                }, () -> { throw new AssertionError("No values found for group: " + rowKey); });
            }
        }
    }

    public void testGroupByNothing() {
        try (var resp = run(String.format(Locale.ROOT, """
            TS %s
            | STATS
                max(max_over_time(metrics.gauge_hdd.bytes.used)),
                avg(avg_over_time(metrics.gauge_hdd.bytes.used)),
                min(min_over_time(metrics.gauge_hdd.bytes.used)) BY tbucket=bucket(@timestamp, 1 minute)
            | SORT tbucket
            | LIMIT 1000""", DATASTREAM_NAME))) {
            List<List<Object>> rows = new ArrayList<>();
            resp.rows().forEach(rowIter -> {
                List<Object> row = new ArrayList<>();
                rowIter.forEach(row::add);
                rows.add(row);
            });
            var groups = groupedRows(documents, List.of(), 60);
            for (List<Object> row : rows) {
                var windowStart = windowStart(row.get(3), 60);
                var windowDataPoints = groups.get(List.of(Long.toString(windowStart)));
                @SuppressWarnings("unchecked")
                var docValues = windowDataPoints.stream()
                    .map(doc -> ((Map<String, Integer>) doc.get("metrics")).get("gauge_hdd.bytes.used"))
                    .toList();
                docValues.stream().max(Integer::compareTo).ifPresentOrElse(maxValue -> {
                    var res = ((Long) row.getFirst()).intValue();
                    assertThat(res, equalTo(maxValue));
                }, () -> { throw new AssertionError("No values found for window starting at " + windowStart); });
                docValues.stream().mapToDouble(Integer::doubleValue).average().ifPresentOrElse(avgValue -> {
                    var res = (Double) row.get(1);
                    assertThat(res, closeTo(avgValue, res * 0.5));
                }, () -> {
                    ;
                    throw new AssertionError("No values found for window starting at " + windowStart);
                });
                docValues.stream().min(Integer::compareTo).ifPresentOrElse(minValue -> {
                    var res = ((Long) row.get(2)).intValue();
                    assertThat(res, equalTo(minValue));
                }, () -> { throw new AssertionError("No values found for window starting at " + windowStart); });
            }
        }
    }
}
