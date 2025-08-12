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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
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
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class RandomizedTimeSeriesIT extends AbstractEsqlIntegTestCase {

    private static final Long NUM_DOCS = 2500L;
    private static final String DATASTREAM_NAME = "tsit_ds";
    private List<XContentBuilder> documents = null;
    private TSDataGenerationHelper dataGenerationHelper;

    List<List<Object>> consumeRows(EsqlQueryResponse resp) {
        List<List<Object>> rows = new ArrayList<>();
        resp.rows().forEach(rowIter -> {
            List<Object> row = new ArrayList<>();
            rowIter.forEach(row::add);
            rows.add(row);
        });
        return rows;
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
        // This calculation looks a little weird, but it simply performs an integer division that
        // throws away the remainder of the division by secondsInWindow. It rounds down
        // the timestamp to the nearest multiple of secondsInWindow.
        var timestampSeconds = Instant.parse((String) timestampCell).toEpochMilli() / 1000;
        return (timestampSeconds / secondsInWindow) * secondsInWindow;
    }

    enum Agg {
        MAX,
        MIN,
        AVG
    }

    static List<Integer> valuesInWindow(List<Map<String, Object>> pointsInGroup, String metricName) {
        @SuppressWarnings("unchecked")
        var values = pointsInGroup.stream()
            .map(doc -> ((Map<String, Integer>) doc.get("metrics")).get(metricName))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        return values;
    }

    static Double aggregateValuesInWindow(List<Integer> values, Agg agg) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("No values to aggregate for " + agg + " operation");
        }
        return switch (agg) {
            case MAX -> Double.valueOf(values.stream().max(Integer::compareTo).orElseThrow());
            case MIN -> Double.valueOf(values.stream().min(Integer::compareTo).orElseThrow());
            case AVG -> values.stream().mapToDouble(Integer::doubleValue).average().orElseThrow();
        };
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
        CompressedXContent mappings = mappingString == null ? null : CompressedXContent.fromJSON(mappingString);
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            RandomizedTimeSeriesIT.DATASTREAM_NAME
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
        dataGenerationHelper = new TSDataGenerationHelper(NUM_DOCS);
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

    /**
     * This test validates Gauge metrics aggregation with grouping by time bucket and a subset of dimensions.
     * The subset of dimensions is a random subset of the dimensions present in the data.
     * The test checks that the max, min, and avg values of the gauge metric - and calculates
     * the same values from the documents in the group.
     */
    public void testGroupBySubset() {
        var dimensions = ESTestCase.randomNonEmptySubsetOf(dataGenerationHelper.attributesForMetrics);
        var dimensionsStr = dimensions.stream().map(d -> "attributes." + d).collect(Collectors.joining(", "));
        try (EsqlQueryResponse resp = run(String.format(Locale.ROOT, """
            TS %s
            | STATS max(max_over_time(metrics.gauge_hdd.bytes.used)),
                min(min_over_time(metrics.gauge_hdd.bytes.used)),
                avg(avg_over_time(metrics.gauge_hdd.bytes.used))
                BY tbucket=bucket(@timestamp, 1 minute), %s
            | SORT tbucket
            | LIMIT 1000""", DATASTREAM_NAME, dimensionsStr))) {
            var groups = groupedRows(documents, dimensions, 60);
            List<List<Object>> rows = consumeRows(resp);
            for (List<Object> row : rows) {
                var rowKey = getRowKey(row, dimensions);
                var docValues = valuesInWindow(groups.get(rowKey), "gauge_hdd.bytes.used");
                // Max of int is always int, so we can safely round the result.
                assertThat(row.getFirst(), equalTo(Math.round(aggregateValuesInWindow(docValues, Agg.MAX))));
                assertThat(row.get(1), equalTo(Math.round(aggregateValuesInWindow(docValues, Agg.MIN))));
                // We check the expected vs ES-calculated average. We divide them to normalize the error
                // and allow for a 20% error margin.
                Double esAvg = (Double) row.get(2);
                Double expectedAvg = aggregateValuesInWindow(docValues, Agg.AVG);
                var ratio = esAvg / expectedAvg;
                assertThat(ratio, closeTo(1, 0.2));

            }
        }
    }

    /**
     * This test validates Gauge metrics aggregation with grouping by time bucket only.
     * The test checks that the max, min, and avg values of the gauge metric - and calculates
     * the same values from the documents in the group. Because there is no grouping by dimensions,
     * there is only one metric group per time bucket.
     */
    public void testGroupByNothing() {
        try (EsqlQueryResponse resp = run(String.format(Locale.ROOT, """
            TS %s
            | STATS
                max(max_over_time(metrics.gauge_hdd.bytes.used)),
                min(min_over_time(metrics.gauge_hdd.bytes.used)),
                avg(avg_over_time(metrics.gauge_hdd.bytes.used)) BY tbucket=bucket(@timestamp, 1 minute)
            | SORT tbucket
            | LIMIT 1000""", DATASTREAM_NAME))) {
            List<List<Object>> rows = consumeRows(resp);
            var groups = groupedRows(documents, List.of(), 60);
            for (List<Object> row : rows) {
                var windowStart = windowStart(row.get(3), 60);
                var docValues = valuesInWindow(groups.get(List.of(Long.toString(windowStart))), "gauge_hdd.bytes.used");
                // Min and Max of int are always int, so we can safely round the result.
                assertThat(row.getFirst(), equalTo(Math.round(aggregateValuesInWindow(docValues, Agg.MAX))));
                assertThat(row.get(1), equalTo(Math.round(aggregateValuesInWindow(docValues, Agg.MIN))));
                // We check the expected vs ES-calculated average. We divide them to normalize the error
                // and allow for a 20% error margin.
                Double esAvg = (Double) row.get(2);
                Double expectedAvg = aggregateValuesInWindow(docValues, Agg.AVG);
                var ratio = esAvg / expectedAvg;
                assertThat(ratio, closeTo(1, 0.2));
            }
        }
    }
}
