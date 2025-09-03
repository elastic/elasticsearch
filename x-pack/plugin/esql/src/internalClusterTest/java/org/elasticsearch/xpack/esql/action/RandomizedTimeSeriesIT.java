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
import org.elasticsearch.test.ESIntegTestCase;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

@SuppressWarnings("unchecked")
@ESIntegTestCase.ClusterScope(maxNumDataNodes = 1)
public class RandomizedTimeSeriesIT extends AbstractEsqlIntegTestCase {
    private static final Long NUM_DOCS = 500L;
    private static final Long TIME_RANGE_SECONDS = 900L;
    private static final String DATASTREAM_NAME = "tsit_ds";
    private static final Integer SECONDS_IN_WINDOW = 60;
    private static final List<Tuple<String, Integer>> WINDOW_OPTIONS = List.of(
        Tuple.tuple("10 seconds", 10),
        Tuple.tuple("30 seconds", 30),
        Tuple.tuple("1 minute", 60),
        Tuple.tuple("2 minutes", 120),
        Tuple.tuple("3 minutes", 180),
        Tuple.tuple("5 minutes", 300),
        Tuple.tuple("10 minutes", 600),
        Tuple.tuple("30 minutes", 1800),
        Tuple.tuple("1 hour", 3600)
    );

    private List<XContentBuilder> documents;
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
        AVG,
        SUM,
        COUNT
    }

    static List<Integer> valuesInWindow(List<Map<String, Object>> pointsInGroup, String metricName) {
        @SuppressWarnings("unchecked")
        var values = pointsInGroup.stream()
            .map(doc -> ((Map<String, Integer>) doc.get("metrics")).get(metricName))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        return values;
    }

    static Map<String, List<Tuple<String, Tuple<Instant, Double>>>> groupByTimeseries(
        List<Map<String, Object>> pointsInGroup,
        String metricName
    ) {
        return pointsInGroup.stream()
            .filter(doc -> doc.containsKey("metrics") && ((Map<String, Object>) doc.get("metrics")).containsKey(metricName))
            .map(doc -> {
                String docKey = ((Map<String, Object>) doc.get("attributes")).entrySet()
                    .stream()
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(","));
                var docTs = Instant.parse((String) doc.get("@timestamp"));
                var docValue = switch (((Map<String, Object>) doc.get("metrics")).get(metricName)) {
                    case Integer i -> i.doubleValue();
                    case Long l -> l.doubleValue();
                    case Float f -> f.doubleValue();
                    case Double d -> d;
                    default -> throw new IllegalStateException(
                        "Unexpected value type: "
                            + ((Map<String, Object>) doc.get("metrics")).get(metricName)
                            + " of class "
                            + ((Map<String, Object>) doc.get("metrics")).get(metricName).getClass()
                    );
                };
                return new Tuple<>(docKey, new Tuple<>(docTs, docValue));
            })
            .collect(Collectors.groupingBy(Tuple::v1));
    }

    static Object aggregatePerTimeseries(
        Map<String, List<Tuple<String, Tuple<Instant, Double>>>> timeseries,
        Agg crossAgg,
        Agg timeseriesAgg
    ) {
        var res = timeseries.values().stream().map(timeseriesList -> {
            List<Double> values = timeseriesList.stream().map(t -> t.v2().v2()).collect(Collectors.toList());
            return aggregateValuesInWindow(values, timeseriesAgg);
        }).filter(Objects::nonNull).toList();

        if (res.isEmpty() && timeseriesAgg == Agg.COUNT) {
            res = List.of(0.0);
        }

        return switch (crossAgg) {
            case MAX -> res.isEmpty() ? null : res.stream().mapToDouble(Double::doubleValue).max().orElseThrow();
            case MIN -> res.isEmpty() ? null : res.stream().mapToDouble(Double::doubleValue).min().orElseThrow();
            case AVG -> res.isEmpty() ? null : res.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
            case SUM -> res.isEmpty() ? null : res.stream().mapToDouble(Double::doubleValue).sum();
            case COUNT -> Integer.toUnsignedLong(res.size());
        };
    }

    static Double aggregateValuesInWindow(List<Double> values, Agg agg) {
        return switch (agg) {
            case MAX -> values.stream().max(Double::compareTo).orElseThrow();
            case MIN -> values.stream().min(Double::compareTo).orElseThrow();
            case AVG -> values.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
            case SUM -> values.isEmpty() ? null : values.stream().mapToDouble(Double::doubleValue).sum();
            case COUNT -> (double) values.size();
        };
    }

    static List<String> getRowKey(List<Object> row, List<String> groupingAttributes, int timestampIndex) {
        List<String> rowKey = new ArrayList<>();
        for (int i = 0; i < groupingAttributes.size(); i++) {
            Object value = row.get(i + timestampIndex + 1);
            if (value != null) {
                rowKey.add(groupingAttributes.get(i) + ":" + value);
            }
        }
        rowKey.add(Long.toString(Instant.parse((String) row.get(timestampIndex)).toEpochMilli() / 1000));
        return rowKey;
    }

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request) {
        assumeTrue("time series available in snapshot builds only", Build.current().isSnapshot());
        return super.run(request);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, LocalStateCompositeXPackPlugin.class, AggregateMetricMapperPlugin.class, EsqlPlugin.class);
    }

    record RateRange(Double lower, Double upper) implements Comparable<RateRange> {
        @Override
        public int compareTo(RateRange o) {
            // Compare first by lower bound, then by upper bound
            int cmp = this.lower.compareTo(o.lower);
            if (cmp == 0) {
                return this.upper.compareTo(o.upper);
            }
            return cmp;
        }

        public int compareToFindingMax(RateRange o) {
            // Compare first by upper bound, then by lower bound
            int cmp = this.upper.compareTo(o.upper);
            if (cmp == 0) {
                return this.lower.compareTo(o.lower);
            }
            return cmp;
        }
    }

    // A record that holds min, max, avg, count and sum of rates calculated from a timeseries.
    record RateStats(Long count, RateRange max, RateRange avg, RateRange min, RateRange sum) {}

    static RateStats calculateRateAggregation(
        Collection<List<Tuple<String, Tuple<Instant, Double>>>> allTimeseries,
        Integer secondsInWindow
    ) {
        List<RateRange> allRates = allTimeseries.stream().map(timeseries -> {
            if (timeseries.size() < 2) {
                return null;
            }
            // Sort the timeseries by timestamp
            timeseries.sort((t1, t2) -> t1.v2().v1().compareTo(t2.v2().v1()));
            var firstTs = timeseries.getFirst().v2().v1();
            var lastTs = timeseries.getLast().v2().v1();
            Double lastValue = null;
            Double counterGrowth = 0.0;
            for (Tuple<String, Tuple<Instant, Double>> point : timeseries) {
                var currentValue = point.v2().v2();
                if (currentValue == null) {
                    throw new IllegalArgumentException("Null value in counter timeseries");
                }
                if (lastValue == null) {
                    lastValue = point.v2().v2(); // Initialize with the first value
                    continue;
                }
                if (currentValue > lastValue) {
                    counterGrowth += currentValue - lastValue; // Incremental growth
                } else if (currentValue < lastValue) {
                    // If the value decreased, we assume a reset and start counting from the current value
                    counterGrowth += currentValue;
                }
                lastValue = currentValue; // Update last value for next iteration
            }
            return new RateRange(
                counterGrowth / secondsInWindow * 0.99, // Add 1% tolerance to the lower bound
                1000.0 * counterGrowth / (lastTs.toEpochMilli() - firstTs.toEpochMilli()) * 1.01 // Add 1% tolerance to the upper bound
            );
        }).filter(Objects::nonNull).toList();
        if (allRates.isEmpty()) {
            return new RateStats(0L, null, null, null, null);
        }
        return new RateStats(
            (long) allRates.size(),
            allRates.stream().max(RateRange::compareToFindingMax).orElseThrow(),
            new RateRange(
                allRates.stream().mapToDouble(r -> r.lower).average().orElseThrow(),
                allRates.stream().mapToDouble(r -> r.upper).average().orElseThrow()
            ),
            allRates.stream().min(RateRange::compareTo).orElseThrow(),
            new RateRange(allRates.stream().mapToDouble(r -> r.lower).sum(), allRates.stream().mapToDouble(r -> r.upper).sum())
        );
    }

    void putTSDBIndexTemplate(List<String> patterns, @Nullable String mappingString) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        // Ensure it will be a TSDB data stream
        settingsBuilder.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES);
        settingsBuilder.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2025-07-31T00:00:00Z");
        settingsBuilder.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-07-31T12:00:00Z");
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
        dataGenerationHelper = new TSDataGenerationHelper(NUM_DOCS, TIME_RANGE_SECONDS);
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
            var indexRequest = client().prepareIndex(DATASTREAM_NAME).setOpType(DocWriteRequest.OpType.CREATE).setSource(document);
            indexRequest.setRefreshPolicy(org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE);
            indexRequest.get();
            documents.add(document);
        }
    }

    void checkWithin(Double actual, RateRange expected) {
        if (expected == null) {
            assertThat(actual, equalTo(null));
            return;
        }
        assertThat(actual, allOf(lessThanOrEqualTo(expected.upper), not(lessThan(expected.lower))));
    }

    void assertNoFailedWindows(List<String> failedWindows, List<List<Object>> rows) {
        if (failedWindows.isEmpty() == false) {
            var pctFailures = (double) failedWindows.size() / rows.size() * 100;
            var failureDetails = String.join("\n", failedWindows);
            if (failureDetails.length() > 2000) {
                failureDetails = failureDetails.substring(0, 2000) + "\n... (truncated)";
            }
            throw new AssertionError("Failed " + failedWindows.size() + " windows(" + pctFailures + "%):\n" + failureDetails);
        }
    }

    /**
     * This test validates Rate metrics aggregation with grouping by time bucket and a subset of dimensions.
     * The subset of dimensions is a random subset of the dimensions present in the data.
     * The test checks that the count, max, min, and avg values of the rate metric - and calculates
     * the same values from the documents in the group.
     */
    public void testRateGroupBySubset() {
        var window = ESTestCase.randomFrom(WINDOW_OPTIONS);
        var windowSize = window.v2();
        var windowStr = window.v1();
        var dimensions = ESTestCase.randomSubsetOf(dataGenerationHelper.attributesForMetrics);
        var dimensionsStr = dimensions.isEmpty()
            ? ""
            : ", " + dimensions.stream().map(d -> "attributes." + d).collect(Collectors.joining(", "));
        try (var resp = run(String.format(Locale.ROOT, """
            TS %s
            | STATS count(rate(metrics.counterl_hdd.bytes.read)),
                    max(rate(metrics.counterl_hdd.bytes.read)),
                    avg(rate(metrics.counterl_hdd.bytes.read)),
                    min(rate(metrics.counterl_hdd.bytes.read)),
                    sum(rate(metrics.counterl_hdd.bytes.read))
                BY tbucket=bucket(@timestamp, %s) %s
            | SORT tbucket
            | LIMIT 1000
            """, DATASTREAM_NAME, windowStr, dimensionsStr))) {
            List<List<Object>> rows = consumeRows(resp);
            List<String> failedWindows = new ArrayList<>();
            var groups = groupedRows(documents, dimensions, windowSize);
            for (List<Object> row : rows) {
                var rowKey = getRowKey(row, dimensions, 5);
                var windowDataPoints = groups.get(rowKey);
                var docsPerTimeseries = groupByTimeseries(windowDataPoints, "counterl_hdd.bytes.read");
                var rateAgg = calculateRateAggregation(docsPerTimeseries.values(), windowSize);
                try {
                    assertThat(row.getFirst(), equalTo(rateAgg.count));
                    checkWithin((Double) row.get(1), rateAgg.max);
                    checkWithin((Double) row.get(2), rateAgg.avg);
                    checkWithin((Double) row.get(3), rateAgg.min);
                    checkWithin((Double) row.get(4), rateAgg.sum);
                } catch (AssertionError e) {
                    failedWindows.add("Failed for row:\n" + row + "\nWanted: " + rateAgg + "\nException: " + e.getMessage());
                }
            }
            assertNoFailedWindows(failedWindows, rows);
        }
    }

    /**
     * This test validates Rate metrics aggregation with grouping by time bucket only.
     * The test checks that the count, max, min, and avg values of the rate metric - and calculates
     * the same values from the documents in the group. Because there is no grouping by dimensions,
     * there is only one metric group per time bucket.
     */
    public void testRateGroupByNothing() {
        var groups = groupedRows(documents, List.of(), 60);
        try (var resp = run(String.format(Locale.ROOT, """
            TS %s
            | STATS count(rate(metrics.counterl_hdd.bytes.read)),
                    max(rate(metrics.counterl_hdd.bytes.read)),
                    avg(rate(metrics.counterl_hdd.bytes.read)),
                    min(rate(metrics.counterl_hdd.bytes.read))
                BY tbucket=bucket(@timestamp, 1 minute)
            | SORT tbucket
            | LIMIT 1000
            """, DATASTREAM_NAME))) {
            List<List<Object>> rows = consumeRows(resp);
            List<String> failedWindows = new ArrayList<>();
            for (List<Object> row : rows) {
                var windowStart = windowStart(row.get(4), SECONDS_IN_WINDOW);
                var windowDataPoints = groups.get(List.of(Long.toString(windowStart)));
                var docsPerTimeseries = groupByTimeseries(windowDataPoints, "counterl_hdd.bytes.read");
                var rateAgg = calculateRateAggregation(docsPerTimeseries.values(), SECONDS_IN_WINDOW);
                try {
                    assertThat(row.getFirst(), equalTo(rateAgg.count));
                    checkWithin((Double) row.get(1), rateAgg.max);
                    checkWithin((Double) row.get(2), rateAgg.avg);
                    checkWithin((Double) row.get(3), rateAgg.min);
                } catch (AssertionError e) {
                    failedWindows.add("Failed for row:\n" + row + "\nWanted: " + rateAgg + "\nException: " + e.getMessage());
                }
            }
            assertNoFailedWindows(failedWindows, rows);
        }
    }

    public void testGaugeGroupByRandomAndRandomAgg() {
        var randomWindow = ESTestCase.randomFrom(WINDOW_OPTIONS);
        var windowSize = randomWindow.v2();
        var windowStr = randomWindow.v1();
        var dimensions = ESTestCase.randomSubsetOf(dataGenerationHelper.attributesForMetrics);
        var dimensionsStr = dimensions.isEmpty()
            ? ""
            : ", " + dimensions.stream().map(d -> "attributes." + d).collect(Collectors.joining(", "));
        var metricName = ESTestCase.randomFrom(List.of("gaugel_hdd.bytes.used", "gauged_cpu.percent"));
        var selectedAggs = ESTestCase.randomSubsetOf(2, Agg.values());
        var aggExpression = String.format(
            Locale.ROOT,
            "%s(%s_over_time(metrics.%s))",
            selectedAggs.get(0),
            selectedAggs.get(1),
            metricName
        );
        // TODO: Remove WHERE clause after fixing https://github.com/elastic/elasticsearch/issues/129524
        var query = String.format(Locale.ROOT, """
            TS %s
            | WHERE %s IS NOT NULL
            | STATS
                %s
                BY tbucket=bucket(@timestamp, %s) %s
            | SORT tbucket
            | LIMIT 1000""", DATASTREAM_NAME, metricName, aggExpression, windowStr, dimensionsStr);
        try (EsqlQueryResponse resp = run(query)) {
            var groups = groupedRows(documents, dimensions, windowSize);
            List<List<Object>> rows = consumeRows(resp);
            for (List<Object> row : rows) {
                var rowKey = getRowKey(row, dimensions, 1);
                var tsGroups = groupByTimeseries(groups.get(rowKey), metricName);
                Object expectedVal = aggregatePerTimeseries(tsGroups, selectedAggs.get(0), selectedAggs.get(1));
                Double actualVal = switch (row.get(0)) {
                    case Long l -> l.doubleValue();
                    case Double d -> d;
                    case null -> null;
                    default -> throw new IllegalStateException(
                        "Unexpected value type: " + row.get(0) + " of class " + row.get(0).getClass()
                    );
                };
                try {
                    switch (expectedVal) {
                        case Double dVal -> assertThat(actualVal, closeTo(dVal, dVal * 0.01));
                        case Long lVal -> assertThat(actualVal, closeTo(lVal.doubleValue(), lVal * 0.01));
                        case null -> assertThat(actualVal, equalTo(null));
                        default -> throw new IllegalStateException(
                            "Unexpected value type: " + expectedVal + " of class " + expectedVal.getClass()
                        );
                    }
                } catch (AssertionError e) {
                    throw new AssertionError(
                        "Failed for aggregations:\n"
                            + selectedAggs
                            + " with total dimensions for grouping: "
                            + dimensions.size()
                            + " on metric "
                            + metricName
                            + "\nWanted val: "
                            + expectedVal
                            + "\nGot val: "
                            + actualVal
                            + "\nException: "
                            + e.getMessage(),
                        e
                    );
                }
            }
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
            | STATS
                values(metrics.gaugel_hdd.bytes.used),
                max(max_over_time(metrics.gaugel_hdd.bytes.used)),
                min(min_over_time(metrics.gaugel_hdd.bytes.used)),
                sum(count_over_time(metrics.gaugel_hdd.bytes.used)),
                sum(sum_over_time(metrics.gaugel_hdd.bytes.used)),
                avg(avg_over_time(metrics.gaugel_hdd.bytes.used)),
                count(count_over_time(metrics.gaugel_hdd.bytes.used))
                BY tbucket=bucket(@timestamp, 1 minute), %s
            | SORT tbucket
            | LIMIT 1000""", DATASTREAM_NAME, dimensionsStr))) {
            var groups = groupedRows(documents, dimensions, 60);
            List<List<Object>> rows = consumeRows(resp);
            for (List<Object> row : rows) {
                var rowKey = getRowKey(row, dimensions, 7);
                var tsGroups = groupByTimeseries(groups.get(rowKey), "gaugel_hdd.bytes.used");
                var docValues = valuesInWindow(groups.get(rowKey), "gaugel_hdd.bytes.used");
                if (row.get(0) instanceof List) {
                    assertThat(
                        (Collection<Long>) row.getFirst(),
                        containsInAnyOrder(docValues.stream().mapToLong(Integer::longValue).boxed().toArray(Long[]::new))
                    );
                } else {
                    assertThat(row.getFirst(), equalTo(docValues.isEmpty() ? null : docValues.getFirst().longValue()));
                }
                assertThat(row.get(1), equalTo(aggregatePerTimeseries(tsGroups, Agg.MAX, Agg.MAX)));
                assertThat(row.get(2), equalTo(aggregatePerTimeseries(tsGroups, Agg.MIN, Agg.MIN)));
                assertThat(row.get(3), equalTo(aggregatePerTimeseries(tsGroups, Agg.SUM, Agg.COUNT)));
                assertThat(row.get(4), equalTo(aggregatePerTimeseries(tsGroups, Agg.SUM, Agg.SUM)));
                var avg = (Double) aggregatePerTimeseries(tsGroups, Agg.AVG, Agg.AVG);
                assertThat((Double) row.get(5), row.get(5) == null ? equalTo(null) : closeTo(avg, avg * 0.01));
                // assertThat(row.get(6), equalTo(aggregatePerTimeseries(tsGroups, Agg.COUNT, Agg.COUNT).longValue()));
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
                values(metrics.gaugel_hdd.bytes.used),
                max(max_over_time(metrics.gaugel_hdd.bytes.used)),
                min(min_over_time(metrics.gaugel_hdd.bytes.used)),
                sum(count_over_time(metrics.gaugel_hdd.bytes.used)),
                sum(sum_over_time(metrics.gaugel_hdd.bytes.used)),
                avg(avg_over_time(metrics.gaugel_hdd.bytes.used)),
                count(count_over_time(metrics.gaugel_hdd.bytes.used))
                BY tbucket=bucket(@timestamp, 1 minute)
            | SORT tbucket
            | LIMIT 1000""", DATASTREAM_NAME))) {
            List<List<Object>> rows = consumeRows(resp);
            var groups = groupedRows(documents, List.of(), 60);
            for (List<Object> row : rows) {
                var windowStart = windowStart(row.get(7), 60);
                List<Integer> docValues = valuesInWindow(groups.get(List.of(Long.toString(windowStart))), "gaugel_hdd.bytes.used");
                var tsGroups = groupByTimeseries(groups.get(List.of(Long.toString(windowStart))), "gaugel_hdd.bytes.used");
                if (row.get(0) instanceof List) {
                    assertThat(
                        (Collection<Long>) row.get(0),
                        containsInAnyOrder(docValues.stream().mapToLong(Integer::longValue).boxed().toArray(Long[]::new))
                    );
                } else {
                    assertThat(row.getFirst(), equalTo(docValues.isEmpty() ? null : docValues.getFirst().longValue()));
                }
                Function<Object, Double> toDouble = cell -> switch (cell) {
                    case Long l -> l.doubleValue();
                    case Double d -> d;
                    case null -> null;
                    default -> throw new IllegalStateException("Unexpected value type: " + cell + " of class " + cell.getClass());
                };
                assertThat(toDouble.apply(row.get(1)), equalTo(aggregatePerTimeseries(tsGroups, Agg.MAX, Agg.MAX)));
                assertThat(toDouble.apply(row.get(2)), equalTo(aggregatePerTimeseries(tsGroups, Agg.MIN, Agg.MIN)));
                assertThat(toDouble.apply(row.get(3)), equalTo(aggregatePerTimeseries(tsGroups, Agg.SUM, Agg.COUNT)));
                assertThat(toDouble.apply(row.get(4)), equalTo(aggregatePerTimeseries(tsGroups, Agg.SUM, Agg.SUM)));
                var avg = (Double) aggregatePerTimeseries(tsGroups, Agg.AVG, Agg.AVG);
                assertThat((Double) row.get(5), row.get(5) == null ? equalTo(null) : closeTo(avg, avg * 0.01));
                // assertThat(row.get(6), equalTo(aggregatePerTimeseries(tsGroups, Agg.COUNT, Agg.COUNT).longValue()));
            }
        }
    }
}
