/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.timeseries.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import static java.time.temporal.ChronoField.INSTANT_SECONDS;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

@TestLogging(value = "org.elasticsearch.timeseries.support:debug", reason = "test")
public class TimeSeriesMetricsIT extends ESIntegTestCase {
    private static final int MAX_RESULT_WINDOW = IndexSettings.MAX_RESULT_WINDOW_SETTING.getDefault(Settings.EMPTY);
    private static final DateFormatter FORMATTER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

    public void testKeywordDimension() throws Exception {
        assertSmallSimple("a", "b", mapping -> mapping.field("type", "keyword"));
    }

    public void testByteDimension() throws Exception {
        assertSmallSimple(0L, 1L, mapping -> mapping.field("type", "byte"));
    }

    public void testShortDimension() throws Exception {
        assertSmallSimple(0L, 1L, mapping -> mapping.field("type", "short"));
    }

    public void testIntDimension() throws Exception {
        assertSmallSimple(0L, 1L, mapping -> mapping.field("type", "integer"));
    }

    public void testLongDimension() throws Exception {
        assertSmallSimple(0L, 1L, mapping -> mapping.field("type", "long"));
    }

    public void testIpDimension() throws Exception {
        assertSmallSimple("192.168.0.1", "2001:db8::1:0:0:1", mapping -> mapping.field("type", "ip"));
    }

    // TODO unsigned long dimension

    public void assertSmallSimple(Object d1, Object d2, CheckedConsumer<XContentBuilder, IOException> dimensionMapping) throws Exception {
        createTsdbIndex(mapping -> {
            mapping.startObject("dim");
            dimensionMapping.accept(mapping);
            mapping.field("time_series_dimension", true);
            mapping.endObject();
            // Add a keyword dimension as a routing parameter
            mapping.startObject("k").field("type", "keyword").field("time_series_dimension", true).endObject();
        }, List.of("k"));
        String[] dates = new String[] {
            "2021-01-01T00:10:00.000Z",
            "2021-01-01T00:11:00.000Z",
            "2021-01-01T00:15:00.000Z",
            "2021-01-01T00:20:00.000Z", };
        indexRandom(
            true,
            false,
            client().prepareIndex("tsdb").setSource(Map.of("@timestamp", dates[0], "k", "k", "dim", d1, "v", 1, "m", 1)),
            client().prepareIndex("tsdb").setSource(Map.of("@timestamp", dates[1], "k", "k", "dim", d1, "v", 2, "m", 2)),
            client().prepareIndex("tsdb").setSource(Map.of("@timestamp", dates[2], "k", "k", "dim", d1, "v", 3, "m", 3)),
            client().prepareIndex("tsdb").setSource(Map.of("@timestamp", dates[3], "k", "k", "dim", d1, "v", 4, "m", 4)),
            client().prepareIndex("tsdb").setSource(Map.of("@timestamp", dates[1], "k", "k", "dim", d2, "v", 5, "m", 6))
        );

        assertMap(
            latest(between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS), TimeValue.timeValueMinutes(5), dates[0]),
            matchesMap().entry(Tuple.tuple("v", Map.of("dim", d1, "k", "k")), List.of(Map.entry(dates[0], 1.0)))
        );

        assertMap(
            latest(between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS), TimeValue.timeValueMinutes(10), dates[2]),
            matchesMap().entry(Tuple.tuple("v", Map.of("dim", d1, "k", "k")), List.of(Map.entry(dates[2], 3.0)))
                .entry(Tuple.tuple("v", Map.of("dim", d2, "k", "k")), List.of(Map.entry(dates[2], 5.0)))
        );

        assertMap(
            latest(between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS), TimeValue.timeValueMinutes(15), dates[3]),
            matchesMap().entry(Tuple.tuple("v", Map.of("dim", d1, "k", "k")), List.of(Map.entry(dates[3], 4.0)))
                .entry(Tuple.tuple("v", Map.of("dim", d2, "k", "k")), List.of(Map.entry(dates[3], 5.0)))
        );

        assertMap(
            range(between(1, MAX_RESULT_WINDOW), TimeValue.timeValueMinutes(15), TimeValue.timeValueMinutes(10), null, dates[3]),
            matchesMap().entry(
                Tuple.tuple("v", Map.of("dim", d1, "k", "k")),
                List.of(Map.entry(dates[1], 2.0), Map.entry(dates[2], 3.0), Map.entry(dates[3], 4.0))
            ).entry(Tuple.tuple("v", Map.of("dim", d2, "k", "k")), List.of(Map.entry(dates[1], 5.0)))
        );

        assertMap(
            range(
                between(1, MAX_RESULT_WINDOW),
                TimeValue.timeValueMinutes(15),
                TimeValue.timeValueMinutes(10),
                TimeValue.timeValueMinutes(1),
                dates[3]
            ),
            matchesMap().entry(
                Tuple.tuple("v", Map.of("dim", d1, "k", "k")),
                List.of(
                    Map.entry("2021-01-01T00:11:00.000Z", 2.0),
                    Map.entry("2021-01-01T00:12:00.000Z", 2.0),
                    Map.entry("2021-01-01T00:13:00.000Z", 2.0),
                    Map.entry("2021-01-01T00:14:00.000Z", 2.0),
                    Map.entry("2021-01-01T00:15:00.000Z", 3.0),
                    Map.entry("2021-01-01T00:16:00.000Z", 3.0),
                    Map.entry("2021-01-01T00:17:00.000Z", 3.0),
                    Map.entry("2021-01-01T00:18:00.000Z", 3.0),
                    Map.entry("2021-01-01T00:19:00.000Z", 3.0),
                    Map.entry("2021-01-01T00:20:00.000Z", 4.0)
                )
            )
                .entry(
                    Tuple.tuple("v", Map.of("dim", d2, "k", "k")),
                    List.of(
                        Map.entry("2021-01-01T00:11:00.000Z", 5.0),
                        Map.entry("2021-01-01T00:12:00.000Z", 5.0),
                        Map.entry("2021-01-01T00:13:00.000Z", 5.0),
                        Map.entry("2021-01-01T00:14:00.000Z", 5.0),
                        Map.entry("2021-01-01T00:15:00.000Z", 5.0),
                        Map.entry("2021-01-01T00:16:00.000Z", 5.0),
                        Map.entry("2021-01-01T00:17:00.000Z", 5.0),
                        Map.entry("2021-01-01T00:18:00.000Z", 5.0),
                        Map.entry("2021-01-01T00:19:00.000Z", 5.0),
                        Map.entry("2021-01-01T00:20:00.000Z", 5.0)
                    )
                )
        );

        assertMap(
            latest(
                List.of(randomBoolean() ? re("[u-z]") : rn("[a-u]")),
                List.of(),
                between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS),
                TimeValue.timeValueMinutes(15),
                FORMATTER.parse(dates[3]).getLong(INSTANT_SECONDS) * 1000
            ),
            matchesMap().entry(Tuple.tuple("v", Map.of("dim", d1, "k", "k")), List.of(Map.entry(dates[3], 4.0)))
                .entry(Tuple.tuple("v", Map.of("dim", d2, "k", "k")), List.of(Map.entry(dates[3], 5.0)))
        );

        assertMap(
            latest(
                List.of(re("[a-t]"), re("[b-s]")),
                List.of(eq("dim", d1.toString())),
                between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS),
                TimeValue.timeValueMinutes(15),
                FORMATTER.parse(dates[3]).getLong(INSTANT_SECONDS) * 1000
            ),
            matchesMap().entry(Tuple.tuple("m", Map.of("dim", d1, "k", "k")), List.of(Map.entry(dates[3], 4.0)))
        );

        assertMap(
            latest(
                List.of(re("[a-t]"), re("[b-s]")),
                List.of(ne("dim", d1.toString())),
                between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS),
                TimeValue.timeValueMinutes(15),
                FORMATTER.parse(dates[3]).getLong(INSTANT_SECONDS) * 1000
            ),
            matchesMap().entry(Tuple.tuple("m", Map.of("dim", d2, "k", "k")), List.of(Map.entry(dates[3], 6.0)))
        );

        if ("a".equals(d1)) {
            // regular expressions don't work with numeric nor ip values
            assertMap(
                latest(
                    List.of(re("[a-t]"), re("[b-s]")),
                    List.of(re("dim", "[" + d2 + "-z]")),
                    between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS),
                    TimeValue.timeValueMinutes(15),
                    FORMATTER.parse(dates[3]).getLong(INSTANT_SECONDS) * 1000
                ),
                matchesMap().entry(Tuple.tuple("m", Map.of("dim", d2, "k", "k")), List.of(Map.entry(dates[3], 6.0)))
            );

            assertMap(
                latest(
                    List.of(re("[a-t]"), re("[b-s]")),
                    List.of(rn("dim", "[" + d2 + "-z]")),
                    between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS),
                    TimeValue.timeValueMinutes(15),
                    FORMATTER.parse(dates[3]).getLong(INSTANT_SECONDS) * 1000
                ),
                matchesMap().entry(Tuple.tuple("m", Map.of("dim", d1, "k", "k")), List.of(Map.entry(dates[3], 4.0)))
            );

            assertMap(
                latest(
                    List.of(eq("v")),
                    List.of(re("dim", "[" + d1 + "-" + d2 + "]")),
                    between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS),
                    TimeValue.timeValueMinutes(15),
                    FORMATTER.parse(dates[3]).getLong(INSTANT_SECONDS) * 1000
                ),
                matchesMap().entry(Tuple.tuple("v", Map.of("dim", d1, "k", "k")), List.of(Map.entry(dates[3], 4.0)))
                    .entry(Tuple.tuple("v", Map.of("dim", d2, "k", "k")), List.of(Map.entry(dates[3], 5.0)))
            );
        }
    }

    public void testManyTimeSeries() throws InterruptedException, ExecutionException, IOException {
        createTsdbIndex("dim");
        assertManyTimeSeries(i -> Map.of("dim", Integer.toString(i, Character.MAX_RADIX)));
    }

    public void testManyTimeSeriesWithManyDimensions() throws InterruptedException, ExecutionException, IOException {
        createTsdbIndex("dim0", "dim1", "dim2", "dim3", "dim4", "dim5", "dim6", "dim7");
        assertManyTimeSeries(i -> {
            int dimCount = (i & 0x07) + 1;
            Map<String, Object> dims = Maps.newMapWithExpectedSize(dimCount);
            int offset = (i >> 3) & 0x03;
            String value = Integer.toString(i, Character.MAX_RADIX);
            for (int d = 0; d < dimCount; d++) {
                dims.put("dim" + ((d + offset) & 0x07), value);
            }
            return dims;
        });
    }

    private void assertManyTimeSeries(IntFunction<Map<String, Object>> gen) throws InterruptedException {
        MapMatcher expectedLatest = matchesMap();
        MapMatcher expectedValues = matchesMap();
        String min = "2021-01-01T00:10:00.000Z";
        String max = "2021-01-01T00:15:00.000Z";
        long minMillis = FORMATTER.parseMillis(min);
        long maxMillis = FORMATTER.parseMillis(max);
        int iterationSize = scaledRandomIntBetween(50, 100);
        int docCount = scaledRandomIntBetween(iterationSize * 2, iterationSize * 100);
        List<IndexRequestBuilder> docs = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            int count = randomBoolean() ? 1 : 2;
            Set<Long> times = new TreeSet<>(); // We're using the ordered sort below
            while (times.size() < count) {
                times.add(randomLongBetween(minMillis + 1, maxMillis));
            }
            List<Map.Entry<String, Double>> expectedValuesForTimeSeries = new ArrayList<>(count);
            Tuple<String, Map<String, Object>> dimensions = Tuple.tuple("v", gen.apply(i));
            String timestamp;
            double value = Double.NaN;
            for (long time : times) {
                timestamp = FORMATTER.formatMillis(time);
                value = randomDouble();
                Map<String, Object> source = new HashMap<>(dimensions.v2());
                source.put("@timestamp", timestamp);
                source.put("v", value);
                if (randomBoolean()) {
                    int garbage = between(1, 10);
                    for (int g = 0; g < garbage; g++) {
                        source.put("garbage" + g, randomAlphaOfLength(5));
                    }
                }
                docs.add(client().prepareIndex("tsdb").setSource(source));
                expectedValuesForTimeSeries.add(Map.entry(timestamp, value));
            }
            expectedLatest = expectedLatest.entry(dimensions, List.of(Map.entry(max, value)));
            expectedValues = expectedValues.entry(dimensions, expectedValuesForTimeSeries);
        }
        indexRandom(true, false, docs);
        assertMap(latest(iterationSize, TimeValue.timeValueMillis(maxMillis - minMillis), maxMillis), expectedLatest);
        assertMap(
            range(
                iterationSize,
                TimeValue.timeValueMinutes(randomIntBetween(1, 10)),
                TimeValue.timeValueMillis(maxMillis - minMillis),
                null,
                maxMillis
            ),
            expectedValues
        );
    }

    public void testManySteps() throws InterruptedException, ExecutionException, IOException {
        createTsdbIndex("dim");
        List<Map.Entry<String, Double>> expectedLatest = new ArrayList<>();
        List<Map.Entry<String, Double>> expectedValues = new ArrayList<>();
        String min = "2021-01-01T00:00:00Z";
        long minMillis = FORMATTER.parseMillis(min);
        int iterationBuckets = scaledRandomIntBetween(50, 100);
        int bucketCount = scaledRandomIntBetween(iterationBuckets * 2, iterationBuckets * 10);
        long maxMillis = minMillis + bucketCount * TimeUnit.SECONDS.toMillis(5);
        List<IndexRequestBuilder> docs = new ArrayList<>(bucketCount);
        for (long millis = minMillis; millis < maxMillis; millis += TimeUnit.SECONDS.toMillis(5)) {
            String timestamp = FORMATTER.formatMillis(millis);
            double v = randomDouble();
            if (randomBoolean()) {
                String beforeTimestamp = FORMATTER.formatMillis(millis - 1);
                double beforeValue = randomDouble();
                docs.add(client().prepareIndex("tsdb").setSource(Map.of("@timestamp", beforeTimestamp, "dim", "dim", "v", beforeValue)));
                if (millis - 1 >= minMillis) {
                    expectedValues.add(Map.entry(beforeTimestamp, beforeValue));
                }
            }
            expectedLatest.add(Map.entry(timestamp, v));
            expectedValues.add(Map.entry(timestamp, v));
            docs.add(client().prepareIndex("tsdb").setSource(Map.of("@timestamp", timestamp, "dim", "dim", "v", v)));
        }
        indexRandom(true, false, docs);
        assertMap(
            range(
                iterationBuckets,
                TimeValue.timeValueMinutes(randomIntBetween(1, 10)),
                TimeValue.timeValueMillis(maxMillis - minMillis),
                TimeValue.timeValueSeconds(5),
                maxMillis - 1
            ),
            matchesMap(Map.of(Tuple.tuple("v", Map.of("dim", "dim")), expectedLatest))
        );
        assertMap(
            range(
                iterationBuckets,
                TimeValue.timeValueMinutes(randomIntBetween(1, 10)),
                TimeValue.timeValueMillis(maxMillis - minMillis),
                null,
                maxMillis - 1
            ),
            matchesMap(Map.of(Tuple.tuple("v", Map.of("dim", "dim")), expectedValues))
        );
    }

    private void createTsdbIndex(String... keywordDimensions) throws IOException {
        createTsdbIndex(mapping -> {
            for (String d : keywordDimensions) {
                mapping.startObject(d).field("type", "keyword").field("time_series_dimension", true).endObject();
            }
        }, Arrays.asList(keywordDimensions));
    }

    private void createTsdbIndex(CheckedConsumer<XContentBuilder, IOException> dimensionMapping, List<String> routingDims)
        throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject().startObject("properties");
        mapping.startObject("@timestamp").field("type", "date").endObject();
        mapping.startObject("v").field("type", "double").field("time_series_metric", "gauge").endObject();
        mapping.startObject("m").field("type", "double").field("time_series_metric", "gauge").endObject();
        dimensionMapping.accept(mapping);
        mapping.endObject().endObject();

        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingDims)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2000-01-08T23:40:53.384Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z")
            .build();
        client().admin().indices().prepareCreate("tsdb").setSettings(settings).setMapping(mapping).get();
    }

    private Map<Tuple<String, Map<String, Object>>, List<Map.Entry<String, Double>>> latest(
        int bucketBatchSize,
        TimeValue staleness,
        String time
    ) {
        return latest(bucketBatchSize, staleness, FORMATTER.parse(time).getLong(INSTANT_SECONDS) * 1000);
    }

    private Map<Tuple<String, Map<String, Object>>, List<Map.Entry<String, Double>>> latest(
        int bucketBatchSize,
        TimeValue staleness,
        long timeMillis
    ) {
        return withMetrics(
            bucketBatchSize,
            between(0, 10000),  // Not used by this method
            staleness,
            (future, metrics) -> metrics.latest(List.of(eq("v")), List.of(), timeMillis, new CollectingListener(future))
        );
    }

    private Map<Tuple<String, Map<String, Object>>, List<Map.Entry<String, Double>>> latest(
        List<TimeSeriesMetrics.TimeSeriesMetricSelector> metricSelectors,
        List<TimeSeriesMetrics.TimeSeriesDimensionSelector> dimensionSelectors,
        int bucketBatchSize,
        TimeValue staleness,
        long timeMillis
    ) {
        return withMetrics(
            bucketBatchSize,
            between(0, 10000),  // Not used by this method
            staleness,
            (future, metrics) -> metrics.latest(metricSelectors, dimensionSelectors, timeMillis, new CollectingListener(future))
        );
    }

    private Map<Tuple<String, Map<String, Object>>, List<Map.Entry<String, Double>>> range(
        int bucketBatchSize,
        TimeValue staleness,
        TimeValue range,
        TimeValue step,
        String time
    ) {
        return range(bucketBatchSize, staleness, range, step, FORMATTER.parse(time).getLong(INSTANT_SECONDS) * 1000);
    }

    private Map<Tuple<String, Map<String, Object>>, List<Map.Entry<String, Double>>> range(
        int bucketBatchSize,
        TimeValue staleness,
        TimeValue range,
        TimeValue step,
        long timeMillis
    ) {
        return withMetrics(
            bucketBatchSize,
            between(1, 10000),
            staleness,
            (future, metrics) -> metrics.range(List.of(eq("v")), List.of(), timeMillis, range, step, new CollectingListener(future))
        );
    }

    private static TimeSeriesMetrics.TimeSeriesMetricSelector eq(String metric) {
        return new TimeSeriesMetrics.TimeSeriesMetricSelector(TimeSeriesMetrics.TimeSeriesSelectorMatcher.EQUAL, metric);
    }

    private static TimeSeriesMetrics.TimeSeriesMetricSelector ne(String metric) {
        return new TimeSeriesMetrics.TimeSeriesMetricSelector(TimeSeriesMetrics.TimeSeriesSelectorMatcher.NOT_EQUAL, metric);
    }

    private static TimeSeriesMetrics.TimeSeriesMetricSelector re(String metric) {
        return new TimeSeriesMetrics.TimeSeriesMetricSelector(TimeSeriesMetrics.TimeSeriesSelectorMatcher.RE_EQUAL, metric);
    }

    private static TimeSeriesMetrics.TimeSeriesMetricSelector rn(String metric) {
        return new TimeSeriesMetrics.TimeSeriesMetricSelector(TimeSeriesMetrics.TimeSeriesSelectorMatcher.RE_NOT_EQUAL, metric);
    }

    private static TimeSeriesMetrics.TimeSeriesDimensionSelector eq(String dimension, String value) {
        return new TimeSeriesMetrics.TimeSeriesDimensionSelector(dimension, TimeSeriesMetrics.TimeSeriesSelectorMatcher.EQUAL, value);
    }

    private static TimeSeriesMetrics.TimeSeriesDimensionSelector ne(String dimension, String value) {
        return new TimeSeriesMetrics.TimeSeriesDimensionSelector(dimension, TimeSeriesMetrics.TimeSeriesSelectorMatcher.NOT_EQUAL, value);
    }

    private static TimeSeriesMetrics.TimeSeriesDimensionSelector re(String dimension, String value) {
        return new TimeSeriesMetrics.TimeSeriesDimensionSelector(dimension, TimeSeriesMetrics.TimeSeriesSelectorMatcher.RE_EQUAL, value);
    }

    private static TimeSeriesMetrics.TimeSeriesDimensionSelector rn(String dimension, String value) {
        return new TimeSeriesMetrics.TimeSeriesDimensionSelector(
            dimension,
            TimeSeriesMetrics.TimeSeriesSelectorMatcher.RE_NOT_EQUAL,
            value
        );
    }

    private <R> R withMetrics(
        int bucketBatchSize,
        int docBatchSize,
        TimeValue staleness,
        BiConsumer<ListenableActionFuture<R>, TimeSeriesMetrics> handle
    ) {
        ListenableActionFuture<R> result = new ListenableActionFuture<>();
        new TimeSeriesMetricsService(client(), bucketBatchSize, docBatchSize, staleness).newMetrics(
            new String[] { "tsdb" },
            IndicesOptions.STRICT_EXPAND_OPEN,
            new ActionListener<>() {
                @Override
                public void onResponse(TimeSeriesMetrics metrics) {
                    handle.accept(result, metrics);
                }

                @Override
                public void onFailure(Exception e) {
                    result.onFailure(e);
                }
            }
        );
        return result.actionGet();
    }

    private static class CollectingListener implements TimeSeriesMetrics.MetricsCallback {
        private final Map<Tuple<String, Map<String, Object>>, List<Map.Entry<String, Double>>> results = new HashMap<>();
        private final ActionListener<Map<Tuple<String, Map<String, Object>>, List<Map.Entry<String, Double>>>> delegate;
        private Tuple<String, Map<String, Object>> currentDimensions = null;
        private List<Map.Entry<String, Double>> currentValues = null;

        CollectingListener(ActionListener<Map<Tuple<String, Map<String, Object>>, List<Map.Entry<String, Double>>>> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onTimeSeriesStart(String metric, Map<String, Object> dimensions) {
            if (currentDimensions != null) {
                results.put(currentDimensions, currentValues);
            }
            currentDimensions = new Tuple<>(metric, dimensions);
            currentValues = results.getOrDefault(currentDimensions, new ArrayList<>());
        }

        @Override
        public void onMetric(long time, double value) {
            currentValues.add(Map.entry(FORMATTER.formatMillis(time), value));
        }

        @Override
        public void onSuccess() {
            results.put(currentDimensions, currentValues);
            delegate.onResponse(results);
        }

        @Override
        public void onError(Exception e) {
            delegate.onFailure(e);
        }
    }
}
