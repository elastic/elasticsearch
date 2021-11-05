/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.timeseries.support;

import io.github.nik9000.mapmatcher.MapMatcher;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import static io.github.nik9000.mapmatcher.MapMatcher.assertMap;
import static io.github.nik9000.mapmatcher.MapMatcher.matchesMap;

@TestLogging(value = "org.elasticsearch.search.tsdb:debug", reason = "test")
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
        });
        String beforeAll = "2021-01-01T00:05:00Z";
        String[] dates = new String[] {
            "2021-01-01T00:10:00.000Z",
            "2021-01-01T00:11:00.000Z",
            "2021-01-01T00:15:00.000Z",
            "2021-01-01T00:20:00.000Z", };
        indexRandom(
            true,
            client().prepareIndex("tsdb").setSource(Map.of("@timestamp", dates[0], "dim", d1, "v", 1)),
            client().prepareIndex("tsdb").setSource(Map.of("@timestamp", dates[1], "dim", d1, "v", 2)),
            client().prepareIndex("tsdb").setSource(Map.of("@timestamp", dates[2], "dim", d1, "v", 3)),
            client().prepareIndex("tsdb").setSource(Map.of("@timestamp", dates[3], "dim", d1, "v", 4)),
            client().prepareIndex("tsdb").setSource(Map.of("@timestamp", dates[1], "dim", d2, "v", 5))
        );
        assertMap(
            latestInRange(between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS), beforeAll, dates[0]),
            matchesMap().entry(Map.of("dim", d1), List.of(Map.entry(dates[0], 1.0)))
        );
        assertMap(
            valuesInRange(between(1, MAX_RESULT_WINDOW), beforeAll, dates[0]),
            matchesMap().entry(Map.of("dim", d1), List.of(Map.entry(dates[0], 1.0)))
        );
        assertMap(
            latestInRange(between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS), dates[0], dates[2]),
            matchesMap().entry(Map.of("dim", d1), List.of(Map.entry(dates[2], 3.0)))
                .entry(Map.of("dim", d2), List.of(Map.entry(dates[1], 5.0)))
        );
        assertMap(
            valuesInRange(between(1, MAX_RESULT_WINDOW), dates[0], dates[2]),
            matchesMap().entry(Map.of("dim", d1), List.of(Map.entry(dates[1], 2.0), Map.entry(dates[2], 3.0)))
                .entry(Map.of("dim", d2), List.of(Map.entry(dates[1], 5.0)))
        );
        assertMap(
            latestInRange(between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS), beforeAll, dates[3]),
            matchesMap().entry(Map.of("dim", d1), List.of(Map.entry(dates[3], 4.0)))
                .entry(Map.of("dim", d2), List.of(Map.entry(dates[1], 5.0)))
        );
        assertMap(
            valuesInRange(between(1, MAX_RESULT_WINDOW), beforeAll, dates[3]),
            matchesMap().entry(
                Map.of("dim", d1),
                List.of(Map.entry(dates[0], 1.0), Map.entry(dates[1], 2.0), Map.entry(dates[2], 3.0), Map.entry(dates[3], 4.0))
            ).entry(Map.of("dim", d2), List.of(Map.entry(dates[1], 5.0)))
        );
        assertMap(
            latestInRanges(
                between(1, MultiBucketConsumerService.DEFAULT_MAX_BUCKETS),
                beforeAll,
                dates[3],
                new DateHistogramInterval("5m")
            ),
            matchesMap().entry(Map.of("dim", d1), List.of(Map.entry(dates[0], 1.0), Map.entry(dates[2], 3.0), Map.entry(dates[3], 4.0)))
                .entry(Map.of("dim", d2), List.of(Map.entry(dates[1], 5.0)))
        );
    }

    public void testManyTimeSeries() throws InterruptedException, ExecutionException, IOException {
        createTsdbIndex("dim");
        assertManyTimeSeries(i -> Map.of("dim", Integer.toString(i, Character.MAX_RADIX)));
    }

    public void testManyTimeSeriesWithManyDimensions() throws InterruptedException, ExecutionException, IOException {
        createTsdbIndex("dim0", "dim1", "dim2", "dim3", "dim4", "dim5", "dim6", "dim7");
        assertManyTimeSeries(i -> {
            int dimCount = (i & 0x07) + 1;
            Map<String, Object> dims = new HashMap<>(dimCount);
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
        String min = "2021-01-01T00:10:00Z";
        String max = "2021-01-01T00:15:00Z";
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
            Map<String, Object> dimensions = gen.apply(i);
            String timestamp = null;
            double value = Double.NaN;
            for (long time : times) {
                timestamp = FORMATTER.formatMillis(time);
                value = randomDouble();
                Map<String, Object> source = new HashMap<>(dimensions);
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
            expectedLatest = expectedLatest.entry(dimensions, List.of(Map.entry(timestamp, value)));
            expectedValues = expectedValues.entry(dimensions, expectedValuesForTimeSeries);
        }
        indexRandom(true, docs);
        assertMap(latestInRange(iterationSize, min, max), expectedLatest);
        assertMap(valuesInRange(iterationSize, min, max), expectedValues);
    }

    public void testManySteps() throws InterruptedException, ExecutionException, IOException {
        createTsdbIndex("dim");
        List<Map.Entry<String, Double>> expectedLatest = new ArrayList<>();
        List<Map.Entry<String, Double>> expectedValues = new ArrayList<>();
        String min = "2021-01-01T00:00:00Z";
        long minMillis = FORMATTER.parseMillis(min);
        int iterationBuckets = scaledRandomIntBetween(50, 100);
        int bucketCount = scaledRandomIntBetween(iterationBuckets * 2, iterationBuckets * 100);
        long maxMillis = minMillis + bucketCount * TimeUnit.SECONDS.toMillis(5);
        String max = FORMATTER.formatMillis(maxMillis);
        List<IndexRequestBuilder> docs = new ArrayList<>(bucketCount);
        for (long millis = minMillis; millis < maxMillis; millis += TimeUnit.SECONDS.toMillis(5)) {
            String timestamp = FORMATTER.formatMillis(millis);
            double v = randomDouble();
            if (randomBoolean()) {
                String beforeTimestamp = FORMATTER.formatMillis(millis - 1);
                double beforeValue = randomDouble();
                docs.add(client().prepareIndex("tsdb").setSource(Map.of("@timestamp", beforeTimestamp, "dim", "dim", "v", beforeValue)));
                expectedValues.add(Map.entry(beforeTimestamp, beforeValue));
            }
            expectedLatest.add(Map.entry(timestamp, v));
            expectedValues.add(Map.entry(timestamp, v));
            docs.add(client().prepareIndex("tsdb").setSource(Map.of("@timestamp", timestamp, "dim", "dim", "v", v)));
        }
        indexRandom(true, docs);
        assertMap(
            latestInRanges(iterationBuckets, "2020-01-01T00:00:00Z", max, new DateHistogramInterval("5s")),
            matchesMap(Map.of(Map.of("dim", "dim"), expectedLatest))
        );
        assertMap(valuesInRange(iterationBuckets, "2020-01-01T00:00:00Z", max), matchesMap(Map.of(Map.of("dim", "dim"), expectedValues)));
    }

    private void createTsdbIndex(String... keywordDimensions) throws IOException {
        createTsdbIndex(mapping -> {
            for (String d : keywordDimensions) {
                mapping.startObject(d).field("type", "keyword").field("time_series_dimension", true).endObject();
            }
        });
    }

    private void createTsdbIndex(CheckedConsumer<XContentBuilder, IOException> dimensionMapping) throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject().startObject("properties");
        mapping.startObject("@timestamp").field("type", "date").endObject();
        mapping.startObject("v").field("type", "double").endObject();
        dimensionMapping.accept(mapping);
        mapping.endObject().endObject();
        client().admin().indices().prepareCreate("tsdb").setMapping(mapping).get();
    }

    private Map<Map<String, Object>, List<Map.Entry<String, Double>>> latestInRange(int bucketBatchSize, String min, String max) {
        TemporalAccessor minT = FORMATTER.parse(min);
        TemporalAccessor maxT = FORMATTER.parse(max);
        if (randomBoolean()) {
            long days = Instant.from(maxT).until(Instant.from(minT), ChronoUnit.DAYS) + 1;
            DateHistogramInterval step = new DateHistogramInterval(days + "d");
            return latestInRanges(bucketBatchSize, minT, maxT, step);
        }
        return latestInRange(bucketBatchSize, minT, maxT);
    }

    private Map<Map<String, Object>, List<Map.Entry<String, Double>>> latestInRange(
        int bucketBatchSize,
        TemporalAccessor min,
        TemporalAccessor max
    ) {
        return withMetrics(
            bucketBatchSize,
            between(0, 10000),  // Not used by this method
            (future, metrics) -> metrics.latestInRange("v", min, max, new CollectingListener(future))
        );
    }

    private Map<Map<String, Object>, List<Map.Entry<String, Double>>> latestInRanges(
        int bucketBatchSize,
        String min,
        String max,
        DateHistogramInterval step
    ) {
        return latestInRanges(bucketBatchSize, FORMATTER.parse(min), FORMATTER.parse(max), step);
    }

    private Map<Map<String, Object>, List<Map.Entry<String, Double>>> latestInRanges(
        int bucketBatchSize,
        TemporalAccessor min,
        TemporalAccessor max,
        DateHistogramInterval step
    ) {
        return withMetrics(
            bucketBatchSize,
            between(0, 10000),   // Not used by this method
            (future, metrics) -> metrics.latestInRanges("v", min, max, step, new CollectingListener(future))
        );
    }

    private Map<Map<String, Object>, List<Map.Entry<String, Double>>> valuesInRange(int docBatchSize, String min, String max) {
        return valuesInRange(docBatchSize, FORMATTER.parse(min), FORMATTER.parse(max));
    }

    private Map<Map<String, Object>, List<Map.Entry<String, Double>>> valuesInRange(
        int docBatchSize,
        TemporalAccessor min,
        TemporalAccessor max
    ) {
        return withMetrics(
            between(0, 10000),   // Not used by this method
            docBatchSize,
            (future, metrics) -> metrics.valuesInRange("v", min, max, new CollectingListener(future))
        );
    }

    private <R> R withMetrics(int bucketBatchSize, int docBatchSize, BiConsumer<ListenableActionFuture<R>, TimeSeriesMetrics> handle) {
        ListenableActionFuture<R> result = new ListenableActionFuture<>();
        new TimeSeriesMetricsService(client(), bucketBatchSize, docBatchSize).newMetrics(
            new String[] { "tsdb" },
            new ActionListener<TimeSeriesMetrics>() {
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

    private class CollectingListener implements TimeSeriesMetrics.MetricsCallback {
        private final Map<Map<String, Object>, List<Map.Entry<String, Double>>> results = new HashMap<>();
        private final ActionListener<Map<Map<String, Object>, List<Map.Entry<String, Double>>>> delegate;
        private Map<String, Object> currentDimensions = null;
        private List<Map.Entry<String, Double>> currentValues = null;

        CollectingListener(ActionListener<Map<Map<String, Object>, List<Map.Entry<String, Double>>>> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onTimeSeriesStart(Map<String, Object> dimensions) {
            if (currentDimensions != null) {
                results.put(currentDimensions, currentValues);
            }
            currentDimensions = dimensions;
            currentValues = new ArrayList<>();
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
