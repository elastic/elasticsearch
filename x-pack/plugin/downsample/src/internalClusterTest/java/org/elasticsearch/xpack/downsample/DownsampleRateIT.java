/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.downsample.DownsampleDataStreamTests.TIMEOUT;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.COLUMN_METADATA_BUCKET;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class DownsampleRateIT extends DownsamplingIntegTestCase {

    private static final String INDEX_NAME = "metrics";
    private static final String DOWNSAMPLED_AGGREGATE_INDEX_NAME = "metrics-aggregated";
    private static final String DOWNSAMPLED_LAST_VALUE_INDEX_NAME = "metrics-last-value";
    private static final String MAPPING = """
        {
          "properties": {
            "@timestamp": {
              "type": "date"
            },
            "metricset": {
              "type": "keyword",
              "time_series_dimension": true
            },
            "counter": {
              "type": "long",
              "time_series_metric": "counter"
            }
          }
        }
        """;
    public static final String START_TIME = "2021-04-29T00:00:00Z";
    public static final String END_TIME = "2021-04-29T23:59:59Z";

    public void testTimeSeriesAggregateRate() {
        runTest(
            List.of(
                new DocumentSpec("2021-04-29T17:01:00.000Z", 1),
                new DocumentSpec("2021-04-29T17:03:12.470Z", 2),
                new DocumentSpec("2021-04-29T17:10:12.470Z", 5),
                new DocumentSpec("2021-04-29T17:22:22.470Z", 6),
                new DocumentSpec("2021-04-29T17:24:22.470Z", 10),
                new DocumentSpec("2021-04-29T17:29:22.470Z", 11),
                new DocumentSpec("2021-04-29T17:32:22.470Z", 12),
                new DocumentSpec("2021-04-29T17:39:22.470Z", 13)
            ),
            "30m"
        );
    }

    public void testTimeSeriesAggregateRate_SingleReset() {
        runTest(
            List.of(
                new DocumentSpec("2021-04-29T17:02:12.470Z", 1),
                new DocumentSpec("2021-04-29T17:03:12.470Z", 2),
                new DocumentSpec("2021-04-29T17:10:12.470Z", 5),
                new DocumentSpec("2021-04-29T17:19:12.470Z", 8),
                new DocumentSpec("2021-04-29T17:20:22.470Z", 3),
                new DocumentSpec("2021-04-29T17:22:22.470Z", 6),
                new DocumentSpec("2021-04-29T17:24:22.470Z", 10),
                new DocumentSpec("2021-04-29T17:29:22.470Z", 11),
                new DocumentSpec("2021-04-29T17:32:22.470Z", 12),
                new DocumentSpec("2021-04-29T17:39:22.470Z", 13)
            ),
            "30m"
        );
    }

    public void testTimeSeriesQueryingSingleLargeReset() {
        runTest(
            List.of(
                new DocumentSpec("2021-04-29T17:02:12.470Z", 1000),
                new DocumentSpec("2021-04-29T17:03:12.470Z", 1003),
                new DocumentSpec("2021-04-29T17:10:12.470Z", 1010),
                new DocumentSpec("2021-04-29T17:19:12.470Z", 1040),
                new DocumentSpec("2021-04-29T17:20:22.470Z", 1060),
                new DocumentSpec("2021-04-29T17:22:22.470Z", 20),
                new DocumentSpec("2021-04-29T17:24:22.470Z", 30),
                new DocumentSpec("2021-04-29T17:29:22.470Z", 40),
                new DocumentSpec("2021-04-29T17:32:22.470Z", 70),
                new DocumentSpec("2021-04-29T17:39:22.470Z", 80)
            ),
            "30m"
        );
    }

    public void testTimeSeriesQuerying_MultipleResets() {
        runTest(
            List.of(
                new DocumentSpec("2021-04-29T17:02:12.470Z", 1000),
                new DocumentSpec("2021-04-29T17:03:12.470Z", 1003),
                new DocumentSpec("2021-04-29T17:05:12.470Z", 1010),
                new DocumentSpec("2021-04-29T17:06:12.470Z", 1040),
                new DocumentSpec("2021-04-29T17:07:22.470Z", 1060),
                new DocumentSpec("2021-04-29T17:08:22.470Z", 20),
                new DocumentSpec("2021-04-29T17:10:22.470Z", 30),
                new DocumentSpec("2021-04-29T17:11:22.470Z", 40),
                new DocumentSpec("2021-04-29T17:12:22.470Z", 70),
                new DocumentSpec("2021-04-29T17:22:22.470Z", 80),
                new DocumentSpec("2021-04-29T17:23:22.470Z", 20),
                new DocumentSpec("2021-04-29T17:24:22.470Z", 10),
                new DocumentSpec("2021-04-29T17:25:22.470Z", 20),
                new DocumentSpec("2021-04-29T17:26:22.470Z", 40),
                new DocumentSpec("2021-04-29T17:27:22.470Z", 60),
                new DocumentSpec("2021-04-29T17:28:22.470Z", 5),
                new DocumentSpec("2021-04-29T17:29:22.470Z", 10),
                new DocumentSpec("2021-04-29T17:59:22.470Z", 20)
            ),
            "30m"
        );
    }

    public void testTimeSeriesQuerying_RandomDocuments() {
        long startTime = Instant.parse(START_TIME).toEpochMilli();
        long endTime = Instant.parse(END_TIME).toEpochMilli();
        int counter = 0;
        long currentTime = startTime;
        List<DocumentSpec> documentSpecs = new ArrayList<>();
        while (currentTime < endTime) {
            if (randomInt(9) > 0) {
                counter = randomInt(100);
            } else {
                counter += randomInt(100);
            }
            documentSpecs.add(new DocumentSpec(randomFrom("pod-1", "pod-2", "pod-3"), DATE_FORMATTER.formatMillis(currentTime), counter));
            currentTime += randomLongBetween(5, 30) * 1000;
        }
        runTest(documentSpecs, "1h");
    }

    private void runTest(List<DocumentSpec> documentSpecs, String interval) {
        createIndex();
        indexDocuments(documentSpecs);
        downsample(new DateHistogramInterval(interval));
        try (
            var baseline = queryRate(INDEX_NAME);
            var aggregateContender = queryRate(DOWNSAMPLED_AGGREGATE_INDEX_NAME);
            var lastValueContender = queryRate(DOWNSAMPLED_LAST_VALUE_INDEX_NAME)
        ) {
            compareResults(baseline, aggregateContender, lastValueContender);
        }
    }

    private void compareResults(EsqlQueryResponse baseline, EsqlQueryResponse aggregateContender, EsqlQueryResponse lastValueContender) {
        assertResultColumns(baseline);
        assertResultColumns(aggregateContender);
        assertResultColumns(lastValueContender);
        List<RateResult> baselineRows = convertToSortedList(baseline);
        List<RateResult> aggregatedRows = convertToSortedList(aggregateContender);
        List<RateResult> lastValueRows = convertToSortedList(lastValueContender);
        for (int i = 0; i < baselineRows.size(); i++) {
            RateResult baselineRow = baselineRows.get(i);
            RateResult aggregatedRow = aggregatedRows.get(i);
            RateResult lastValueRow = lastValueRows.get(i);
            // We need these assertions to correctly identify the rate
            assertThat(aggregatedRow.timeseries, equalTo(baselineRow.timeseries));
            assertThat(aggregatedRow.timestamp, equalTo(baselineRow.timestamp));
            assertThat(lastValueRow.timeseries, equalTo(baselineRow.timeseries));
            assertThat(lastValueRow.timestamp, equalTo(baselineRow.timestamp));
            // Aggregate counter downsampling should be closer to the baseline than the last value
            assertThat(Math.abs(aggregatedRow.rate - baselineRow.rate), lessThanOrEqualTo(Math.abs(lastValueRow.rate - baselineRow.rate)));
        }
    }

    // We need to convert the result to a list and sort it by timeseries first and then by timestamp
    // to compare the results row by row
    private static List<RateResult> convertToSortedList(EsqlQueryResponse result) {
        var rows = new ArrayList<RateResult>((int) result.getRowCount());
        for (Iterable<Object> objects : result.rows()) {
            var row = objects.iterator();
            while (row.hasNext()) {
                var rate = (double) row.next();
                var timeseries = (String) row.next();
                var timestamp = (String) row.next();
                rows.add(new RateResult(timeseries, timestamp, rate));
            }
        }
        rows.sort(Comparator.comparing(RateResult::timeseries).thenComparing(RateResult::timestamp));
        return rows;
    }

    private void assertResultColumns(EsqlQueryResponse response) {
        Map<String, Object> bucketMeta = COLUMN_METADATA_BUCKET.isEnabled()
            ? Map.of("bucket", Map.of("interval", 1L, "unit", "hour"))
            : null;
        var columns = response.columns();
        assertThat(columns, hasSize(3));
        assertThat(
            response.columns(),
            equalTo(
                List.of(
                    new ColumnInfoImpl("rate", "double", null),
                    new ColumnInfoImpl("_timeseries", "keyword", null),
                    new ColumnInfoImpl("time_bucket", "date", null, bucketMeta)
                )
            )
        );
    }

    private EsqlQueryResponse queryRate(String indexName) {
        String command = "TS " + indexName + " | STATS rate=RATE(counter) BY time_bucket = TBUCKET(1 hour) | SORT time_bucket";
        return client().execute(EsqlQueryAction.INSTANCE, new EsqlQueryRequest().query(command)).actionGet(30, TimeUnit.SECONDS);
    }

    private static void createIndex() {
        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
        request.settings(
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "metricset")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), START_TIME)
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), END_TIME)
        );
        request.mapping(MAPPING);
        assertAcked(client().admin().indices().create(request));
    }

    private void indexDocuments(List<DocumentSpec> documentSpecs) {
        AtomicInteger i = new AtomicInteger();
        Supplier<XContentBuilder> nextDoc = () -> {
            try {
                assertThat(i.get(), lessThan(documentSpecs.size()));
                var docSpec = documentSpecs.get(i.getAndIncrement());
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", docSpec.timestamp)
                    .field("metricset", docSpec.dimension)
                    .field("counter", docSpec.counter)
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(INDEX_NAME, nextDoc, documentSpecs.size());
    }

    private void downsample(DateHistogramInterval interval) {
        // Set the source index to read-only state
        assertAcked(
            indicesAdmin().prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
        );

        assertAcked(
            client().execute(
                DownsampleAction.INSTANCE,
                new DownsampleAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    INDEX_NAME,
                    DOWNSAMPLED_AGGREGATE_INDEX_NAME,
                    TIMEOUT,
                    new DownsampleConfig(interval, DownsampleConfig.SamplingMethod.AGGREGATE)
                )
            )
        );
        assertAcked(
            client().execute(
                DownsampleAction.INSTANCE,
                new DownsampleAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    INDEX_NAME,
                    DOWNSAMPLED_LAST_VALUE_INDEX_NAME,
                    TIMEOUT,
                    new DownsampleConfig(interval, DownsampleConfig.SamplingMethod.LAST_VALUE)
                )
            )
        );

        // Wait for downsampling to complete
        SubscribableListener<Void> listener = ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            final var aggregatedIndexMetadata = clusterState.metadata().getProject().index(DOWNSAMPLED_AGGREGATE_INDEX_NAME);
            final var lastValueIndexMetadata = clusterState.metadata().getProject().index(DOWNSAMPLED_LAST_VALUE_INDEX_NAME);
            if (aggregatedIndexMetadata == null || lastValueIndexMetadata == null) {
                return false;
            }
            var downsampleAggregateStatus = IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(aggregatedIndexMetadata.getSettings());
            var downsampleLastValueStatus = IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(lastValueIndexMetadata.getSettings());
            return downsampleAggregateStatus == IndexMetadata.DownsampleTaskStatus.SUCCESS
                && downsampleLastValueStatus == IndexMetadata.DownsampleTaskStatus.SUCCESS;
        });
        safeAwait(listener);
    }

    record DocumentSpec(String dimension, String timestamp, int counter) {
        DocumentSpec(String timestamp, int counter) {
            this("pod", timestamp, counter);
        }
    }

    record RateResult(String timeseries, String timestamp, double rate) {}
}
