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
import org.elasticsearch.core.Tuple;
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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.downsample.DownsampleDataStreamTests.TIMEOUT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

public class DownsampleRateIT extends DownsamplingIntegTestCase {

    private static final String INDEX_NAME = "metrics";
    private static final String DOWNSAMPLED_INDEX_NAME = "metrics-downsampled";
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

    public void testTimeSeriesAggregateRate() throws Exception {
        runTest(
            List.of(
                Tuple.tuple("2021-04-29T17:01:00.000Z", 1),
                Tuple.tuple("2021-04-29T17:03:12.470Z", 2),
                Tuple.tuple("2021-04-29T17:10:12.470Z", 5),
                Tuple.tuple("2021-04-29T17:22:22.470Z", 6),
                Tuple.tuple("2021-04-29T17:24:22.470Z", 10),
                Tuple.tuple("2021-04-29T17:29:22.470Z", 11),
                Tuple.tuple("2021-04-29T17:32:22.470Z", 12),
                Tuple.tuple("2021-04-29T17:39:22.470Z", 13)
            )
        );
    }

    public void testTimeSeriesAggregateRate_SingleReset() throws Exception {
        runTest(
            List.of(
                Tuple.tuple("2021-04-29T17:02:12.470Z", 1),
                Tuple.tuple("2021-04-29T17:03:12.470Z", 2),
                Tuple.tuple("2021-04-29T17:10:12.470Z", 5),
                Tuple.tuple("2021-04-29T17:19:12.470Z", 8),
                Tuple.tuple("2021-04-29T17:20:22.470Z", 3),
                Tuple.tuple("2021-04-29T17:22:22.470Z", 6),
                Tuple.tuple("2021-04-29T17:24:22.470Z", 10),
                Tuple.tuple("2021-04-29T17:29:22.470Z", 11),
                Tuple.tuple("2021-04-29T17:32:22.470Z", 12),
                Tuple.tuple("2021-04-29T17:39:22.470Z", 13)
            )
        );
    }

    public void testTimeSeriesQueryingSingleLargeReset() throws Exception {
        runTest(
            List.of(
                Tuple.tuple("2021-04-29T17:02:12.470Z", 1000),
                Tuple.tuple("2021-04-29T17:03:12.470Z", 1003),
                Tuple.tuple("2021-04-29T17:10:12.470Z", 1010),
                Tuple.tuple("2021-04-29T17:19:12.470Z", 1040),
                Tuple.tuple("2021-04-29T17:20:22.470Z", 1060),
                Tuple.tuple("2021-04-29T17:22:22.470Z", 20),
                Tuple.tuple("2021-04-29T17:24:22.470Z", 30),
                Tuple.tuple("2021-04-29T17:29:22.470Z", 40),
                Tuple.tuple("2021-04-29T17:32:22.470Z", 70),
                Tuple.tuple("2021-04-29T17:39:22.470Z", 80)
            )
        );
    }

    public void testTimeSeriesQuerying_MultipleResets() throws Exception {
        runTest(
            List.of(
                Tuple.tuple("2021-04-29T17:02:12.470Z", 1000),
                Tuple.tuple("2021-04-29T17:03:12.470Z", 1003),
                Tuple.tuple("2021-04-29T17:05:12.470Z", 1010),
                Tuple.tuple("2021-04-29T17:06:12.470Z", 1040),
                Tuple.tuple("2021-04-29T17:07:22.470Z", 1060),
                Tuple.tuple("2021-04-29T17:08:22.470Z", 20),
                Tuple.tuple("2021-04-29T17:10:22.470Z", 30),
                Tuple.tuple("2021-04-29T17:11:22.470Z", 40),
                Tuple.tuple("2021-04-29T17:12:22.470Z", 70),
                Tuple.tuple("2021-04-29T17:22:22.470Z", 80),
                Tuple.tuple("2021-04-29T17:23:22.470Z", 20),
                Tuple.tuple("2021-04-29T17:24:22.470Z", 10),
                Tuple.tuple("2021-04-29T17:25:22.470Z", 20),
                Tuple.tuple("2021-04-29T17:26:22.470Z", 40),
                Tuple.tuple("2021-04-29T17:27:22.470Z", 60),
                Tuple.tuple("2021-04-29T17:28:22.470Z", 5),
                Tuple.tuple("2021-04-29T17:29:22.470Z", 10),
                Tuple.tuple("2021-04-29T17:59:22.470Z", 20)
            )
        );
    }

    private void runTest(List<Tuple<String, Integer>> documents) throws Exception {
        createIndex();
        indexDocuments(documents);
        DownsampleConfig downsampleConfig = new DownsampleConfig(
            new DateHistogramInterval("30m"),
            DownsampleConfig.SamplingMethod.AGGREGATE
        );
        downsample(downsampleConfig);

        try (var baseline = queryRate(INDEX_NAME); var contender = queryRate(DOWNSAMPLED_INDEX_NAME)) {
            compareResults(baseline, contender);
        }
    }

    private void compareResults(EsqlQueryResponse baseline, EsqlQueryResponse contender) {
        assertResultColumns(baseline);
        assertResultColumns(contender);
        Iterator<Object> baselineRow = baseline.rows().iterator().next().iterator();
        Iterator<Object> contenderRow = contender.rows().iterator().next().iterator();
        // Check rate
        var baselineRate = (double) baselineRow.next();
        var contenderRate = (double) contenderRow.next();
        assertEquals(contenderRate, baselineRate, 0.005);
        // Check timestamp
        var baselineBucket = baselineRow.next();
        var contenderBucket = contenderRow.next();
        assertThat(contenderBucket, equalTo(baselineBucket));
    }

    private void assertResultColumns(EsqlQueryResponse response) {
        var columns = response.columns();
        assertThat(columns, hasSize(2));
        assertThat(
            response.columns(),
            equalTo(List.of(new ColumnInfoImpl("max_rate", "double", null), new ColumnInfoImpl("time_bucket", "date", null)))
        );
    }

    private EsqlQueryResponse queryRate(String indexName) {
        String command = "TS " + indexName + " | STATS max_rate=MAX(RATE(counter)) BY time_bucket = TBUCKET(1 hour) | SORT time_bucket";
        return client().execute(EsqlQueryAction.INSTANCE, new EsqlQueryRequest().query(command)).actionGet(30, TimeUnit.SECONDS);
    }

    private static void createIndex() {
        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
        request.settings(
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "metricset")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-29T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T23:59:59Z")
        );
        request.mapping(MAPPING);
        assertAcked(client().admin().indices().create(request));
    }

    private void indexDocuments(List<Tuple<String, Integer>> documents) throws IOException {
        AtomicInteger i = new AtomicInteger();
        Supplier<XContentBuilder> nextDoc = () -> {
            try {
                assertThat(i.get(), lessThan(documents.size()));
                var docSpec = documents.get(i.getAndIncrement());
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", docSpec.v1())
                    .field("metricset", "pod")
                    .field("counter", docSpec.v2())
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(INDEX_NAME, nextDoc, documents.size());
    }

    private void downsample(DownsampleConfig downsampleConfig) throws Exception {
        // Set the source index to read-only state
        assertAcked(
            indicesAdmin().prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
        );

        assertAcked(
            client().execute(
                DownsampleAction.INSTANCE,
                new DownsampleAction.Request(TEST_REQUEST_TIMEOUT, INDEX_NAME, DOWNSAMPLED_INDEX_NAME, TIMEOUT, downsampleConfig)
            )
        );

        // Wait for downsampling to complete
        SubscribableListener<Void> listener = ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            final var indexMetadata = clusterState.metadata().getProject().index(DOWNSAMPLED_INDEX_NAME);
            if (indexMetadata == null) {
                return false;
            }
            var downsampleStatus = IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(indexMetadata.getSettings());
            return downsampleStatus == IndexMetadata.DownsampleTaskStatus.SUCCESS;
        });
        safeAwait(listener);
    }
}
