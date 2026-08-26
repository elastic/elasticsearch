/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 1)
public class BatchBulkTSDBIT extends ESIntegTestCase {

    @ClassRule
    public static TestRule snapshotBuildRule = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeTrue("batch indexing requires snapshot builds", Build.current().isSnapshot());
            base.evaluate();
        }
    };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true)
            .build();
    }

    private String findCoordinatingNode() {
        for (String nodeName : internalCluster().getNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().canContainData() == false
                && internalCluster().clusterService(nodeName).localNode().isMasterNode() == false) {
                return nodeName;
            }
        }
        return internalCluster().getNodeNames()[internalCluster().getNodeNames().length - 1];
    }

    private void createTsdbTemplate(String dataStreamName) throws IOException {
        // Use a long dimension so mapColumnBatch works in time_series mode without
        // the keyword SortedSet encoding that isn't yet columnar-supported.
        String mapping = """
            {
                "properties": {
                    "@timestamp": {
                        "type": "date"
                    },
                    "series_id": {
                        "type": "long",
                        "time_series_dimension": true
                    }
                }
            }
            """;
        var request = new TransportPutComposableIndexTemplateAction.Request(dataStreamName + "-template");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName + "*"))
                .template(
                    new Template(
                        Settings.builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0)
                            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                            .build(),
                        CompressedXContent.fromJSON(mapping),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
    }

    public void testTimestampOnlyTsdbColumnarBatchMode() throws IOException {
        String dataStreamName = "test-tsdb-batch-ds";
        createTsdbTemplate(dataStreamName);

        var createRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createRequest).actionGet());
        ensureGreen(dataStreamName);

        String coordinatingNode = findCoordinatingNode();
        int numDocs = randomIntBetween(10, 50);
        // Use current time so documents fall inside the backing index's auto-computed time range.
        Instant baseTime = Instant.now();

        // Warm-up bulk: the first batch may fall back to the row path while the mapping is being
        // established. Send it without MockLog assertions so the mapping settles.
        BulkRequest warmUp = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            warmUp.add(
                new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("@timestamp", baseTime.minusSeconds(numDocs - i).toEpochMilli(), "series_id", 1L))
            );
        }
        BulkResponse warmUpResponse = client(coordinatingNode).bulk(warmUp).actionGet();
        assertNoFailures(warmUpResponse);
        assertThat(warmUpResponse.getItems().length, equalTo(numDocs));

        // Main bulk: mapping is established; this batch must go through the columnar path.
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("@timestamp", baseTime.plusSeconds(i).toEpochMilli(), "series_id", 1L))
            );
        }

        final Logger batchLogger = LogManager.getLogger(ShardBatchIndexer.class);
        final Logger resolverLogger = LogManager.getLogger(ShardBatchMapper.class);
        final Level origBatchLevel = batchLogger.getLevel();
        final Level origResolverLevel = resolverLogger.getLevel();
        Loggers.setLevel(batchLogger, Level.TRACE);
        Loggers.setLevel(resolverLogger, Level.DEBUG);
        try (var mockLog = MockLog.capture(ShardBatchIndexer.class, ShardBatchMapper.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "tsdb columnar batch indexed on primary",
                    ShardBatchIndexer.class.getName(),
                    Level.TRACE,
                    "batch indexed * operations on primary shard *"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no columnar fallback", ShardBatchMapper.class.getName(), Level.DEBUG, "*disabled*")
            );

            BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
            assertNoFailures(bulkResponse);
            assertThat(bulkResponse.getItems().length, equalTo(numDocs));

            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(batchLogger, origBatchLevel);
            Loggers.setLevel(resolverLogger, origResolverLevel);
        }

        refresh(dataStreamName);

        // Both warm-up and main bulk docs must be visible.
        assertResponse(prepareSearch(dataStreamName).setSize(0).setTrackTotalHits(true), response -> {
            assertNoFailures(response);
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) numDocs * 2));
        });
    }
}
