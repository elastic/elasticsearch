/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end test that verifies {@code WildcardFieldMapper} uses the columnar batch-indexing fast
 * path in a COLUMNAR index. Modelled after {@code BatchBulkTSDBIT} in the data-streams module,
 * since {@code BatchBulkIT} (server) has no dependency on the wildcard plugin.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 1)
public class BatchBulkWildcardIT extends ESIntegTestCase {

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
        return List.of(Wildcard.class);
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

    public void testWildcardFieldColumnarBatchMode() throws Exception {
        final String index = "test-wildcard-columnar";
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "path": { "type": "wildcard" }
              }
            }
            """;
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping(mapping)
        );
        ensureGreen(index);

        final String coordinatingNode = findCoordinatingNode();
        final int numDocs = randomIntBetween(20, 100);

        BulkRequest warmUp = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            warmUp.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .id("w" + i)
                    .source(Map.of("path", "/var/log/app-" + i + ".log"), XContentType.JSON)
            );
        }
        BulkResponse warmUpResponse = client(coordinatingNode).bulk(warmUp).actionGet();
        assertNoFailures(warmUpResponse);
        assertThat(warmUpResponse.getItems().length, equalTo(numDocs));

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .id("b" + i)
                    .source(Map.of("path", "/opt/service/log-" + i + ".txt"), XContentType.JSON)
            );
        }

        assertBulkTakesColumnarPath(coordinatingNode, bulkRequest, numDocs, "wildcard columnar batch indexed on primary");

        refresh(index);

        assertResponse(prepareSearch(index).setSize(0).setTrackTotalHits(true), response -> {
            assertNoFailures(response);
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) numDocs * 2));
        });

        assertResponse(
            prepareSearch(index).setSize(0).setTrackTotalHits(true).setQuery(new WildcardQueryBuilder("path", "*log-*.txt")),
            response -> {
                assertNoFailures(response);
                assertThat(response.getHits().getTotalHits().value(), equalTo((long) numDocs));
            }
        );

        assertResponse(
            prepareSearch(index).setSize(0).setTrackTotalHits(true).setQuery(new RegexpQueryBuilder("path", ".*/service/.*")),
            response -> {
                assertNoFailures(response);
                assertThat(response.getHits().getTotalHits().value(), equalTo((long) numDocs));
            }
        );

        GetResponse getResponse = client().prepareGet(index, "b0").get();
        assertTrue("batch-indexed document must be retrievable by id", getResponse.isExists());
        assertEquals("/opt/service/log-0.txt", getResponse.getSource().get("path"));
    }

    private void assertBulkTakesColumnarPath(String coordinatingNode, BulkRequest bulkRequest, int expectedItems, String description) {
        withBatchLoggingEnabled(mockLog -> {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    description,
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
            assertThat(bulkResponse.getItems().length, equalTo(expectedItems));
        });
    }

    private void withBatchLoggingEnabled(Consumer<MockLog> body) {
        final Logger batchLogger = LogManager.getLogger(ShardBatchIndexer.class);
        final Logger resolverLogger = LogManager.getLogger(ShardBatchMapper.class);
        final Level origBatchLevel = batchLogger.getLevel();
        final Level origResolverLevel = resolverLogger.getLevel();
        Loggers.setLevel(batchLogger, Level.TRACE);
        Loggers.setLevel(resolverLogger, Level.DEBUG);
        try (var mockLog = MockLog.capture(ShardBatchIndexer.class, ShardBatchMapper.class)) {
            body.accept(mockLog);
            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(batchLogger, origBatchLevel);
            Loggers.setLevel(resolverLogger, origResolverLevel);
        }
    }
}
