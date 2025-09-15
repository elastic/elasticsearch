/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.reshard.ReshardIndexRequest;
import co.elastic.elasticsearch.stateless.reshard.TransportReshardAction;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.node.NodeRoleSettings;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that resharding operations are resilient in presence of failures.
 */
public class StatelessReshardDisruptionIT extends AbstractStatelessIntegTestCase {
    public void testReshardWithDisruption() throws IOException, InterruptedException {
        var masterNode = startMasterOnlyNode();

        int nodes = randomIntBetween(2, 5);
        startIndexNodes(nodes);
        startSearchNodes(nodes);

        int clusterSize = nodes * 2 + 1;
        ensureStableCluster(clusterSize);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        Index index = resolveIndex(indexName);

        checkNumberOfShardsSetting(indexName, 1);

        var multiple = randomIntBetween(2, 5);

        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        // We currently need to flush all indexed data in order for copy logic to see it.
        // This will be included in later stages of resharding that currently don't exist.
        var flushResponse = indicesAdmin().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
        assertNoFailures(flushResponse);

        try (var disruptionExecutorService = Executors.newSingleThreadExecutor()) {
            logger.info("--> start inducing failures");

            var failuresStarted = new CountDownLatch(1);
            var failuresStopped = new CountDownLatch(1);
            var stop = new AtomicBoolean(false);
            disruptionExecutorService.submit(() -> {
                failuresStarted.countDown();
                do {
                    Failure randomFailure = randomFrom(Failure.values());
                    try {
                        induceFailure(randomFailure, index, clusterSize, multiple);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } while (stop.get() == false);
                failuresStopped.countDown();
            });

            failuresStarted.await();

            try {
                logger.info("--> resharding an index [{}] under disruption", indexName);
                assertAcked(
                    client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple))
                        .actionGet(TEST_REQUEST_TIMEOUT)
                );
                waitForReshardCompletion(indexName);
                logger.info("--> done resharding an index [{}]", indexName);
            } finally {
                stop.set(true);
                failuresStopped.await();
            }

            checkNumberOfShardsSetting(indexName, multiple);

            ensureGreen(indexName);

            // Explicit refresh currently needed because it is not done in scope of split.
            refresh(indexName);

            // All data movement is done properly.
            var search = prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true).setSize(numDocs * 2);

            assertResponse(search, r -> { assertEquals(numDocs, r.getHits().getTotalHits().value()); });
        }
    }

    @Override
    protected boolean addMockFsRepository() {
        // Use FS repository because it supports blob copy
        return false;
    }

    // Inspired by StatelessTranslogIT.
    private void induceFailure(Failure failure, Index index, int clusterSize, int shardCount) throws Exception {
        switch (failure) {
            case RESTART -> {
                String nodeToRestart = indexingNode();
                logger.info("--> restarting node [{}]", nodeToRestart);
                internalCluster().restartNode(nodeToRestart);
                ensureStableCluster(clusterSize);
            }
            case REPLACE_FAILED_NODE -> {
                String nodeToReplace = indexingNode();
                logger.info("--> replacing node [{}]", nodeToReplace);
                internalCluster().stopNode(nodeToReplace);
                startIndexNode();
                ensureStableCluster(clusterSize);
            }
            case LOCAL_FAIL_SHARD -> {
                try {
                    IndexShard indexShard = findIndexShard(index, randomIntBetween(0, shardCount));
                    logger.info("--> failing shard {}", indexShard.shardId());
                    indexShard.failShard("broken", new Exception("boom local"));
                    ensureGreen(index.getName());
                } catch (AssertionError e) {
                    // Unlucky, shard does not exist yet.
                }
            }
        }
    }

    private static String indexingNode() {
        return Stream.generate(() -> internalCluster().getNodeNameThat(settings -> {
            List<DiscoveryNodeRole> discoveryNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings);
            return discoveryNodeRoles.contains(DiscoveryNodeRole.INDEX_ROLE);
        })).findFirst().get();
    }

    private enum Failure {
        RESTART,
        REPLACE_FAILED_NODE,
        LOCAL_FAIL_SHARD
    }

    private static void checkNumberOfShardsSetting(String indexName, int expected_shards) {
        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client().admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(expected_shards)
        );
    }

    private void waitForReshardCompletion(String indexName) {
        awaitClusterState((state) -> state.projectState().metadata().index(indexName).getReshardingMetadata() == null);
    }
}
