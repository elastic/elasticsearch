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

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ClusterServiceUtils;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that resharding operations are resilient in presence of failures.
 */
public class StatelessReshardDisruptionIT extends AbstractStatelessIntegTestCase {
    public void testReshardWithDisruption() throws IOException, InterruptedException {
        var masterNode = startMasterOnlyNode();

        int nodes = 2;
        startIndexNodes(nodes);
        startSearchNodes(nodes);

        int clusterSize = nodes * 2 + 1;
        ensureStableCluster(clusterSize);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        // TODO test initial number of shards other than 1
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        Index index = resolveIndex(indexName);

        checkNumberOfShardsSetting(indexName, 1);

        var multiple = 2;

        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

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
                client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple))
                    .actionGet(TEST_REQUEST_TIMEOUT);
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
                    var listener = ClusterServiceUtils.addTemporaryStateListener(
                        cs -> cs.routingTable().index(index.getName()).shard(indexShard.shardId().id()).primaryShard().unassigned()
                    );
                    logger.info("--> failing shard {}", indexShard.shardId());
                    indexShard.failShard("broken", new Exception("boom local"));
                    // ensureGreen may succeed before the cluster state reflects the failed shard
                    safeAwait(listener);
                    ensureGreen(index.getName());
                } catch (AssertionError e) {
                    // Unlucky, shard does not exist yet.
                }
            }
            case RELOCATE_SHARD -> {
                ensureGreen(index.getName());
                var shardId = randomIntBetween(0, shardCount);
                var clusterState = clusterService().state();
                var shardRoutingTable = clusterState.routingTable().index(index.getName()).shard(shardId);
                if (shardRoutingTable == null) {
                    // target shard doesn't exist yet, relocate source instead
                    assert shardId != 0;
                    shardId = 0;
                    shardRoutingTable = clusterState.routingTable().index(index.getName()).shard(shardId);
                }
                var fromNode = clusterState.getNodes().get(shardRoutingTable.primaryShard().currentNodeId()).getName();
                var eligibleNodes = clusterState.nodes()
                    .getAllNodes()
                    .stream()
                    .filter(node -> node.getName().equals(fromNode) == false && node.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()))
                    .map(DiscoveryNode::getName)
                    .toList();
                if (eligibleNodes.isEmpty() == false) {
                    var toNode = randomFrom(eligibleNodes);
                    logger.info("--> relocating shard {} from {} to {}", shardId, fromNode, toNode);
                    ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(index.getName(), shardId, fromNode, toNode));
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
        LOCAL_FAIL_SHARD,
        RELOCATE_SHARD,
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
