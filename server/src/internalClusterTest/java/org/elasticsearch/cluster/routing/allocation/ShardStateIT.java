/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;

public class ShardStateIT extends ESIntegTestCase {

    public void testPrimaryFailureIncreasesTerm() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("test").setSettings(indexSettings(2, 1)).get();
        ensureGreen();
        assertPrimaryTerms(1, 1);

        logger.info("--> disabling allocation to capture shard failure");
        disableAllocation("test");

        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final int shard = randomBoolean() ? 0 : 1;
        final String nodeId = state.routingTable().index("test").shard(shard).primaryShard().currentNodeId();
        final String node = state.nodes().get(nodeId).getName();
        logger.info("--> failing primary of [{}] on node [{}]", shard, node);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        indicesService.indexService(resolveIndex("test")).getShard(shard).failShard("simulated test failure", null);

        logger.info("--> waiting for a yellow index");
        // we can't use ensureYellow since that one is just as happy with a GREEN status.
        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, "test").get().getStatus(),
                equalTo(ClusterHealthStatus.YELLOW)
            )
        );

        final long term0 = shard == 0 ? 2 : 1;
        final long term1 = shard == 1 ? 2 : 1;
        assertPrimaryTerms(term0, term1);

        logger.info("--> enabling allocation");
        enableAllocation("test");
        ensureGreen();
        assertPrimaryTerms(term0, term1);
    }

    protected void assertPrimaryTerms(long shard0Term, long shard1Term) {
        for (String node : internalCluster().getNodeNames()) {
            logger.debug("--> asserting primary terms terms on [{}]", node);
            ClusterState state = client(node).admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).setLocal(true).get().getState();
            IndexMetadata metadata = state.metadata().getProject().index("test");
            assertThat(metadata.primaryTerm(0), equalTo(shard0Term));
            assertThat(metadata.primaryTerm(1), equalTo(shard1Term));
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexService = indicesService.indexService(metadata.getIndex());
            if (indexService != null) {
                for (IndexShard shard : indexService) {
                    assertThat(
                        "term mismatch for shard " + shard.shardId(),
                        shard.getPendingPrimaryTerm(),
                        equalTo(metadata.primaryTerm(shard.shardId().id()))
                    );
                }
            }
        }
    }

    public void testGetPendingTasksSourceStringDataForFailedAndStartedShards() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        prepareCreate("test").setSettings(indexSettings(1, 0)).get();
        ensureGreen();

        final var masterNodeClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var barrier = new CyclicBarrier(2);

        final var finalBlockingQueue = masterNodeClusterService.createTaskQueue("final-block", Priority.NORMAL, batchExecutionContext -> {
            safeAwait(barrier);
            batchExecutionContext.taskContexts().forEach(c -> c.success(() -> {}));
            return batchExecutionContext.initialState();
        });

        masterNodeClusterService.createTaskQueue("initial-block", Priority.NORMAL, batchExecutionContext -> {
            safeAwait(barrier);
            safeAwait(barrier);
            batchExecutionContext.taskContexts().forEach(c -> c.success(() -> {}));
            // Submit the final blocking task before exiting so that it will be queued before the expected shard-started task.
            finalBlockingQueue.submitTask("final-block", ignored -> {}, null);
            return batchExecutionContext.initialState();
        }).submitTask("initial-block", ignored -> {}, null);

        // Sync up with our initial blocking executor.
        safeAwait(barrier);

        // Obtain a reference to the IndexShard for shard 0.
        final var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final var shard0RoutingTable = state.routingTable().index("test").shard(0);
        assertNotNull(shard0RoutingTable);
        final var nodeId = shard0RoutingTable.primaryShard().currentNodeId();
        final var node = state.nodes().get(nodeId).getName();
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard0 = indicesService.indexService(resolveIndex("test")).getShard(0);
        assertNotNull(shard0);

        // Create a failed shard state action for shard 0.
        final var shardFailedReason = "simulated test failure";
        final var shardFailedException = new ElasticsearchException("simulated exception");
        shard0.failShard(shardFailedReason, shardFailedException);

        // Get the pending tasks and verify we see the shard-failed state action and expected source string components.
        final var masterService = masterNodeClusterService.getMasterService();
        assertBusy(() -> {
            assertTrue(masterService.pendingTasks().stream().anyMatch(task -> {
                final var src = task.getSource().string();
                // We expect the failure reason and exception message, but not the stack trace.
                return src.startsWith("shard-failed ")
                    && src.contains("[test][0]")
                    && src.contains(shardFailedReason)
                    && src.contains(shardFailedException.getMessage())
                    && src.contains(ExceptionsHelper.stackTrace(shardFailedException)) == false;
            }));
        });

        // Unblock the master service from the initial-block executor and allow the failed shard task to get processed.
        safeAwait(barrier);

        // Wait for recovery and a shard-started pending task for shard 0.
        assertBusy(() -> {
            assertTrue(masterService.pendingTasks().stream().anyMatch(task -> {
                final var src = task.getSource().string();
                return src.startsWith("shard-started ") && src.contains("[test][0]") && src.contains("after existing store recovery");
            }));
        });

        // Unblock the master service and wait for all tasks to clear and the cluster to complete the recovery.
        safeAwait(barrier);
        assertBusy(() -> assertTrue(masterService.pendingTasks().isEmpty()));
        ensureGreen();
    }
}
