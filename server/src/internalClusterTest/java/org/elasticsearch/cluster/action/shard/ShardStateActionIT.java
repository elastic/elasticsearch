/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.action.shard;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING;

public class ShardStateActionIT extends ESIntegTestCase {

    public void testDefaultReroutePriority() {
        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var loopingTask = new LoopingTask(masterClusterService, Priority.HIGH); // prevents all NORMAL or lower tasks from running
        loopingTask.start();

        final var indexName = randomIndexName();
        createIndex(indexName);
        awaitClusterState(cs -> cs.routingTable(ProjectId.DEFAULT).index(indexName).allShardsActive());
        loopingTask.stop();
        ensureGreen(indexName);
    }

    public void testReduceReroutePriorityWhenAllAssigned() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final var dataNodeNames = Iterators.toList(
            Iterators.map(internalCluster().getDataNodeInstances(ClusterService.class).iterator(), cs -> cs.localNode().getName())
        );
        final var dataNode0 = dataNodeNames.getFirst();
        final var dataNode1 = dataNodeNames.getLast();

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var loopingTask = new LoopingTask(masterClusterService, Priority.HIGH); // prevents all NORMAL or lower tasks from running
        try {
            updateClusterSettings(
                Settings.builder().put(ShardStateAction.SHARD_STARTED_REROUTE_ALL_ASSIGNED_PRIORITY.getKey(), Priority.NORMAL)
            );

            loopingTask.start();

            // Assigning the shards of the index for the first time does not block on any NORMAL/LOW tasks:
            final var indexName = randomIndexName();
            createIndex(indexName, indexSettings(2, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", dataNode0).build());
            awaitClusterState(
                cs -> cs.getRoutingNodes().node(cs.nodes().resolveNode(dataNode0).getId()).size() == 2
                    && cs.getRoutingNodes().node(cs.nodes().resolveNode(dataNode1).getId()).isEmpty()
                    && cs.routingTable(ProjectId.DEFAULT).index(indexName).allShardsActive()
            );

            // But moving the shards to the other node does block:
            updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", dataNode1));
            // Specifically, this triggers a new desired balance whose convergence causes a reconciliation which starts one of the shards
            // moving, but the other is throttled. When the first shard movement completes, the follow-up reroute runs at NORMAL and
            // loopingTask prevents it from running so it gets stuck here.
            awaitClusterState(
                cs -> cs.getRoutingNodes().node(cs.nodes().resolveNode(dataNode0).getId()).size() == 1
                    && cs.getRoutingNodes().node(cs.nodes().resolveNode(dataNode1).getId()).size() == 1
            );
            assertFalse(
                safeGet(
                    clusterAdmin().prepareHealth(SAFE_AWAIT_TIMEOUT, indexName)
                        .setWaitForNoInitializingShards(true)
                        .setWaitForNoRelocatingShards(true)
                        .execute()
                ).isTimedOut()
            );

            final var noInitializingState = masterClusterService.state();
            assertEquals(1, noInitializingState.getRoutingNodes().node(noInitializingState.nodes().resolveNode(dataNode0).getId()).size());
            assertEquals(1, noInitializingState.getRoutingNodes().node(noInitializingState.nodes().resolveNode(dataNode1).getId()).size());
        } finally {
            loopingTask.stop();
            // Allows the reroute to proceed.
            try {
                awaitClusterState(
                    cs -> cs.getRoutingNodes().node(cs.nodes().resolveNode(dataNode0).getId()).isEmpty()
                        && cs.getRoutingNodes().node(cs.nodes().resolveNode(dataNode1).getId()).size() == 2
                );
            } finally {
                updateClusterSettings(Settings.builder().putNull(ShardStateAction.SHARD_STARTED_REROUTE_ALL_ASSIGNED_PRIORITY.getKey()));
            }
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 1)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), TestPlugin.class);
    }

    public static class TestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                ShardStateAction.SHARD_STARTED_REROUTE_ALL_ASSIGNED_PRIORITY,
                ShardStateAction.SHARD_STARTED_REROUTE_SOME_UNASSIGNED_PRIORITY
            );
        }
    }

    private static class LoopingTask implements ClusterStateTaskExecutor<LoopingTask>, ClusterStateTaskListener {

        private final MasterServiceTaskQueue<LoopingTask> queue;
        private int iteration;
        private volatile boolean keepGoing = true;

        LoopingTask(ClusterService clusterService, Priority priority) {
            queue = clusterService.createTaskQueue("looping at " + priority, priority, this);
        }

        void start() {
            if (keepGoing) {
                queue.submitTask("iteration " + (iteration++), this, null);
            }
        }

        @Override
        public ClusterState execute(BatchExecutionContext<LoopingTask> batchExecutionContext) {
            final var onCompletion = new RunOnce(this::start);
            for (var taskContext : batchExecutionContext.taskContexts()) {
                taskContext.success(onCompletion);
            }
            return batchExecutionContext.initialState();
        }

        @Override
        public void onFailure(Exception e) {
            fail(e);
        }

        void stop() {
            keepGoing = false;
        }

        @Override
        public String toString() {
            return "looping task";
        }

        @Override
        public String describeTasks(List<LoopingTask> tasks) {
            return "";
        }
    }
}
