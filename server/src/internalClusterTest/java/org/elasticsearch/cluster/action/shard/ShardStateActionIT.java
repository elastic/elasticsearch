/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.action.shard;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.coordination.NodeJoinExecutor;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;

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
                Settings.builder().put(ShardStartedTaskExecutor.SHARD_STARTED_REROUTE_ALL_ASSIGNED_PRIORITY.getKey(), Priority.NORMAL)
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
                updateClusterSettings(
                    Settings.builder().putNull(ShardStartedTaskExecutor.SHARD_STARTED_REROUTE_ALL_ASSIGNED_PRIORITY.getKey())
                );
            }
        }
    }

    /**
     * During a node replacement, the shards leaving the departing node finish recovering one after another, and each one sends a
     * {@code shard-started} update. If these arrive faster than the master processes them, it never gets to tasks below
     * {@link ShardStateAction#SHARD_STARTED_TASK_PRIORITY}. The reroute after a node join should not be blocked by them,
     * because it is important to get the cluster back to a balanced state.
     */
    public void testShardStartedUpdatesDoesNotStarvePostNodeJoinReroute() throws Exception {
        final var shardCount = randomIntBetween(1, 5);
        final var dataNode = internalCluster().startDataOnlyNode();
        final var masterNode = internalCluster().getMasterName();
        final var masterClusterService = internalCluster().clusterService(masterNode);
        final var masterService = masterClusterService.getMasterService();
        final var masterTransportService = MockTransportService.getInstance(masterNode);
        final var indexName = randomIndexName();
        final var heldUpdates = new HeldShardStartedUpdates();
        final var updatesAheadOfReroute = new AtomicInteger();

        // The master applies each new cluster state before it completes the batch that produced it, so this listener runs inside that
        // batch. Releasing one held shard there, first in the node-join batch and then in each shard-started batch, leaves an update
        // waiting every time the master picks what to run next, just as a steady stream of completing recoveries does.
        final ClusterStateListener releaseOneShardPerBatch = event -> {
            final var shardStarted = startedShardCount(event.state(), indexName) > startedShardCount(event.previousState(), indexName);
            if (shardStarted && isPostNodeJoinReroutePending(masterService)) {
                updatesAheadOfReroute.incrementAndGet();
            }
            if (event.nodesAdded() || shardStarted) {
                heldUpdates.releaseNextShard();
            }
        };

        masterTransportService.<StartedShardEntry>addRequestHandlingBehavior(
            ShardStateAction.SHARD_STARTED_ACTION_NAME,
            (handler, request, channel, task) -> heldUpdates.receive(request.shardId, () -> {
                try {
                    handler.messageReceived(request, channel, task);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            })
        );
        try {
            // Let the data node recover every shard at once, so that all their updates are waiting to be released.
            updateClusterSettings(
                Settings.builder().put(CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), shardCount)
            );
            prepareCreate(indexName).setSettings(
                indexSettings(shardCount, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", dataNode)
            ).setWaitForActiveShards(ActiveShardCount.NONE).get();
            assertBusy(() -> assertThat(heldUpdates.heldShardCount(), equalTo(shardCount)));

            masterClusterService.addListener(releaseOneShardPerBatch);
            internalCluster().startDataOnlyNode();
            ensureGreen(indexName);
        } finally {
            masterClusterService.removeListener(releaseOneShardPerBatch);
            masterTransportService.clearInboundRules();
            heldUpdates.releaseAll(); // only needed if the test failed part-way, so that the cluster can shut down cleanly
            updateClusterSettings(
                Settings.builder().putNull(CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey())
            );
        }

        assertThat("shard-started updates that ran while the post-join reroute was still pending", updatesAheadOfReroute.get(), equalTo(0));
    }

    private static long startedShardCount(ClusterState state, String indexName) {
        final var routingTable = state.routingTable(ProjectId.DEFAULT);
        return routingTable.hasIndex(indexName) ? routingTable.allShards(indexName).stream().filter(ShardRouting::started).count() : 0;
    }

    private static boolean isPostNodeJoinReroutePending(MasterService masterService) {
        return masterService.pendingTasks().stream().anyMatch(task -> task.getSource().contains(NodeJoinExecutor.POST_JOIN_REROUTE_REASON));
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
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.addAll(List.of(TestPlugin.class, MockTransportService.TestPlugin.class));
        return plugins;
    }

    public static class TestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                ShardStartedTaskExecutor.SHARD_STARTED_REROUTE_ALL_ASSIGNED_PRIORITY,
                ShardStartedTaskExecutor.SHARD_STARTED_REROUTE_SOME_UNASSIGNED_PRIORITY
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

    /**
     * Holds the {@code shard-started} updates that the master receives, grouped by shard, until the test releases them a shard at a time.
     * A data node re-sends the update for a recovered shard each time it applies a new cluster state, so holding by shard rather than by
     * request keeps all of a shard's updates back until the test decides that shard has finished recovering.
     */
    private static class HeldShardStartedUpdates {
        private final Map<ShardId, List<Runnable>> heldByShard = new LinkedHashMap<>();
        private final Set<ShardId> releasedShards = new HashSet<>();

        void receive(ShardId shardId, Runnable update) {
            synchronized (this) {
                if (releasedShards.contains(shardId) == false) {
                    heldByShard.computeIfAbsent(shardId, ignored -> new ArrayList<>()).add(update);
                    return;
                }
            }
            update.run();
        }

        synchronized int heldShardCount() {
            return heldByShard.size();
        }

        void releaseNextShard() {
            final List<Runnable> updates;
            synchronized (this) {
                final var iterator = heldByShard.entrySet().iterator();
                if (iterator.hasNext() == false) {
                    return;
                }
                final var next = iterator.next();
                iterator.remove();
                releasedShards.add(next.getKey());
                updates = next.getValue();
            }
            updates.forEach(Runnable::run);
        }

        void releaseAll() {
            while (heldShardCount() > 0) {
                releaseNextShard();
            }
        }
    }
}
