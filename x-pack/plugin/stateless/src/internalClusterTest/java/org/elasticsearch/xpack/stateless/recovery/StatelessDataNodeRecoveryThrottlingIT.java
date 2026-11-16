/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryInfo;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoveryPriority;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.IndexBalanceConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.MockIndexEventListener;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.allocation.StatelessThrottlingConcurrentRecoveriesAllocationDecider;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexRequest;
import org.elasticsearch.xpack.stateless.reshard.SplitSourceService;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardAction;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardSplitAction;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationHandoffAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;

/// Integration tests for the data node recovery throttling limits on stateless indexing and search nodes.
///
/// The heap of the test nodes depends on the test JVM, so the tests enabling a heap-based limit set their per-heap-GB
/// ratio to [#MIN_PER_HEAP_GB]. The heap-based limit `ceil(heapGb * perHeapGb)` is then 1 whatever the heap. Unit tests
/// in `ThrottlingRecoveryServiceTests` and `DataNodeRecoveryThrottlingSettingsTests` cover the limit computation correctness
/// for other values.
public class StatelessDataNodeRecoveryThrottlingIT extends AbstractStatelessPluginIntegTestCase {

    /// The smallest allowed per-heap-GB ratio, which deterministically sets heap-based limit to 1.
    private static final double MIN_PER_HEAP_GB = Double.MIN_NORMAL;

    /// Unthrottle master-side limits.
    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(
            ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(),
            Integer.MAX_VALUE
        )
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put(
                StatelessThrottlingConcurrentRecoveriesAllocationDecider.MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING
                    .getKey(),
                "0b"
            )
            .put(
                StatelessThrottlingConcurrentRecoveriesAllocationDecider.CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB.getKey(),
                Integer.MAX_VALUE
            )
            // Disable rebalancing to make the test recovery counts deterministic
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockIndexEventListener.TestPlugin.class);
        plugins.add(ShutdownPlugin.class);
        plugins.add(NotPreferredToRemainPlugin.class);
        return plugins;
    }

    @Override
    protected boolean addMockFsRepository() {
        // Use the FS repository because it supports the blob copy that reshard splits rely on
        return false;
    }

    @After
    public void clearNotPreferredToRemainNode() {
        NotPreferredToRemainDecider.nodeId = null;
        NotPreferredToRemainDecider.indexName = null;
    }

    @After
    public void verifyNoOutstandingRecoveries() {
        final Map<String, Predicate<RecoveryStats>> noCurrentRecoveries = new HashMap<>();
        for (final var nodeName : internalCluster().getNodeNames()) {
            noCurrentRecoveries.put(nodeName, RecoveryStats::noCurrentRecoveries);
        }
        awaitRecoveryCountStats(noCurrentRecoveries);
    }

    public void testIndexNodeQueuesIncomingRecoveriesPastConcurrencyLimit() {
        startMasterOnlyNode();
        final int maxConcurrentRecoveries = between(1, 5);
        final int nonCriticalPercent = between(1, 100);
        final boolean heapBasedLimit = randomBoolean();
        final int expectedMaxConcurrentRecoveries = heapBasedLimit ? 1 : maxConcurrentRecoveries;
        final int expectedNonCriticalLimit = expectedDataNodeNonCriticalLimit(expectedMaxConcurrentRecoveries, nonCriticalPercent);
        final int totalShards = expectedNonCriticalLimit + between(1, 3);

        final var sourceNode = startIndexNode();
        final var targetNode = startIndexNode(
            incomingDataNodeThrottlingSettings(maxConcurrentRecoveries, nonCriticalPercent, heapBasedLimit)
        );

        final var indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(totalShards, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode).build()
        );
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        // Stall all handoffs so that the running relocations keep their recovery slots.
        final var proceedWithHandoffs = new CountDownLatch(1);
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                safeAwait(proceedWithHandoffs);
                handler.messageReceived(request, channel, task);
            });

        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == expectedNonCriticalLimit
                    && stats.currentAsTargetQueued() == totalShards - expectedNonCriticalLimit
            )
        );

        // New primaries are critical recoveries, so they can use the slots that the non-critical limit leaves free.
        final int freeSlots = expectedMaxConcurrentRecoveries - expectedNonCriticalLimit;
        final int newShards = freeSlots + between(1, 2);
        final var newIndexName = randomIndexName();

        // Hold the new primaries' recoveries.
        final var releaseNewPrimaries = new SubscribableListener<Void>();
        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, targetNode).setNewDelegate(new IndexEventListener() {
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                if (indexShard.shardId().getIndexName().equals(newIndexName)) {
                    releaseNewPrimaries.addListener(listener);
                } else {
                    listener.onResponse(null);
                }
            }
        });
        prepareCreate(newIndexName).setSettings(
            indexSettings(newShards, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", targetNode)
        ).setWaitForActiveShards(ActiveShardCount.NONE).get();
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == expectedNonCriticalLimit
                    && stats.currentAsTargetQueued() == totalShards - expectedNonCriticalLimit
                    && stats.currentFromStore() == freeSlots
                    && stats.currentFromStoreQueued() == newShards - freeSlots
            )
        );

        releaseNewPrimaries.onResponse(null);
        proceedWithHandoffs.countDown();
        ensureGreen(indexName, newIndexName);
    }

    public void testIndexNodeQueuesOutgoingRecoveriesPastConcurrencyLimit() {
        startMasterOnlyNode();
        final int maxConcurrentRecoveries = between(1, 5);
        final boolean heapBasedLimit = randomBoolean();
        final int expectedLimit = heapBasedLimit ? 1 : maxConcurrentRecoveries;
        final int totalShards = expectedLimit + between(1, 3);

        final var sourceSettings = Settings.builder()
            .put(
                DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(),
                maxConcurrentRecoveries
            );
        if (heapBasedLimit) {
            sourceSettings.put(
                DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_PER_HEAP_GB_SETTING.getKey(),
                MIN_PER_HEAP_GB
            );
        }
        final var sourceNode = startIndexNode(sourceSettings.build());
        final var targetNode = startIndexNode();

        final var indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(totalShards, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode).build()
        );
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        // Stall all handoffs so that the running relocations keep their source slots.
        final var proceedWithHandoffs = new CountDownLatch(1);
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                safeAwait(proceedWithHandoffs);
                handler.messageReceived(request, channel, task);
            });

        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);
        awaitRecoveryCountStats(
            Map.of(
                sourceNode,
                stats -> stats.currentAsSource() == expectedLimit && stats.currentAsSourceQueued() == totalShards - expectedLimit
            )
        );

        proceedWithHandoffs.countDown();
        ensureGreen(indexName);
    }

    public void testSearchNodeQueuesIncomingRecoveriesPastConcurrencyLimit() {
        startMasterOnlyNode();
        final int maxConcurrentRecoveries = between(2, 6);
        // Expected non-critical limit is at least 2 (so we can ensure the heap-based limit, which is 1, does not apply)
        final int nonCriticalPercent = between(100 / maxConcurrentRecoveries + 1, 100);
        final int expectedNonCriticalLimit = expectedDataNodeNonCriticalLimit(maxConcurrentRecoveries, nonCriticalPercent);
        assertThat(expectedNonCriticalLimit, greaterThan(1));
        final int totalShards = expectedNonCriticalLimit + between(1, 3);

        final var indexNode = startIndexNode();
        final var sourceNode = startSearchNode();
        final var targetNode = startSearchNode(incomingDataNodeThrottlingSettings(maxConcurrentRecoveries, nonCriticalPercent, true));

        final var indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(totalShards, 1).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode).build()
        );
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        // Stall the commit registrations of the search shard recoveries on the indexing node, so that the running
        // relocations keep their target slots.
        final var proceedWithRegistrations = new CountDownLatch(1);
        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                safeAwait(proceedWithRegistrations);
                handler.messageReceived(request, channel, task);
            });

        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == expectedNonCriticalLimit
                    && stats.currentAsTargetQueued() == totalShards - expectedNonCriticalLimit
            )
        );

        // New search shards are critical recoveries, so they can use the slots that the non-critical limit leaves free.
        final int freeSlots = maxConcurrentRecoveries - expectedNonCriticalLimit;
        final int newShards = freeSlots + between(1, 2);
        final var newIndexName = randomIndexName();
        prepareCreate(newIndexName).setSettings(
            indexSettings(newShards, 1).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode)
        ).setWaitForActiveShards(ActiveShardCount.NONE).get();
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == maxConcurrentRecoveries
                    && stats.currentAsTargetQueued() == (totalShards - expectedNonCriticalLimit) + (newShards - freeSlots)
            )
        );

        proceedWithRegistrations.countDown();
        ensureGreen(indexName, newIndexName);
    }

    public void testMasterThrottlesApplyHeapBasedSettingsToIndexRelocations() throws Exception {
        startMasterOnlyNode();
        final int nodeConcurrentRecoveries = between(2, 5);
        final int totalShards = nodeConcurrentRecoveries + between(1, 2);

        final var sourceNode = startIndexNode();
        final var targetNode = startIndexNode();
        applyMasterThrottlingSettings(nodeConcurrentRecoveries, targetNode);

        final var indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(totalShards, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode).build()
        );
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        // Stall all handoffs so that the running relocations stay initializing on the target.
        final var proceedWithHandoffs = new CountDownLatch(1);
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                safeAwait(proceedWithHandoffs);
                handler.messageReceived(request, channel, task);
            });

        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);
        // The heap-based limit allows a single relocation, below `nodeConcurrentRecoveries`.
        awaitRecoveryCountStats(Map.of(targetNode, stats -> stats.currentAsTarget() == 1 && stats.currentAsTargetQueued() == 0));
        waitNoPendingTasksOnAll();

        final var state = internalCluster().clusterService().state();
        assertThat(state.getRoutingNodes().node(getNodeId(targetNode)).numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(1));

        proceedWithHandoffs.countDown();
        ensureGreen(indexName);
    }

    public void testMasterThrottlesDoNotApplyHeapBasedSettingsToSearchRelocations() throws Exception {
        startMasterOnlyNode();
        final int nodeConcurrentRecoveries = between(2, 5);
        final int totalShards = nodeConcurrentRecoveries + between(1, 2);

        final var indexNode = startIndexNode();
        final var sourceNode = startSearchNode();
        final var targetNode = startSearchNode();
        applyMasterThrottlingSettings(nodeConcurrentRecoveries, targetNode);

        final var indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(totalShards, 1).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode).build()
        );
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        // Stall the commit registrations of the search shard recoveries on the indexing node, so that the running
        // relocations stay initializing on the target.
        final var proceedWithRegistrations = new CountDownLatch(1);
        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                safeAwait(proceedWithRegistrations);
                handler.messageReceived(request, channel, task);
            });

        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);
        // Only the static per-node limits applies, the heap based setting is not applicable to search shard relocations.
        awaitRecoveryCountStats(
            Map.of(targetNode, stats -> stats.currentAsTarget() == nodeConcurrentRecoveries && stats.currentAsTargetQueued() == 0)
        );
        waitNoPendingTasksOnAll();

        final var state = internalCluster().clusterService().state();
        assertThat(
            state.getRoutingNodes().node(getNodeId(targetNode)).numberOfShardsWithState(ShardRoutingState.INITIALIZING),
            equalTo(nodeConcurrentRecoveries)
        );

        proceedWithRegistrations.countDown();
        ensureGreen(indexName);
    }

    /// Simulates a node evacuation during a large reshard: the split targets fill the incoming recovery slots of the
    /// node receiving the evacuated shards, and hold them while their split sources (on another node, so not cancelled
    /// by the evacuation) do the work. The relocations away from the shutting down node must take the next free slots,
    /// ahead of the queued split targets.
    public void testRelocationsAwayFromShuttingDownNodeAreNotStarvedBySplitTargets() throws Exception {
        startMasterOnlyNode();
        final int maxConcurrentRecoveries = between(1, 3);
        final int queuedSplits = between(1, 3);
        final int sourceShards = maxConcurrentRecoveries + queuedSplits;
        final int relocations = between(1, maxConcurrentRecoveries);

        final var shuttingDownNode = startIndexNode();
        final var splitSourceNode = startIndexNode(
            // The unowned documents grace period would otherwise delay the completion of the reshard by minutes
            Settings.builder().put(SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO).build()
        );
        final var targetNode = startIndexNode(incomingDataNodeThrottlingSettings(maxConcurrentRecoveries, 100, false));

        final var evacuatedIndex = randomIndexName();
        createIndex(
            evacuatedIndex,
            indexSettings(relocations, 0).put(
                IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name",
                splitSourceNode + "," + targetNode
            ).build()
        );
        // All the split sources are allocated to the split source node. Limiting the index to sourceShards shards per node then
        // keeps the split targets off the split source node, so they are all allocated to the target node.
        final var reshardedIndex = randomIndexName();
        createIndex(
            reshardedIndex,
            indexSettings(sourceShards, 0).put(
                IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name",
                shuttingDownNode + "," + targetNode
            ).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), sourceShards).build()
        );
        indexDocs(evacuatedIndex, between(1, 50));
        indexDocs(reshardedIndex, between(1, 50));
        flush(evacuatedIndex, reshardedIndex);
        ensureGreen(evacuatedIndex, reshardedIndex);

        // From now on, both the evacuated shards and the split targets may only be allocated to the target node. Rebalancing is
        // disabled (see nodeSettings), so the existing shards stay where they are until the shutdown forces the evacuated ones to move.
        updateIndexSettings(
            Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", splitSourceNode),
            evacuatedIndex
        );
        updateIndexSettings(
            Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", shuttingDownNode),
            reshardedIndex
        );

        // Stall the splits on the source node, so that the split targets keep their recovery slots on the target node
        final var stallSplits = new AtomicBoolean(true);
        final var stalledSplits = new LinkedBlockingQueue<CheckedRunnable<Exception>>();
        final var allRunningSplitsStalled = new CountDownLatch(maxConcurrentRecoveries);
        MockTransportService.getInstance(splitSourceNode)
            .addRequestHandlingBehavior(TransportReshardSplitAction.START_SPLIT_ACTION_NAME, (handler, request, channel, task) -> {
                synchronized (stalledSplits) {
                    if (stallSplits.get()) {
                        stalledSplits.add(() -> handler.messageReceived(request, channel, task));
                        allRunningSplitsStalled.countDown();
                        return;
                    }
                }
                handler.messageReceived(request, channel, task);
            });
        final var splitSourceExecutor = internalCluster().getInstance(ThreadPool.class, splitSourceNode).generic();
        final Runnable releaseOneSplit = () -> {
            final var stalledSplit = stalledSplits.poll();
            assertNotNull(stalledSplit);
            splitSourceExecutor.execute(() -> {
                try {
                    stalledSplit.run();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
        };

        // Stall the handoffs of the relocations, so that the relocations keep their recovery slots once started. The handoffs
        // are queued rather than blocking the handler thread, so they cannot time out however long the test takes to release them.
        final var stallHandoffs = new AtomicBoolean(true);
        final var stalledHandoffs = new ConcurrentLinkedQueue<CheckedRunnable<Exception>>();
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                synchronized (stalledHandoffs) {
                    if (stallHandoffs.get()) {
                        stalledHandoffs.add(() -> handler.messageReceived(request, channel, task));
                        return;
                    }
                }
                handler.messageReceived(request, channel, task);
            });
        final var targetExecutor = internalCluster().getInstance(ThreadPool.class, targetNode).generic();

        safeGet(client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(reshardedIndex)));
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentFromStore() == maxConcurrentRecoveries && stats.currentFromStoreQueued() == queuedSplits
            )
        );
        safeAwait(allRunningSplitsStalled);
        assertThat(stalledSplits.size(), equalTo(maxConcurrentRecoveries));

        // Shut down the node, so its shards must relocate to the target node, where they queue as all the slots are taken by splits
        safeGet(
            clusterAdmin().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    getNodeId(shuttingDownNode),
                    SIGTERM,
                    "test",
                    null,
                    null,
                    TimeValue.timeValueMinutes(1)
                )
            )
        );
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == 0
                    && stats.currentAsTargetQueued() == relocations
                    && stats.currentFromStore() == maxConcurrentRecoveries
                    && stats.currentFromStoreQueued() == queuedSplits
            )
        );

        // Let some splits complete: the slots they free must go to the relocations, not to the queued splits
        final int releasedSplits = between(1, relocations);
        for (int i = 0; i < releasedSplits; i++) {
            releaseOneSplit.run();
        }
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == releasedSplits
                    && stats.currentAsTargetQueued() == relocations - releasedSplits
                    && stats.currentFromStore() == maxConcurrentRecoveries - releasedSplits
                    && stats.currentFromStoreQueued() == queuedSplits
            )
        );

        // Release everything and let the reshard and the evacuation complete
        synchronized (stalledSplits) {
            stallSplits.set(false);
        }
        while (stalledSplits.isEmpty() == false) {
            releaseOneSplit.run();
        }
        synchronized (stalledHandoffs) {
            stallHandoffs.set(false);
        }
        stalledHandoffs.forEach(handoff -> targetExecutor.execute(() -> {
            try {
                handoff.run();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }));
        awaitClusterState(state -> state.projectState().metadata().index(reshardedIndex).getReshardingMetadata() == null);
        ensureGreen(evacuatedIndex, reshardedIndex);
        final var state = internalCluster().clusterService().state();
        assertThat(state.getRoutingNodes().node(getNodeId(shuttingDownNode)).size(), equalTo(0));
        internalCluster().stopNode(shuttingDownNode);
    }

    /// Same as [#testRelocationsAwayFromShuttingDownNodeAreNotStarvedBySplitTargets], except that the relocations are created
    /// before their source node is marked for shutdown, so the master assigns them a priority below the split targets. The
    /// target node should then raise the priority of the queued relocations once their source node is marked for removal.
    public void testRelocationsStartedBeforeSourceNodeShutdownAreNotStarvedBySplitTargets() throws Exception {
        startMasterOnlyNode();
        final int maxConcurrentRecoveries = between(1, 3);
        final int queuedSplits = between(1, 3);
        final int sourceShards = maxConcurrentRecoveries + queuedSplits;
        final int relocations = between(1, maxConcurrentRecoveries);

        final var shuttingDownNode = startIndexNode();
        final var splitSourceNode = startIndexNode(
            // The unowned documents grace period would otherwise delay the completion of the reshard by minutes
            Settings.builder().put(SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO).build()
        );
        final var targetNode = startIndexNode(incomingDataNodeThrottlingSettings(maxConcurrentRecoveries, 100, false));

        final var evacuatedIndex = randomIndexName();
        createIndex(
            evacuatedIndex,
            indexSettings(relocations, 0).put(
                IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name",
                splitSourceNode + "," + targetNode
            ).build()
        );
        // All the split sources are allocated to the split source node. Limiting the index to sourceShards shards per
        // node then keeps the split targets off the split source node, so they are all allocated to the target node.
        final var reshardedIndex = randomIndexName();
        createIndex(
            reshardedIndex,
            indexSettings(sourceShards, 0).put(
                IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name",
                shuttingDownNode + "," + targetNode
            ).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), sourceShards).build()
        );
        indexDocs(evacuatedIndex, between(1, 50));
        indexDocs(reshardedIndex, between(1, 50));
        flush(evacuatedIndex, reshardedIndex);
        ensureGreen(evacuatedIndex, reshardedIndex);

        // From now on, both the evacuated shards and the split targets may only be allocated to the target node. Rebalancing is
        // disabled (see nodeSettings), so the existing shards stay where they are until the shutdown forces the evacuated ones to move.
        updateIndexSettings(
            Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", splitSourceNode),
            evacuatedIndex
        );
        updateIndexSettings(
            Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", shuttingDownNode),
            reshardedIndex
        );

        // Stall the splits on the source node, so that the split targets keep their recovery slots on the target node
        final var stallSplits = new AtomicBoolean(true);
        final var stalledSplits = new LinkedBlockingQueue<CheckedRunnable<Exception>>();
        final var allRunningSplitsStalled = new CountDownLatch(maxConcurrentRecoveries);
        MockTransportService.getInstance(splitSourceNode)
            .addRequestHandlingBehavior(TransportReshardSplitAction.START_SPLIT_ACTION_NAME, (handler, request, channel, task) -> {
                synchronized (stalledSplits) {
                    if (stallSplits.get()) {
                        stalledSplits.add(() -> handler.messageReceived(request, channel, task));
                        allRunningSplitsStalled.countDown();
                        return;
                    }
                }
                handler.messageReceived(request, channel, task);
            });
        final var splitSourceExecutor = internalCluster().getInstance(ThreadPool.class, splitSourceNode).generic();
        final Runnable releaseOneSplit = () -> {
            final var stalledSplit = stalledSplits.poll();
            assertNotNull(stalledSplit);
            splitSourceExecutor.execute(() -> {
                try {
                    stalledSplit.run();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
        };

        // Stall the handoffs of the relocations, so that the relocations keep their recovery slots once started.
        final var stallHandoffs = new AtomicBoolean(true);
        final var stalledHandoffs = new ConcurrentLinkedQueue<CheckedRunnable<Exception>>();
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                synchronized (stalledHandoffs) {
                    if (stallHandoffs.get()) {
                        stalledHandoffs.add(() -> handler.messageReceived(request, channel, task));
                        return;
                    }
                }
                handler.messageReceived(request, channel, task);
            });
        final var targetExecutor = internalCluster().getInstance(ThreadPool.class, targetNode).generic();

        safeGet(client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(reshardedIndex)));
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentFromStore() == maxConcurrentRecoveries && stats.currentFromStoreQueued() == queuedSplits
            )
        );
        safeAwait(allRunningSplitsStalled);
        assertThat(stalledSplits.size(), equalTo(maxConcurrentRecoveries));

        // The evacuated shards are not preferred on their node, so they relocate to the target node with a priority
        // below the split targets, and queue behind them
        NotPreferredToRemainDecider.nodeId = getNodeId(shuttingDownNode);
        ClusterRerouteUtils.reroute(client());
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == 0
                    && stats.currentAsTargetQueued() == relocations
                    && stats.currentFromStore() == maxConcurrentRecoveries
                    && stats.currentFromStoreQueued() == queuedSplits
            )
        );
        // Before the shutdown, the relocations have the priority the master assigned to them, which is below the split targets
        final var prioritiesBeforeShutdown = getRelocationPriorities(evacuatedIndex, targetNode, relocations);
        assertThat(prioritiesBeforeShutdown.size(), equalTo(relocations));
        assertThat(prioritiesBeforeShutdown.values(), everyItem(equalTo(RecoveryPriority.RELOCATION_CAN_REMAIN_NOT_PREFERRED)));

        // Shut down the source node of the relocations, which are still queued on the target node behind the split targets.
        // The put-shutdown response is only sent once every node has applied the shutdown, and the target node raises the
        // priority of its queued relocations while applying it.
        triggerSigtermShutdown(shuttingDownNode);
        final var priorities = getRelocationPriorities(evacuatedIndex, targetNode, relocations);
        assertThat(priorities.size(), equalTo(relocations));
        assertThat(priorities.values(), everyItem(equalTo(RecoveryPriority.RELOCATION_CAN_REMAIN_NO)));
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == 0
                    && stats.currentAsTargetQueued() == relocations
                    && stats.currentFromStore() == maxConcurrentRecoveries
                    && stats.currentFromStoreQueued() == queuedSplits
            )
        );

        // Let some splits complete: the slots they free must go to the relocations, not to the queued splits
        final int releasedSplits = between(1, relocations);
        for (int i = 0; i < releasedSplits; i++) {
            releaseOneSplit.run();
        }
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == releasedSplits
                    && stats.currentAsTargetQueued() == relocations - releasedSplits
                    && stats.currentFromStore() == maxConcurrentRecoveries - releasedSplits
                    && stats.currentFromStoreQueued() == queuedSplits
            )
        );

        // Release everything and let the reshard and the evacuation complete
        synchronized (stalledSplits) {
            stallSplits.set(false);
        }
        while (stalledSplits.isEmpty() == false) {
            releaseOneSplit.run();
        }
        synchronized (stalledHandoffs) {
            stallHandoffs.set(false);
        }
        stalledHandoffs.forEach(handoff -> targetExecutor.execute(() -> {
            try {
                handoff.run();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }));
        awaitClusterState(state -> state.projectState().metadata().index(reshardedIndex).getReshardingMetadata() == null);
        ensureGreen(evacuatedIndex, reshardedIndex);
        final var state = internalCluster().clusterService().state();
        assertThat(state.getRoutingNodes().node(getNodeId(shuttingDownNode)).size(), equalTo(0));
        internalCluster().stopNode(shuttingDownNode);
    }

    /// A relocation that starts before its source node is marked for removal keeps, in the routing table, the priority that
    /// the master assigned to it. Once its source node is marked for removal, the target node raises the priority of the
    /// relocations still queued on it, and the recovery API reports the raised priority. The relocation that had already
    /// started keeps its priority.
    public void testRecoveryApiReportsRaisedPriorityOfQueuedRelocations() throws Exception {
        startMasterOnlyNode();
        final int queuedRelocations = between(1, 3);
        final int relocations = 1 + queuedRelocations;
        final var sourceNode = startIndexNode();
        // A single recovery slot, which the first relocation to start holds until its handoff is released
        final var targetNode = startIndexNode(incomingDataNodeThrottlingSettings(1, 100, false));
        updateClusterSettings(Settings.builder().put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), false));

        // The shards of this index are made not preferred on the source node, so that they all relocate before the shutdown
        final var notPreferredIndexName = randomIndexName();
        // The shards of this index stay on the source node until the shutdown forces them off it.
        final var evacuatedIndexName = randomIndexName();
        final int forcedRelocations = between(1, 3);
        createIndex(
            notPreferredIndexName,
            indexSettings(relocations, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode).build()
        );
        createIndex(
            evacuatedIndexName,
            indexSettings(forcedRelocations, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode).build()
        );
        indexDocs(notPreferredIndexName, between(1, 50));
        indexDocs(evacuatedIndexName, between(1, 50));
        flush(notPreferredIndexName, evacuatedIndexName);
        ensureGreen(notPreferredIndexName, evacuatedIndexName);
        updateIndexSettings(
            Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name"),
            notPreferredIndexName,
            evacuatedIndexName
        );

        // Stall the handoffs of the relocations, so that the relocations keep their recovery slots once started.
        final var stallHandoffs = new AtomicBoolean(true);
        final var stalledHandoffs = new ConcurrentLinkedQueue<CheckedRunnable<Exception>>();
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                synchronized (stalledHandoffs) {
                    if (stallHandoffs.get()) {
                        stalledHandoffs.add(() -> handler.messageReceived(request, channel, task));
                        return;
                    }
                }
                handler.messageReceived(request, channel, task);
            });
        final var targetExecutor = internalCluster().getInstance(ThreadPool.class, targetNode).generic();

        // The shards are not preferred on the source node, so they relocate to the target node with a non-critical priority
        NotPreferredToRemainDecider.indexName = notPreferredIndexName;
        NotPreferredToRemainDecider.nodeId = getNodeId(sourceNode);
        ClusterRerouteUtils.reroute(client());
        awaitRecoveryCountStats(
            Map.of(targetNode, stats -> stats.currentAsTarget() == 1 && stats.currentAsTargetQueued() == queuedRelocations)
        );
        final var relocationsBeforeShutdown = getRelocationPriorities(notPreferredIndexName, targetNode, relocations);
        assertThat(relocationsBeforeShutdown.size(), equalTo(relocations));
        assertThat(relocationsBeforeShutdown.values(), everyItem(equalTo(RecoveryPriority.RELOCATION_CAN_REMAIN_NOT_PREFERRED)));

        // The target node raises the priority of the relocations that were queued when their source node was marked for removal.
        triggerSigtermShutdown(sourceNode);
        // The shutdown also forces the remaining shards off the source node. They queue behind the relocation that holds the slot.
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == 1 && stats.currentAsTargetQueued() == queuedRelocations + forcedRelocations
            )
        );

        final var priorities = getRelocationPriorities(notPreferredIndexName, targetNode, relocations);
        assertThat(priorities.keySet(), equalTo(relocationsBeforeShutdown.keySet()));
        // The recovery API reports the raised priority of the queued relocations. The one that started keeps its priority.
        assertThat(
            priorities.values().stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting())),
            equalTo(
                Map.of(
                    RecoveryPriority.RELOCATION_CAN_REMAIN_NOT_PREFERRED,
                    1L,
                    RecoveryPriority.RELOCATION_CAN_REMAIN_NO,
                    (long) queuedRelocations
                )
            )
        );
        // The forced relocations get the priority of a forced move from the master
        final var forcedPriorities = getRelocationPriorities(evacuatedIndexName, targetNode, forcedRelocations);
        assertThat(forcedPriorities.size(), equalTo(forcedRelocations));
        assertThat(forcedPriorities.values(), everyItem(equalTo(RecoveryPriority.RELOCATION_CAN_REMAIN_NO)));

        // Release the handoffs and let the relocations complete
        synchronized (stalledHandoffs) {
            stallHandoffs.set(false);
        }
        stalledHandoffs.forEach(handoff -> targetExecutor.execute(() -> {
            try {
                handoff.run();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }));
        ensureGreen(notPreferredIndexName, evacuatedIndexName);
        final var state = internalCluster().clusterService().state();
        assertThat(state.getRoutingNodes().node(getNodeId(sourceNode)).size(), equalTo(0));
        internalCluster().stopNode(sourceNode);
    }

    /// Registers a [org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type#SIGTERM] shutdown for the given
    /// node, which marks it for removal.
    private void triggerSigtermShutdown(String nodeName) {
        safeGet(
            clusterAdmin().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    getNodeId(nodeName),
                    SIGTERM,
                    "test",
                    null,
                    null,
                    TimeValue.timeValueMinutes(1)
                )
            )
        );
    }

    /// Returns the priority that the target node reports in the recovery API for each relocation of the index to it, which is the
    /// priority its queue uses. Waits until the target node has applied a cluster state with `expectedRelocations`
    /// relocations of the index to it.
    private Map<ShardId, RecoveryPriority> getRelocationPriorities(String indexName, String targetNode, int expectedRelocations) {
        final var targetNodeId = getNodeId(targetNode);
        // The target node enqueues a relocation while applying the cluster state that starts it, so the recovery stats can
        // report it before that state becomes visible through the target node's ClusterService#state. The recovery API
        // picks the shard copies to report on from the coordinating node's cluster state, so wait for the target node to
        // finish applying it and then coordinate on that node.
        awaitClusterState(
            targetNode,
            state -> state.getRoutingNodes()
                .node(targetNodeId)
                .shardsWithState(indexName, ShardRoutingState.INITIALIZING)
                .filter(ShardRouting::isRelocationTarget)
                .count() == expectedRelocations
        );
        return client(targetNode).admin()
            .indices()
            .prepareRecoveries(indexName)
            .setActiveOnly(true)
            .get()
            .shardRecoveryInfos()
            .getOrDefault(indexName, List.of())
            .stream()
            .map(ShardRecoveryInfo::recoveryState)
            .filter(recoveryState -> recoveryState.getTargetNode().getId().equals(targetNodeId))
            .collect(Collectors.toMap(RecoveryState::getShardId, RecoveryState::getRecoveryPriority));
    }

    private static int expectedDataNodeNonCriticalLimit(int maxConcurrentRecoveries, int nonCriticalPercent) {
        return (int) Math.ceil(maxConcurrentRecoveries * RatioValue.ofPercent(nonCriticalPercent).getAsRatio());
    }

    private static Settings incomingDataNodeThrottlingSettings(
        int maxConcurrentRecoveries,
        int nonCriticalPercent,
        boolean heapBasedLimit
    ) {
        final var settings = Settings.builder()
            .put(
                DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                maxConcurrentRecoveries
            )
            .put(
                DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING.getKey(),
                nonCriticalPercent + "%"
            );
        if (heapBasedLimit) {
            settings.put(
                DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_PER_HEAP_GB_SETTING.getKey(),
                MIN_PER_HEAP_GB
            );
        }
        return settings.build();
    }

    private void applyMasterThrottlingSettings(int nodeConcurrentRecoveries, String nodeName) {
        final var settings = Settings.builder()
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(),
                nodeConcurrentRecoveries
            )
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                nodeConcurrentRecoveries
            )
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(),
                nodeConcurrentRecoveries
            )
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), between(1, 4))
            .put(
                ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(),
                between(1, 4)
            );
        // `max(1, round(heapGb * 0))` allows a single concurrent primary relocation whatever the heap.
        settings.put(
            StatelessThrottlingConcurrentRecoveriesAllocationDecider.MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey(),
            "0b"
        ).put(StatelessThrottlingConcurrentRecoveriesAllocationDecider.CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB.getKey(), 0.0);
        updateClusterSettings(settings);

        final var clusterInfoService = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(
            ClusterInfoService.class
        );
        final var clusterInfo = ClusterInfoServiceUtils.refresh(clusterInfoService);
        assertThat(clusterInfo.getMaxHeapSizePerNode(), hasKey(getNodeId(nodeName)));
    }

    public static class NotPreferredToRemainPlugin extends Plugin implements ClusterPlugin {
        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return List.of(new NotPreferredToRemainDecider());
        }
    }

    /// Makes the shards on the node [#nodeId] not preferred to remain there, so that the balancer moves them away with
    /// [RecoveryPriority#RELOCATION_CAN_REMAIN_NOT_PREFERRED]. If [#indexName] is set, only the shards of that index are
    /// affected, otherwise the shards of all indices are.
    public static class NotPreferredToRemainDecider extends AllocationDecider {

        static volatile String nodeId;
        static volatile String indexName;

        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            final String index = indexName;
            return node.nodeId().equals(nodeId) && (index == null || index.equals(indexMetadata.getIndex().getName()))
                ? Decision.NOT_PREFERRED
                : Decision.YES;
        }
    }
}
