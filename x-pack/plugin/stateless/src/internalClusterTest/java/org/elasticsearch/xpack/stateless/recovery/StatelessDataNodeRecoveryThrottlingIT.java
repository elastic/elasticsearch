/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.MockIndexEventListener;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.allocation.StatelessThrottlingConcurrentRecoveriesAllocationDecider;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationHandoffAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static org.hamcrest.Matchers.equalTo;
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
        return plugins;
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
        final int relocationPercent = between(1, 100);
        final boolean heapBasedLimit = randomBoolean();
        final int expectedMaxConcurrentRecoveries = heapBasedLimit ? 1 : maxConcurrentRecoveries;
        final int expectedRelocationLimit = expectedDataNodeRelocationLimit(expectedMaxConcurrentRecoveries, relocationPercent);
        final int totalShards = expectedRelocationLimit + between(1, 3);

        final var sourceNode = startIndexNode();
        final var targetNode = startIndexNode(
            incomingDataNodeThrottlingSettings(maxConcurrentRecoveries, relocationPercent, heapBasedLimit)
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
                stats -> stats.currentAsTarget() == expectedRelocationLimit
                    && stats.currentAsTargetQueued() == totalShards - expectedRelocationLimit
            )
        );

        // New primaries are not relocations, so they can use the slots that the relocations leave free.
        final int freeSlots = expectedMaxConcurrentRecoveries - expectedRelocationLimit;
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
                stats -> stats.currentAsTarget() == expectedRelocationLimit
                    && stats.currentAsTargetQueued() == totalShards - expectedRelocationLimit
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
        // Expected relocation limit is at least 2 (so we can ensure the heap-based limit, which is 1, does not apply)
        final int relocationPercent = between(100 / maxConcurrentRecoveries + 1, 100);
        final int expectedRelocationLimit = expectedDataNodeRelocationLimit(maxConcurrentRecoveries, relocationPercent);
        assertThat(expectedRelocationLimit, greaterThan(1));
        final int totalShards = expectedRelocationLimit + between(1, 3);

        final var indexNode = startIndexNode();
        final var sourceNode = startSearchNode();
        final var targetNode = startSearchNode(incomingDataNodeThrottlingSettings(maxConcurrentRecoveries, relocationPercent, true));

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
                stats -> stats.currentAsTarget() == expectedRelocationLimit
                    && stats.currentAsTargetQueued() == totalShards - expectedRelocationLimit
            )
        );

        // New search shards are unassigned recoveries, not relocations, so they can use the slots that the relocations leave free.
        final int freeSlots = maxConcurrentRecoveries - expectedRelocationLimit;
        final int newShards = freeSlots + between(1, 2);
        final var newIndexName = randomIndexName();
        prepareCreate(newIndexName).setSettings(
            indexSettings(newShards, 1).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode)
        ).setWaitForActiveShards(ActiveShardCount.NONE).get();
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == maxConcurrentRecoveries
                    && stats.currentAsTargetQueued() == (totalShards - expectedRelocationLimit) + (newShards - freeSlots)
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

    private static int expectedDataNodeRelocationLimit(int maxConcurrentRecoveries, int relocationPercent) {
        return (int) Math.ceil(maxConcurrentRecoveries * RatioValue.ofPercent(relocationPercent).getAsRatio());
    }

    private static Settings incomingDataNodeThrottlingSettings(int maxConcurrentRecoveries, int relocationPercent, boolean heapBasedLimit) {
        final var settings = Settings.builder()
            .put(
                DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                maxConcurrentRecoveries
            )
            .put(
                DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_INCOMING_RECOVERIES_MAX_RELOCATION_PROPORTION_SETTING.getKey(),
                relocationPercent + "%"
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
}
