/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.CompositeRecoverySchedulingListener;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.TestRecoverySchedulingListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.allocation.StatelessThrottlingConcurrentRecoveriesAllocationDecider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.lessThan;

/// Integration tests for source-side relocation throttling in stateless Elasticsearch.
public class StatelessIndexThrottlingRecoveryIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    /// Unthrottle master-side allocation so concurrent primary relocations can reach the source queue.
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
                StatelessThrottlingConcurrentRecoveriesAllocationDecider.MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING
                    .getKey(),
                "0b"
            )
            .put(
                StatelessThrottlingConcurrentRecoveriesAllocationDecider.CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB.getKey(),
                Integer.MAX_VALUE
            )
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE);
    }

    public void testSourceNodeQueuesRelocationsPastConcurrencyLimit() {
        startMasterOnlyNode();
        final var sourceNode = startIndexNode(
            Settings.builder()
                .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1)
                .build()
        );
        final var targetNode = startIndexNode();

        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(2, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode).build());
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        // Stall first relocation at the primary-context handoff so the second relocation is queued.
        final var proceedWithHandoff = new CountDownLatch(1);
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(
                TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
                (handler, request, channel, task) -> {
                    safeAwait(proceedWithHandoff);
                    handler.messageReceived(request, channel, task);
                }
            );

        // Trigger relocation to the target.
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);
        awaitRecoveryCountStats(Map.of(sourceNode, stats -> stats.currentAsSource() == 1 && stats.currentAsSourceQueued() == 1));

        proceedWithHandoff.countDown();
        ensureGreen(indexName);
    }

    public void testQueuedRelocationCancelledWhenTargetNodeLeaves() throws Exception {
        startMasterOnlyNode();
        final var sourceNode = startIndexNode(
            Settings.builder()
                .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1)
                .build()
        );
        final var targetNode = startIndexNode();

        final var indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(2, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0)
                .build()
        );
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        // Stall the active relocation on the source before the handoff is sent, so the slot stays occupied.
        // Stall on the source (not the target) so stopping the target does not leave a blocked handler on a dying node.
        final var proceedWithHandoff = new CountDownLatch(1);
        MockTransportService.getInstance(sourceNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME)) {
                safeAwait(proceedWithHandoff);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // Trigger relocation to the target.
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);
        awaitRecoveryCountStats(Map.of(sourceNode, stats -> stats.currentAsSource() == 1 && stats.currentAsSourceQueued() == 1));

        internalCluster().stopNode(targetNode);
        ensureStableCluster(2);

        final var updatedStats = getRecoveryStats(sourceNode);
        assertThat("expected queued relocation to be cancelled after target node left", updatedStats.currentAsSourceQueued(), lessThan(1));

        proceedWithHandoff.countDown();
        startIndexNode();
        ensureGreen(indexName);
        assertTrue(getRecoveryStats(sourceNode).noCurrentRecoveries());
    }

    public void testAllQueuedRelocationsEventuallyComplete() {
        startMasterOnlyNode();
        final int limit = between(1, 3);
        final int totalShards = between(3, 6);

        final var sourceNode = startIndexNode(
            Settings.builder()
                .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), limit)
                .build()
        );
        final var targetNode = startIndexNode();

        final var indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(totalShards, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode).build()
        );
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        // Stall all handoffs so that exactly `limit` slots are occupied and the remaining shards are queued.
        final var proceedWithHandoffs = new CountDownLatch(1);
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(
                TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
                (handler, request, channel, task) -> {
                    safeAwait(proceedWithHandoffs);
                    handler.messageReceived(request, channel, task);
                }
            );

        // Trigger relocation to the target.
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);
        awaitRecoveryCountStats(
            Map.of(sourceNode, stats -> stats.currentAsSource() == limit && stats.currentAsSourceQueued() == totalShards - limit)
        );

        proceedWithHandoffs.countDown();
        ensureGreen(indexName);
    }

    public void testDynamicLimitIncreaseDispatchesPendingRelocationsUpToLimit() {
        startMasterOnlyNode();
        final int firstLimit = between(1, 3);
        final int secondLimit = firstLimit + between(1, 3);
        final int totalShards = secondLimit + between(1, 2);

        final var sourceNode = startIndexNode(
            Settings.builder()
                .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), firstLimit)
                .build()
        );
        final var targetNode = startIndexNode();

        final var indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(totalShards, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode).build()
        );
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        // Stall all handoffs to keep slots occupied so the queue actually drains when the limit is increased.
        final var proceedWithHandoffs = new CountDownLatch(1);
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(
                TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
                (handler, request, channel, task) -> {
                    safeAwait(proceedWithHandoffs);
                    handler.messageReceived(request, channel, task);
                }
            );

        // Trigger relocation to the target.
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);
        awaitRecoveryCountStats(
            Map.of(sourceNode, stats -> stats.currentAsSource() == firstLimit && stats.currentAsSourceQueued() == totalShards - firstLimit)
        );

        // Increasing the limit dispatches pending relocations up to the new limit.
        updateClusterSettings(
            Settings.builder()
                .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), secondLimit)
        );
        awaitRecoveryCountStats(
            Map.of(
                sourceNode,
                stats -> stats.currentAsSource() == secondLimit && stats.currentAsSourceQueued() == totalShards - secondLimit
            )
        );

        proceedWithHandoffs.countDown();
        ensureGreen(indexName);
    }

    /// Waits until the given per-node recovery-stats predicates are all satisfied. Re-checks on every recovery scheduling
    /// event on all given nodes.
    private void awaitRecoveryCountStats(Map<String, Predicate<RecoveryStats>> predicatePerNode) {
        final var conditionLatch = new CountDownLatch(1);
        final var success = new AtomicBoolean();

        final Map<String, IndicesService> indicesServices = new ConcurrentHashMap<>();
        final Map<String, CompositeRecoverySchedulingListener> schedulingListeners = new ConcurrentHashMap<>();
        for (final var nodeName : predicatePerNode.keySet()) {
            indicesServices.put(nodeName, internalCluster().getInstance(IndicesService.class, nodeName));
            schedulingListeners.put(nodeName, internalCluster().getInstance(CompositeRecoverySchedulingListener.class, nodeName));
        }

        final var listener = new TestRecoverySchedulingListener() {
            @Override
            public void onRecoverySchedulingChange() {
                if (success.get()) {
                    return;
                }
                for (final var nodePredicate : predicatePerNode.entrySet()) {
                    final var stats = new RecoveryStats();
                    for (IndexService indexService : indicesServices.get(nodePredicate.getKey())) {
                        for (IndexShard shard : indexService) {
                            stats.add(shard.recoveryStats());
                        }
                    }
                    if (nodePredicate.getValue().test(stats) == false) {
                        return;
                    }
                }
                success.set(true);
                conditionLatch.countDown();
            }
        };

        for (final var nodeName : predicatePerNode.keySet()) {
            schedulingListeners.get(nodeName).addListener(listener);
        }
        try {
            listener.onRecoverySchedulingChange(); // check in case conditions were already met
            safeAwait(conditionLatch);
        } finally {
            for (final var nodeName : predicatePerNode.keySet()) {
                schedulingListeners.get(nodeName).removeListener(listener);
            }
        }
    }

    private static RecoveryStats getRecoveryStats(String node) {
        return clusterAdmin().prepareNodesStats(node)
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
            .get()
            .getNodes()
            .getFirst()
            .getIndices()
            .getRecoveryStats();
    }
}
