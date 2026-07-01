/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.CancelRecoveriesAction;
import org.elasticsearch.indices.recovery.RecoveryCancelledException;
import org.elasticsearch.indices.recovery.RecoveryMetricsCollector;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static org.hamcrest.Matchers.equalTo;

public class StatelessDirectRecoveryCancellationIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    /// Stateless reuses [org.elasticsearch.index.shard.IndexShard#activateWithPrimaryContext] for its own primary
    /// relocation handoff, the same method the ordinary peer-recovery handover path relies on to abort at the pre-handover
    /// boundary. This mirrors `DirectRecoveryCancellationIT#testDirectCancellationOfPrimaryRelocationAtHandoverBoundary`
    /// for the stateless relocation path.
    public void testDirectCancellationOfStatelessPrimaryRelocationAtHandoverBoundary() throws Exception {
        final var masterNode = startMasterOnlyNode();
        final var sourceNode = startIndexNode();

        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        final var targetNode = startIndexNode();

        // Stall the primary context handoff on the target.
        final var blockedHandoff = new CountDownLatch(1);
        final var proceedWithHandoff = new CountDownLatch(1);
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                blockedHandoff.countDown();
                safeAwait(proceedWithHandoff);
                handler.messageReceived(request, channel, task);
            });

        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, sourceNode, targetNode));
        safeAwait(blockedHandoff);
        disableAllocation();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, targetNode);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var shardFailureReceived = shardCancelledFailureReceivedLatch(masterNode, shardId);

        waitNoPendingTasksOnAll();
        final var clusterService = internalCluster().getInstance(ClusterService.class, targetNode);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(targetNode).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();

        // activateWithPrimaryContext should detect the flag and abort the handover
        proceedWithHandoff.countDown();

        safeAwait(shardFailureReceived);

        // Aborted handover leaves the primary on sourceNode
        ensureGreen(indexName);
        final var finalState = internalCluster().getInstance(ClusterService.class, sourceNode).state();
        final var primaryShard = finalState.routingTable().shardRoutingTable(shardId).primaryShard();
        assertTrue("primary shard is still started", primaryShard.started());
        assertThat(
            "primary shard is still located on source node",
            primaryShard.currentNodeId(),
            equalTo(finalState.nodes().resolveNode(sourceNode).getId())
        );

        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, targetNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        assertThat(
            plugin.getLongCounterMeasurement(RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC)
                .stream()
                .mapToLong(Measurement::getLong)
                .sum(),
            equalTo(1L)
        );
    }

    private void disableAllocation() {
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TimeValue.timeValueSeconds(10), TimeValue.timeValueSeconds(10))
                .setPersistentSettings(
                    Settings.builder()
                        .put(
                            EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(),
                            EnableAllocationDecider.Allocation.NONE
                        )
                )
        );
    }

    private static CountDownLatch shardCancelledFailureReceivedLatch(String node, ShardId shardId) {
        final var shardFailureReceivedLatch = new CountDownLatch(1);
        MockTransportService.getInstance(node)
            .addRequestHandlingBehavior(ShardStateAction.SHARD_FAILED_ACTION_NAME, (handler, request, channel, task) -> {
                if (request instanceof ShardStateAction.FailedShardEntry failedShard) {
                    if (failedShard.getShardId().equals(shardId)
                        && ExceptionsHelper.unwrap(failedShard.getFailure(), RecoveryCancelledException.class) != null) {
                        shardFailureReceivedLatch.countDown();
                    }
                }
                handler.messageReceived(request, channel, task);
            });
        return shardFailureReceivedLatch;
    }
}
