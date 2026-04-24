/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/// Regression test for the `failedShardsCache` eviction bug in [IndicesClusterStateService].
///
/// When a primary shard fails and there are no replicas, the master bumps the primary term
/// (`IndexMetadataUpdater.shardFailed`) but can re-allocate to the same node reusing the existing
/// allocation id (it is the only entry in `inSyncAllocationIds`, so `GatewayAllocator` picks it up).
/// The cache eviction in `updateFailedShardsCache` only compares allocation ids via
/// [ShardRouting#isSameAllocation], ignoring the primary term bump, so the stale entry is retained
/// and the shard is never re-created on the first attempt.
@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class FailedShardCacheIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    /// Verifies that a shard is re-created with only one shardFailed round after the master re-allocates it
    /// with a bumped primary term but the same allocation id.
    ///
    /// The test creates a primary-only index, fails the started primary via [IndexShard#failShard],
    /// and simulates the data node missing the intermediate UNASSIGNED cluster state (transient network partition).
    /// If `updateFailedShardsCache` fails to evict the stale cache entry, it will resend `localShardFailed`
    /// to the master, causing an unnecessary round of allocation.
    public void testShardRecreatedAfterPrimaryTermBump() throws Exception {
        final var masterNodeName = internalCluster().startMasterOnlyNode();
        final var primaryNodeName = internalCluster().startDataOnlyNode();

        final var indexName = "test";
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(indexName);

        final var masterClusterState = internalCluster().getInstance(ClusterService.class, masterNodeName).state();
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var startedRouting = masterClusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final var initialTerm = masterClusterState.metadata().getProject().index(indexName).primaryTerm(0);

        // Intercept COMMIT_STATE on the data node. Drops every COMMIT whose last-accepted state
        // has the shard UNASSIGNED, until the first INITIALIZING is delivered, to simulate a transient partition
        // that causes the data node to miss the UNASSIGNED window and see the re-allocation directly.
        // `failedShardsCache` still holds the stale entry when INITIALIZING arrives with the same
        // allocation id.
        final var coordinator = internalCluster().getInstance(Coordinator.class, primaryNodeName);
        final var dataTransport = MockTransportService.getInstance(primaryNodeName);
        final var seenInitializing = new AtomicBoolean(false);

        dataTransport.addRequestHandlingBehavior(Coordinator.COMMIT_STATE_ACTION_NAME, (handler, request, channel, task) -> {
            final var indexRouting = coordinator.getLastAcceptedState().routingTable().index(indexName);
            final var primary = indexRouting == null ? null : indexRouting.shard(0).primaryShard();
            if (primary != null && primary.initializing()) {
                seenInitializing.set(true);
            }
            final boolean shouldDrop = seenInitializing.get() == false && primary != null && primary.unassigned();
            if (shouldDrop) {
                logger.info("---> COMMIT intercepted and dropped: primary={}", primary);
                // We respond with an empty success response rather than an error or no response, which is somewhat
                // artificial. This is meant to avoid test instability. A network error would eject the data
                // node from the cluster, and lowering the timeout could cause master failures.
                // In the "real" production path for this bug, the node would not respond and the master would move on
                // after timeout.
                channel.sendResponse(ActionResponse.Empty.INSTANCE);
            } else {
                handler.messageReceived(request, channel, task);
            }
        });

        // Count how many SHARD_FAILED requests the master receives; latch fires on the first.
        final var shardFailedRequestCount = new AtomicInteger(0);
        final var masterReceivedShardFailedRequest = new CountDownLatch(1);
        final var masterTransport = MockTransportService.getInstance(masterNodeName);
        masterTransport.addRequestHandlingBehavior(ShardStateAction.SHARD_FAILED_ACTION_NAME, (handler, request, channel, task) -> {
            shardFailedRequestCount.incrementAndGet();
            masterReceivedShardFailedRequest.countDown();
            handler.messageReceived(request, channel, task);
        });

        final var primaryShard = internalCluster().getInstance(IndicesService.class, primaryNodeName).getShardOrNull(shardId);
        assertNotNull(primaryShard);
        try {
            primaryShard.failShard("test", new IOException("simulated engine failure"));

            // Wait for the first localShardFailed to reach the master.
            safeAwait(masterReceivedShardFailedRequest);

            // Wait for ICSS to finish all processing.
            final var icss = internalCluster().getInstance(IndicesClusterStateService.class, primaryNodeName);
            assertBusy(() -> assertTrue(icss.failedShardsCache.isEmpty()));
            ensureGreen(TimeValue.timeValueSeconds(30), indexName);

            final var finalState = internalCluster().getInstance(ClusterService.class, masterNodeName).state();
            final var finalRouting = finalState.routingTable().shardRoutingTable(shardId).primaryShard();
            final var finalTerm = finalState.metadata().getProject().index(indexName).primaryTerm(0);
            assertThat(finalRouting.allocationId().getId(), equalTo(startedRouting.allocationId().getId()));
            assertThat(
                "expected exactly one localShardFailed request round, got " + shardFailedRequestCount.get(),
                shardFailedRequestCount.get(),
                equalTo(1)
            );
            // At most two term bumps. One from SHARD_FAILED processing, optionally one from GatewayAllocator
            // detecting the failed shard store internally.
            assertThat("unexpected term bumps", finalTerm, lessThanOrEqualTo(initialTerm + 2));
        } finally {
            dataTransport.clearAllRules();
            masterTransport.clearAllRules();
        }
    }
}
