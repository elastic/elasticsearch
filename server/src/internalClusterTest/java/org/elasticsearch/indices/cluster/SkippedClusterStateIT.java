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
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
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
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/// Integration tests covering bugs that previously surfaced on a data node when it misses one or
/// more intermediate cluster states for a primary shard transition.
///
/// Each test in this class drops a different window of COMMIT messages on the data node to
/// reproduce a different bug from this family.
@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class SkippedClusterStateIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test";
    private static final int SHARD_ID = 0;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    /// Regression test for the `failedShardsCache` eviction bug in [IndicesClusterStateService].
    ///
    /// Verifies that a shard is re-created with only one shardFailed round after the master
    /// re-allocates it with a bumped primary term but the same allocation id.
    ///
    /// The test creates a primary-only index, fails the started primary via [IndexShard#failShard],
    /// and simulates the data node missing the intermediate UNASSIGNED cluster state (transient
    /// network partition). If `updateFailedShardsCache` fails to evict the stale cache entry, it
    /// will resend `localShardFailed` to the master, causing an unnecessary round of allocation.
    public void testFailedShardCacheEvictedAfterPrimaryTermBump() throws Exception {
        final var masterNodeName = internalCluster().startMasterOnlyNode();
        final var primaryNodeName = internalCluster().startDataOnlyNode();

        final var index = createTestIndex();
        final var shardId = new ShardId(index, SHARD_ID);
        final var masterClusterService = internalCluster().getInstance(ClusterService.class, masterNodeName);
        final var startedRouting = masterClusterService.state().routingTable().shardRoutingTable(shardId).primaryShard();
        final var initialTerm = masterClusterService.state().metadata().getProject().index(INDEX_NAME).primaryTerm(SHARD_ID);

        // Intercept COMMIT_STATE on the data node. Drops every COMMIT whose last-accepted state has
        // the shard UNASSIGNED, until the first INITIALIZING is delivered, to simulate a transient
        // partition that causes the data node to miss the UNASSIGNED window and see the
        // re-allocation directly. `failedShardsCache` still holds the stale entry when INITIALIZING
        // arrives with the same allocation id.
        final var seenInitializing = new AtomicBoolean(false);
        final var dataTransport = setupCommitStateHandler(primaryNodeName, primary -> {
            if (primary.initializing()) {
                seenInitializing.set(true);
            }
            return seenInitializing.get() == false && primary.unassigned();
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

        try {
            final var primaryShard = internalCluster().getInstance(IndicesService.class, primaryNodeName).getShardOrNull(shardId);
            assertNotNull(primaryShard);
            primaryShard.failShard("test", new IOException("simulated engine failure"));

            // Wait for the first localShardFailed to reach the master.
            safeAwait(masterReceivedShardFailedRequest);

            // Wait for ICSS to finish all processing.
            final var icss = internalCluster().getInstance(IndicesClusterStateService.class, primaryNodeName);
            assertBusy(() -> assertTrue(icss.failedShardsCache.isEmpty()));
            ensureGreen(TimeValue.timeValueSeconds(30), INDEX_NAME);

            final var finalState = masterClusterService.state();
            final var finalRouting = finalState.routingTable().shardRoutingTable(shardId).primaryShard();
            final var finalTerm = finalState.metadata().getProject().index(INDEX_NAME).primaryTerm(SHARD_ID);
            assertThat(finalRouting.allocationId().getId(), equalTo(startedRouting.allocationId().getId()));
            assertThat(
                "expected exactly one localShardFailed request round, got " + shardFailedRequestCount.get(),
                shardFailedRequestCount.get(),
                equalTo(1)
            );
            // At most two term bumps. One from SHARD_FAILED processing, optionally one from
            // GatewayAllocator detecting the failed shard store internally.
            assertThat("unexpected term bumps", finalTerm, lessThanOrEqualTo(initialTerm + 2));
        } finally {
            dataTransport.clearAllRules();
            masterTransport.clearAllRules();
        }
    }

    /// Regression test for the "term is only increased as part of primary promotion" assertion
    /// failure at [org.elasticsearch.index.shard.IndexShard#updateShardState] when a node batches
    /// intermediate `STARTED` and `UNASSIGNED` states for one of its primary shards.
    public void testInitializingPrimaryTermBump() throws Exception {
        final var masterNodeName = internalCluster().startMasterOnlyNode();
        final var primaryNodeName = internalCluster().startDataOnlyNode();

        // After exactly 1 INITIALIZING seen, drop every STARTED/UNASSIGNED commit so the data node stays at INITIALIZING locally.
        final var initializingCount = new AtomicInteger(0);
        final var dataTransport = setupCommitStateHandler(primaryNodeName, primary -> {
            if (primary.initializing()) {
                initializingCount.incrementAndGet();
                return false;
            }
            return initializingCount.get() == 1 && (primary.started() || primary.unassigned());
        });

        try {
            final var index = createTestIndex();
            final var shardId = new ShardId(index, SHARD_ID);
            final var masterClusterService = internalCluster().getInstance(ClusterService.class, masterNodeName);

            // Cluster state is on `STARTED` but data node is still `INITIALIZING`
            ClusterServiceUtils.awaitClusterState(state -> {
                final var primary = state.routingTable().shardRoutingTable(shardId).primaryShard();
                return primary != null && primary.started();
            }, masterClusterService);

            final var startedRoutingOnMaster = masterClusterService.state().routingTable().shardRoutingTable(shardId).primaryShard();
            final var initialTerm = masterClusterService.state().metadata().getProject().index(INDEX_NAME).primaryTerm(SHARD_ID);

            final var indicesService = internalCluster().getInstance(IndicesService.class, primaryNodeName);
            final var primaryShard = indicesService.getShardOrNull(shardId);
            assertNotNull("primary index shard should exist on the data node", primaryShard);
            final var localRouting = primaryShard.routingEntry();
            assertTrue("data node should still see its local primary as INITIALIZING, was " + localRouting, localRouting.initializing());
            assertThat(
                "local allocation id should match the master's STARTED allocation id",
                localRouting.allocationId().getId(),
                equalTo(startedRoutingOnMaster.allocationId().getId())
            );

            // Before fixing, "term is only increased as part of primary promotion" would show up here and reroute would fail
            ClusterRerouteUtils.reroute(client(masterNodeName), new CancelAllocationCommand(INDEX_NAME, SHARD_ID, primaryNodeName, true));
            ensureGreen(TimeValue.timeValueSeconds(30), INDEX_NAME);

            final var finalState = masterClusterService.state();
            final var finalRouting = finalState.routingTable().shardRoutingTable(shardId).primaryShard();
            final var finalTerm = finalState.metadata().getProject().index(INDEX_NAME).primaryTerm(SHARD_ID);
            assertEquals(finalRouting.allocationId().getId(), startedRoutingOnMaster.allocationId().getId());
            assertThat("expected the primary term to have been bumped", finalTerm, greaterThan(initialTerm));
            assertThat(initializingCount.get(), greaterThanOrEqualTo(2));
        } finally {
            dataTransport.clearAllRules();
        }
    }

    /// Installs a `COMMIT_STATE` interceptor on the given data node. For each commit, if
    /// `dropDecider` returns true the commit is silently dropped (we respond with an empty success
    /// response so the master's lag detector does not eject the data node). Otherwise, the commit
    /// is passed to the regular handler.
    ///
    /// Returns the [MockTransportService] so the caller can `clearAllRules()` in a `finally` block.
    private MockTransportService setupCommitStateHandler(String dataNodeName, Predicate<ShardRouting> dropDecider) {
        final var coordinator = internalCluster().getInstance(Coordinator.class, dataNodeName);
        final var dataTransport = MockTransportService.getInstance(dataNodeName);

        dataTransport.addRequestHandlingBehavior(Coordinator.COMMIT_STATE_ACTION_NAME, (handler, request, channel, task) -> {
            final var indexRouting = coordinator.getLastAcceptedState().routingTable().index(INDEX_NAME);
            final var primary = indexRouting == null ? null : indexRouting.shard(SHARD_ID).primaryShard();
            if (primary != null && dropDecider.test(primary)) {
                logger.info("---> COMMIT intercepted and dropped on {}: primary={}", dataNodeName, primary);
                // We respond with an empty success response rather than an error or no response, which is somewhat artificial.
                // This is meant to avoid test instability. A network error would eject the data node from the cluster, and
                // lowering the timeout could cause master failures. In the "real" production path for this bug, the node would
                // not respond and the master would move on after timeout.
                channel.sendResponse(ActionResponse.Empty.INSTANCE);
            } else {
                handler.messageReceived(request, channel, task);
            }
        });
        return dataTransport;
    }

    private Index createTestIndex() {
        createIndex(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(INDEX_NAME);
        return resolveIndex(INDEX_NAME);
    }
}
