/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.MockIndexEventListener;
import org.elasticsearch.test.MockLog;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class AllocationFailuresResetIT extends ESIntegTestCase {

    private static final String INDEX = "index-1";
    private static final int SHARD = 0;

    @Override
    protected List<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockIndexEventListener.TestPlugin.class);
    }

    private void injectAllocationFailures(String node) {
        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, node).setNewDelegate(new IndexEventListener() {
            @Override
            public void beforeIndexShardCreated(ShardRouting routing, Settings indexSettings) {
                throw new RuntimeException("shard allocation failure");
            }
        });
    }

    private void removeAllocationFailuresInjection(String node) {
        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, node).setNewDelegate(new IndexEventListener() {
        });
    }

    private void awaitShardAllocMaxRetries() throws Exception {
        var maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(internalCluster().getDefaultSettings());
        assertBusy(() -> {
            var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
            var index = state.getRoutingTable().index(INDEX);
            assertNotNull(index);
            var shard = index.shard(SHARD).primaryShard();
            assertNotNull(shard);
            var unassigned = shard.unassignedInfo();
            assertNotNull(unassigned);
            assertEquals(maxRetries.intValue(), unassigned.failedAllocations());
        });
    }

    private void awaitShardAllocSucceed() throws Exception {
        assertBusy(() -> {
            var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
            var index = state.getRoutingTable().index(INDEX);
            assertNotNull(index);
            var shard = index.shard(SHARD).primaryShard();
            assertNotNull(shard);
            assertTrue(shard.assignedToNode());
            assertTrue(shard.started());
        });
    }

    public void testResetAllocationFailuresOnNodeJoin() throws Exception {
        var node1 = internalCluster().startNode();
        injectAllocationFailures(node1);
        prepareCreate(INDEX, indexSettings(1, 0)).execute();
        awaitShardAllocMaxRetries();
        removeAllocationFailuresInjection(node1);
        try (var mockLog = MockLog.capture(RoutingNodes.class)) {
            var shardId = internalCluster().clusterService().state().routingTable().index(INDEX).shard(SHARD).shardId();
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "log resetting failed allocations",
                    RoutingNodes.class.getName(),
                    Level.INFO,
                    Strings.format(RoutingNodes.RESET_FAILED_ALLOCATION_COUNTER_LOG_MSG, 1, List.of(shardId))
                )
            );
            internalCluster().startNode();
            awaitShardAllocSucceed();
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testResetRelocationFailuresOnNodeJoin() throws Exception {
        String node1 = internalCluster().startNode();
        createIndex(INDEX, 1, 0);
        ensureGreen(INDEX);
        final var failRelocation = new AtomicBoolean(true);
        String node2 = internalCluster().startNode();
        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, node2).setNewDelegate(new IndexEventListener() {
            @Override
            public void beforeIndexCreated(Index index, Settings indexSettings) {
                if (failRelocation.get()) {
                    throw new RuntimeException("FAIL");
                }
            }
        });
        updateIndexSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", node1), INDEX);
        ensureGreen(INDEX);
        // await all relocation attempts are exhausted
        var maxAttempts = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        assertBusy(() -> {
            var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
            var shard = state.routingTable().index(INDEX).shard(SHARD).primaryShard();
            assertThat(shard, notNullValue());
            assertThat(shard.relocationFailureInfo().failedRelocations(), equalTo(maxAttempts));
        });
        // ensure the shard remain started
        var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
        var shard = state.routingTable().index(INDEX).shard(SHARD).primaryShard();
        assertThat(shard, notNullValue());
        assertThat(shard.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(state.nodes().get(shard.currentNodeId()).getName(), equalTo(node1));
        failRelocation.set(false);
        // A new node joining should reset the counter and allow more relocation retries
        try (var mockLog = MockLog.capture(RoutingNodes.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "log resetting failed relocations",
                    RoutingNodes.class.getName(),
                    Level.INFO,
                    Strings.format(RoutingNodes.RESET_FAILED_RELOCATION_COUNTER_LOG_MSG, 1, List.of(shard.shardId()))
                )
            );
            internalCluster().startNode();
            assertBusy(() -> {
                var stateAfterNodeJoin = internalCluster().clusterService().state();
                var relocatedShard = stateAfterNodeJoin.routingTable().index(INDEX).shard(SHARD).primaryShard();
                assertThat(relocatedShard, notNullValue());
                assertThat(stateAfterNodeJoin.nodes().get(relocatedShard.currentNodeId()).getName(), not(equalTo(node1)));
            });
            mockLog.assertAllExpectationsMatched();
        }
    }
}
