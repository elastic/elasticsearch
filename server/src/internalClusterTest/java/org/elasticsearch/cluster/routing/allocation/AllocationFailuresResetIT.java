/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.MockIndexEventListener;

import java.util.List;

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
            var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
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
            var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            var index = state.getRoutingTable().index(INDEX);
            assertNotNull(index);
            var shard = index.shard(SHARD).primaryShard();
            assertNotNull(shard);
            assertTrue(shard.assignedToNode());
            assertTrue(shard.started());
        });
    }

    public void testResetFailuresOnNodeJoin() throws Exception {
        var node1 = internalCluster().startNode();
        injectAllocationFailures(node1);
        prepareCreate(INDEX, indexSettings(1, 0)).execute();
        awaitShardAllocMaxRetries();
        removeAllocationFailuresInjection(node1);
        internalCluster().startNode();
        awaitShardAllocSucceed();
    }

}
