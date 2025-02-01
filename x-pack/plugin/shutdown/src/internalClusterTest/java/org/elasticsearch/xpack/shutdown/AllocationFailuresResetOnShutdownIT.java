/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockIndexEventListener;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AllocationFailuresResetOnShutdownIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockIndexEventListener.TestPlugin.class, ShutdownPlugin.class);
    }

    public void testResetRelocationFailuresOnNodeShutdown() throws Exception {
        String node1 = internalCluster().startNode();
        createIndex("index1", 1, 0);
        ensureGreen("index1");
        final var failRelocation = new AtomicBoolean(true);
        String node2 = internalCluster().startNode();
        internalCluster().getInstances(MockIndexEventListener.TestEventListener.class)
            .forEach(testEventListener -> testEventListener.setNewDelegate(new IndexEventListener() {
                @Override
                public void beforeIndexShardCreated(ShardRouting routing, Settings indexSettings) {
                    if (failRelocation.get()) {
                        throw new RuntimeException("FAIL");
                    }
                }
            }));
        updateIndexSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", node1), "index1");
        ensureGreen("index1");
        // await all relocation attempts are exhausted
        var maxAttempts = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        assertBusy(() -> {
            var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
            var shard = state.routingTable().index("index1").shard(0).primaryShard();
            assertThat(shard, notNullValue());
            assertThat(shard.relocationFailureInfo().failedRelocations(), equalTo(maxAttempts));
        });
        // ensure the shard remain started
        var stateAfterFailures = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
        var shardAfterFailures = stateAfterFailures.routingTable().index("index1").shard(0).primaryShard();
        assertThat(shardAfterFailures, notNullValue());
        assertThat(shardAfterFailures.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(stateAfterFailures.nodes().get(shardAfterFailures.currentNodeId()).getName(), equalTo(node1));
        failRelocation.set(false);
        if (randomBoolean()) {
            // A RESTART marker shouldn't cause a reset of failures
            final var request = createRequest(SingleNodeShutdownMetadata.Type.RESTART, node1, null);
            safeGet(client().execute(PutShutdownNodeAction.INSTANCE, request));
            var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
            var shard = state.routingTable().index("index1").shard(0).primaryShard();
            assertThat(shard, notNullValue());
            assertThat(shard.relocationFailureInfo().failedRelocations(), equalTo(maxAttempts));
            assertThat(state.nodes().get(shard.currentNodeId()).getName(), equalTo(node1));
        }
        // A non-RESTART node shutdown should reset the counter and allow more relocation retries
        final var request = createRequest(
            randomFrom(
                SingleNodeShutdownMetadata.Type.REPLACE,
                SingleNodeShutdownMetadata.Type.SIGTERM,
                SingleNodeShutdownMetadata.Type.REMOVE
            ),
            node1,
            node2
        );
        safeGet(client().execute(PutShutdownNodeAction.INSTANCE, request));
        assertBusy(() -> {
            var stateAfterNodeJoin = internalCluster().clusterService().state();
            var relocatedShard = stateAfterNodeJoin.routingTable().index("index1").shard(0).primaryShard();
            assertThat(relocatedShard.relocationFailureInfo().failedRelocations(), Matchers.lessThan(maxAttempts));
            assertThat(relocatedShard, notNullValue());
            assertThat(stateAfterNodeJoin.nodes().get(relocatedShard.currentNodeId()).getName(), not(equalTo(node1)));
        });
    }

    public void testResetRelocationFailuresOnNodeShutdownRemovalOfExistingNode() throws Exception {
        String node1 = internalCluster().startNode();
        createIndex("index1", 1, 0);
        ensureGreen("index1");
        final var failRelocation = new AtomicBoolean(true);
        String node2 = internalCluster().startNode();
        internalCluster().startNode();
        internalCluster().getInstances(MockIndexEventListener.TestEventListener.class)
            .forEach(testEventListener -> testEventListener.setNewDelegate(new IndexEventListener() {
                @Override
                public void beforeIndexShardCreated(ShardRouting routing, Settings indexSettings) {
                    if (failRelocation.get()) {
                        throw new RuntimeException("FAIL");
                    }
                }
            }));
        // add shutdown to the new node
        final var request = createRequest(randomFrom(SingleNodeShutdownMetadata.Type.values()), node2, randomIdentifier());
        safeGet(client().execute(PutShutdownNodeAction.INSTANCE, request));

        updateIndexSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", node1), "index1");
        ensureGreen("index1");
        // await all relocation attempts are exhausted
        var maxAttempts = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        assertBusy(() -> {
            var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
            var shard = state.routingTable().index("index1").shard(0).primaryShard();
            assertThat(shard, notNullValue());
            assertThat(shard.relocationFailureInfo().failedRelocations(), equalTo(maxAttempts));
        });
        // ensure the shard remain started
        var stateAfterFailures = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
        var shardAfterFailures = stateAfterFailures.routingTable().index("index1").shard(0).primaryShard();
        assertThat(shardAfterFailures, notNullValue());
        assertThat(shardAfterFailures.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(stateAfterFailures.nodes().get(shardAfterFailures.currentNodeId()).getName(), equalTo(node1));
        failRelocation.set(false);
        // Removing the non-RESTART shutdown marker should reset the counter and allow more relocation retries
        safeGet(
            client().execute(
                DeleteShutdownNodeAction.INSTANCE,
                new DeleteShutdownNodeAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, getNodeId(node2))
            )
        );
        if (request.getType() != SingleNodeShutdownMetadata.Type.RESTART) {
            assertBusy(() -> {
                var stateAfterNodeJoin = internalCluster().clusterService().state();
                var relocatedShard = stateAfterNodeJoin.routingTable().index("index1").shard(0).primaryShard();
                assertThat(relocatedShard.relocationFailureInfo().failedRelocations(), Matchers.lessThan(maxAttempts));
                assertThat(relocatedShard, notNullValue());
                assertThat(stateAfterNodeJoin.nodes().get(relocatedShard.currentNodeId()).getName(), not(equalTo(node1)));
            });
        } else {
            var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
            var shard = state.routingTable().index("index1").shard(0).primaryShard();
            assertThat(shard, notNullValue());
            assertThat(shard.relocationFailureInfo().failedRelocations(), equalTo(maxAttempts));
            assertThat(state.nodes().get(shard.currentNodeId()).getName(), equalTo(node1));
        }
    }

    public void testResetAllocationFailuresOnNodeShutdown() throws Exception {
        String node1 = internalCluster().startNode();
        String node2 = internalCluster().startNode();
        internalCluster().startNode();

        final var failAllocation = new AtomicBoolean(true);
        internalCluster().getInstances(MockIndexEventListener.TestEventListener.class)
            .forEach(testEventListener -> testEventListener.setNewDelegate(new IndexEventListener() {
                @Override
                public void beforeIndexShardCreated(ShardRouting routing, Settings indexSettings) {
                    if (failAllocation.get()) {
                        throw new RuntimeException("FAIL");
                    }
                }
            }));

        prepareCreate("index1", indexSettings(1, 0)).execute();

        // await all allocation attempts are exhausted
        var maxAttempts = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        ensureRed("index1");
        assertBusy(() -> {
            var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
            var index = state.getRoutingTable().index("index1");
            assertNotNull(index);
            var shard = index.shard(0).primaryShard();
            assertNotNull(shard);
            assertNotNull(shard.unassignedInfo());
            assertThat(maxAttempts, equalTo(shard.unassignedInfo().failedAllocations()));
        });

        failAllocation.set(false);

        if (randomBoolean()) {
            // A RESTART marker shouldn't cause a reset of failures
            final var request = createRequest(randomFrom(SingleNodeShutdownMetadata.Type.RESTART), node1, null);
            safeGet(client().execute(PutShutdownNodeAction.INSTANCE, request));
            ensureRed("index1");
            var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
            var shard = state.routingTable().index("index1").shard(0).primaryShard();
            assertThat(shard, notNullValue());
            assertNotNull(shard.unassignedInfo());
            assertThat(shard.unassignedInfo().failedAllocations(), equalTo(maxAttempts));
        }

        // A non-RESTART node shutdown should reset the counter and allow more relocation retries
        final var request = createRequest(
            randomFrom(
                SingleNodeShutdownMetadata.Type.REPLACE,
                SingleNodeShutdownMetadata.Type.SIGTERM,
                SingleNodeShutdownMetadata.Type.REMOVE
            ),
            node1,
            node2
        );
        safeGet(client().execute(PutShutdownNodeAction.INSTANCE, request));
        ensureGreen("index1");
    }

    public void testResetAllocationFailuresOnNodeShutdownRemovalOfExistingNode() throws Exception {
        String node1 = internalCluster().startNode();
        String node2 = internalCluster().startNode();
        internalCluster().startNode();

        final var failAllocation = new AtomicBoolean(true);
        internalCluster().getInstances(MockIndexEventListener.TestEventListener.class)
            .forEach(testEventListener -> testEventListener.setNewDelegate(new IndexEventListener() {
                @Override
                public void beforeIndexShardCreated(ShardRouting routing, Settings indexSettings) {
                    if (failAllocation.get()) {
                        throw new RuntimeException("FAIL");
                    }
                }
            }));

        final var request = createRequest(randomFrom(SingleNodeShutdownMetadata.Type.values()), node1, node2);
        safeGet(client().execute(PutShutdownNodeAction.INSTANCE, request));

        prepareCreate("index1", indexSettings(1, 0)).execute();

        // await all allocation attempts are exhausted
        var maxAttempts = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        ensureRed("index1");
        assertBusy(() -> {
            var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
            var shard = state.getRoutingTable().index("index1").shard(0).primaryShard();
            assertNotNull(shard);
            assertNotNull(shard.unassignedInfo());
            assertThat(maxAttempts, equalTo(shard.unassignedInfo().failedAllocations()));
        });

        failAllocation.set(false);

        // A none-RESTART node shutdown should reset the counter and allow more allocation retries
        safeGet(
            client().execute(
                DeleteShutdownNodeAction.INSTANCE,
                new DeleteShutdownNodeAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, getNodeId(node1))
            )
        );

        if (request.getType() != SingleNodeShutdownMetadata.Type.RESTART) {
            ensureGreen("index1");
        } else {
            ensureRed("index1");
            var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute()).getState();
            var shard = state.routingTable().index("index1").shard(0).primaryShard();
            assertThat(shard, notNullValue());
            assertNotNull(shard.unassignedInfo());
            assertThat(shard.unassignedInfo().failedAllocations(), equalTo(maxAttempts));
        }
    }

    private PutShutdownNodeAction.Request createRequest(SingleNodeShutdownMetadata.Type type, String nodeName, String targetNodeName) {
        var nodeId = getNodeId(nodeName);
        switch (type) {
            case REMOVE -> {
                return new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    nodeId,
                    SingleNodeShutdownMetadata.Type.REMOVE,
                    "test",
                    null,
                    null,
                    null
                );
            }
            case REPLACE -> {
                return new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    nodeId,
                    SingleNodeShutdownMetadata.Type.REPLACE,
                    "test",
                    null,
                    targetNodeName,
                    null
                );
            }
            case RESTART -> {
                return new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    nodeId,
                    SingleNodeShutdownMetadata.Type.RESTART,
                    "test",
                    null,
                    null,
                    null
                );
            }
            case SIGTERM -> {
                return new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    nodeId,
                    SingleNodeShutdownMetadata.Type.SIGTERM,
                    "test",
                    null,
                    null,
                    randomTimeValue()
                );
            }
            default -> throw new AssertionError("unknown shutdown metadata type: " + type);
        }
    }
}
