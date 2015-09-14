/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.indices;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.index.shard.IndexShardState.CLOSED;
import static org.elasticsearch.index.shard.IndexShardState.CREATED;
import static org.elasticsearch.index.shard.IndexShardState.POST_RECOVERY;
import static org.elasticsearch.index.shard.IndexShardState.RECOVERING;
import static org.elasticsearch.index.shard.IndexShardState.STARTED;
import static org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndicesLifecycleListenerIT extends ESIntegTestCase {

    @Test
    public void testBeforeIndexAddedToCluster() throws Exception {
        String node1 = internalCluster().startNode();
        String node2 = internalCluster().startNode();
        String node3 = internalCluster().startNode();

        final AtomicInteger beforeAddedCount = new AtomicInteger(0);
        final AtomicInteger allCreatedCount = new AtomicInteger(0);

        IndicesLifecycle.Listener listener = new IndicesLifecycle.Listener() {
            @Override
            public void beforeIndexAddedToCluster(Index index, @IndexSettings Settings indexSettings) {
                beforeAddedCount.incrementAndGet();
                if (indexSettings.getAsBoolean("index.fail", false)) {
                    throw new ElasticsearchException("failing on purpose");
                }
            }

            @Override
            public void beforeIndexCreated(Index index, @IndexSettings Settings indexSettings) {
                allCreatedCount.incrementAndGet();
            }
        };

        internalCluster().getInstance(IndicesLifecycle.class, node1).addListener(listener);
        internalCluster().getInstance(IndicesLifecycle.class, node2).addListener(listener);
        internalCluster().getInstance(IndicesLifecycle.class, node3).addListener(listener);

        client().admin().indices().prepareCreate("test")
                .setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).get();
        ensureGreen("test");
        assertThat("beforeIndexAddedToCluster called only once", beforeAddedCount.get(), equalTo(1));
        assertThat("beforeIndexCreated called on each data node", allCreatedCount.get(), greaterThanOrEqualTo(3));

        try {
            client().admin().indices().prepareCreate("failed").setSettings("index.fail", true).get();
            fail("should have thrown an exception during creation");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("failing on purpose"));
            ClusterStateResponse resp = client().admin().cluster().prepareState().get();
            assertFalse(resp.getState().routingTable().indicesRouting().keySet().contains("failed"));
        }
    }

    /**
     * Tests that if an *index* structure creation fails on relocation to a new node, the shard
     * is not stuck but properly failed.
     */
    @Test
    public void testIndexShardFailedOnRelocation() throws Throwable {
        String node1 = internalCluster().startNode();
        client().admin().indices().prepareCreate("index1").setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0).get();
        ensureGreen("index1");
        String node2 = internalCluster().startNode();
        internalCluster().getInstance(IndicesLifecycle.class, node2).addListener(new IndexShardStateChangeListener() {
            @Override
            public void beforeIndexCreated(Index index, @IndexSettings Settings indexSettings) {
                throw new RuntimeException("FAIL");
            }
        });
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(new ShardId("index1", 0), node1, node2)).get();
        ensureGreen("index1");
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        List<ShardRouting> shard = state.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED);
        assertThat(shard, hasSize(1));
        assertThat(state.nodes().resolveNode(shard.get(0).currentNodeId()).getName(), Matchers.equalTo(node1));
    }

    @Test
    public void testIndexStateShardChanged() throws Throwable {

        //start with a single node
        String node1 = internalCluster().startNode();
        IndexShardStateChangeListener stateChangeListenerNode1 = new IndexShardStateChangeListener();
        //add a listener that keeps track of the shard state changes
        internalCluster().getInstance(IndicesLifecycle.class, node1).addListener(stateChangeListenerNode1);

        //create an index that should fail
        try {
            client().admin().indices().prepareCreate("failed").setSettings(SETTING_NUMBER_OF_SHARDS, 1, "index.fail", true).get();
            fail("should have thrown an exception");
        } catch (ElasticsearchException e) {
            assertTrue(e.getMessage().contains("failing on purpose"));
            ClusterStateResponse resp = client().admin().cluster().prepareState().get();
            assertFalse(resp.getState().routingTable().indicesRouting().keySet().contains("failed"));
        }


        //create an index
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 6, SETTING_NUMBER_OF_REPLICAS, 0));
        ensureGreen();
        assertThat(stateChangeListenerNode1.creationSettings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1), equalTo(6));
        assertThat(stateChangeListenerNode1.creationSettings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1), equalTo(0));

        //new shards got started
        assertShardStatesMatch(stateChangeListenerNode1, 6, CREATED, RECOVERING, POST_RECOVERY, STARTED);


        //add a node: 3 out of the 6 shards will be relocated to it
        //disable allocation before starting a new node, as we need to register the listener first
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setPersistentSettings(builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "none")));
        String node2 = internalCluster().startNode();
        IndexShardStateChangeListener stateChangeListenerNode2 = new IndexShardStateChangeListener();
        //add a listener that keeps track of the shard state changes
        internalCluster().getInstance(IndicesLifecycle.class, node2).addListener(stateChangeListenerNode2);
        //re-enable allocation
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setPersistentSettings(builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "all")));
        ensureGreen();

        //the 3 relocated shards get closed on the first node
        assertShardStatesMatch(stateChangeListenerNode1, 3, CLOSED);
        //the 3 relocated shards get created on the second node
        assertShardStatesMatch(stateChangeListenerNode2, 3, CREATED, RECOVERING, POST_RECOVERY, STARTED);


        //increase replicas from 0 to 1
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(builder().put(SETTING_NUMBER_OF_REPLICAS, 1)));
        ensureGreen();

        //3 replicas are allocated to the first node
        assertShardStatesMatch(stateChangeListenerNode1, 3, CREATED, RECOVERING, POST_RECOVERY, STARTED);

        //3 replicas are allocated to the second node
        assertShardStatesMatch(stateChangeListenerNode2, 3, CREATED, RECOVERING, POST_RECOVERY, STARTED);


        //close the index
        assertAcked(client().admin().indices().prepareClose("test"));

        assertThat(stateChangeListenerNode1.afterCloseSettings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1), equalTo(6));
        assertThat(stateChangeListenerNode1.afterCloseSettings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1), equalTo(0));

        assertShardStatesMatch(stateChangeListenerNode1, 6, CLOSED);
        assertShardStatesMatch(stateChangeListenerNode2, 6, CLOSED);
    }

    private static void assertShardStatesMatch(final IndexShardStateChangeListener stateChangeListener, final int numShards, final IndexShardState... shardStates)
            throws InterruptedException {

        BooleanSupplier waitPredicate = () -> {
            if (stateChangeListener.shardStates.size() != numShards) {
                return false;
            }
            for (List<IndexShardState> indexShardStates : stateChangeListener.shardStates.values()) {
                if (indexShardStates == null || indexShardStates.size() != shardStates.length) {
                    return false;
                }
                for (int i = 0; i < shardStates.length; i++) {
                    if (indexShardStates.get(i) != shardStates[i]) {
                        return false;
                    }
                }
            }
            return true;
        };
        if (!awaitBusy(waitPredicate, 1, TimeUnit.MINUTES)) {
            fail("failed to observe expect shard states\n" +
                    "expected: [" + numShards + "] shards with states: " + Strings.arrayToCommaDelimitedString(shardStates) + "\n" +
                    "observed:\n" + stateChangeListener);
        }

        stateChangeListener.shardStates.clear();
    }

    private static class IndexShardStateChangeListener extends IndicesLifecycle.Listener {
        //we keep track of all the states (ordered) a shard goes through
        final ConcurrentMap<ShardId, List<IndexShardState>> shardStates = new ConcurrentHashMap<>();
        Settings creationSettings = Settings.EMPTY;
        Settings afterCloseSettings = Settings.EMPTY;

        @Override
        public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState, IndexShardState newState, @Nullable String reason) {
            List<IndexShardState> shardStates = this.shardStates.putIfAbsent(indexShard.shardId(),
                    new CopyOnWriteArrayList<>(new IndexShardState[]{newState}));
            if (shardStates != null) {
                shardStates.add(newState);
            }
        }

        @Override
        public void beforeIndexCreated(Index index, @IndexSettings Settings indexSettings) {
            this.creationSettings = indexSettings;
            if (indexSettings.getAsBoolean("index.fail", false)) {
                throw new ElasticsearchException("failing on purpose");
            }
        }

        @Override
        public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, @IndexSettings Settings indexSettings) {
            this.afterCloseSettings = indexSettings;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<ShardId, List<IndexShardState>> entry : shardStates.entrySet()) {
                sb.append(entry.getKey()).append(" --> ").append(Strings.collectionToCommaDelimitedString(entry.getValue())).append("\n");
            }
            return sb.toString();
        }
    }
}
