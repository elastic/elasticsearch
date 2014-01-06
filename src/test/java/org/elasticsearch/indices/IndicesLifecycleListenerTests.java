/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.indices;

import com.google.common.collect.Maps;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.allocation.decider.DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION;
import static org.elasticsearch.common.settings.ImmutableSettings.builder;
import static org.elasticsearch.index.shard.IndexShardState.*;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

@ClusterScope(scope = Scope.TEST, numNodes = 0)
public class IndicesLifecycleListenerTests extends ElasticsearchIntegrationTest {

    @Test
    public void testIndexStateShardChanged() throws Throwable {

        //start with a single node
        String node1 = cluster().startNode();
        IndexShardStateChangeListener stateChangeListenerNode1 = new IndexShardStateChangeListener();
        //add a listener that keeps track of the shard state changes
        cluster().getInstance(IndicesLifecycle.class, node1).addListener(stateChangeListenerNode1);

        //create an index
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 6, SETTING_NUMBER_OF_REPLICAS, 0));
        ensureGreen();

        //new shards got started
        assertShardStatesMatch(stateChangeListenerNode1, 6, CREATED, RECOVERING, POST_RECOVERY, STARTED);


        //add a node: 3 out of the 6 shards will be relocated to it
        //disable allocation before starting a new node, as we need to register the listener first
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setPersistentSettings(builder().put(CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, true)));
        String node2 = cluster().startNode();
        IndexShardStateChangeListener stateChangeListenerNode2 = new IndexShardStateChangeListener();
        //add a listener that keeps track of the shard state changes
        cluster().getInstance(IndicesLifecycle.class, node2).addListener(stateChangeListenerNode2);
        //re-enable allocation
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setPersistentSettings(builder().put(CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, false)));
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

        assertShardStatesMatch(stateChangeListenerNode1, 6, CLOSED);
        assertShardStatesMatch(stateChangeListenerNode2, 6, CLOSED);
    }

    private static void assertShardStatesMatch(IndexShardStateChangeListener stateChangeListener, int numShards, IndexShardState... shardStates) {
        assertThat(stateChangeListener.shardStates.size(), equalTo(numShards));
        for (List<IndexShardState> indexShardStates : stateChangeListener.shardStates.values()) {
            assertThat(indexShardStates, notNullValue());
            assertThat(indexShardStates.size(), equalTo(shardStates.length));
            for (int i = 0; i < shardStates.length; i++) {
                assertThat(indexShardStates.get(i), equalTo(shardStates[i]));
            }
        }
        stateChangeListener.shardStates.clear();
    }

    private static class IndexShardStateChangeListener extends IndicesLifecycle.Listener {
        //we keep track of all the states (ordered) a shard goes through
        final ConcurrentMap<ShardId, List<IndexShardState>> shardStates = Maps.newConcurrentMap();

        @Override
        public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState, IndexShardState newState, @Nullable String reason) {
            List<IndexShardState> shardStates = this.shardStates.putIfAbsent(indexShard.shardId(),
                    new CopyOnWriteArrayList<IndexShardState>(new IndexShardState[]{newState}));
            if (shardStates != null) {
                shardStates.add(newState);
            }
        }
    }
}
