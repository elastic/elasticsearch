/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.unit.cluster.structure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.cluster.routing.operation.plain.PlainOperationRouting;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.unit.cluster.routing.allocation.RoutingAllocationTests;
import org.junit.Test;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.metadata.IndexMetaData.newIndexMetaDataBuilder;
import static org.elasticsearch.cluster.metadata.MetaData.newMetaDataBuilder;
import static org.elasticsearch.cluster.node.DiscoveryNodes.newNodesBuilder;
import static org.elasticsearch.cluster.routing.RoutingBuilders.routingTable;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class RoutingIteratorTests {

    @Test
    public void testEmptyIterator() {
        ShardIterator shardIterator = new PlainShardIterator(new ShardId("test1", 0), ImmutableList.<ShardRouting>of(), 0);
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.firstOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = new PlainShardIterator(new ShardId("test1", 0), ImmutableList.<ShardRouting>of(), 1);
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.firstOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = new PlainShardIterator(new ShardId("test1", 0), ImmutableList.<ShardRouting>of(), 2);
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.firstOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = new PlainShardIterator(new ShardId("test1", 0), ImmutableList.<ShardRouting>of(), 3);
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.firstOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
    }

    @Test
    public void testIterator1() {
        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test1").numberOfShards(1).numberOfReplicas(2))
                .build();
        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test1"))
                .build();

        ShardIterator shardIterator = routingTable.index("test1").shard(0).shardsIt(0);
        assertThat(shardIterator.size(), equalTo(3));
        ShardRouting firstRouting = shardIterator.firstOrNull();
        assertThat(shardIterator.firstOrNull(), notNullValue());
        assertThat(shardIterator.remaining(), equalTo(3));
        assertThat(shardIterator.firstOrNull(), sameInstance(shardIterator.firstOrNull()));
        assertThat(shardIterator.remaining(), equalTo(3));
        ShardRouting shardRouting1 = shardIterator.nextOrNull();
        assertThat(firstRouting, sameInstance(shardRouting1));
        assertThat(shardRouting1, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(2));
        ShardRouting shardRouting2 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(1));
        assertThat(shardRouting2, not(sameInstance(shardRouting1)));
        ShardRouting shardRouting3 = shardIterator.nextOrNull();
        assertThat(shardRouting3, notNullValue());
        assertThat(shardRouting3, not(sameInstance(shardRouting1)));
        assertThat(shardRouting3, not(sameInstance(shardRouting2)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
    }

    @Test
    public void testIterator2() {
        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(newIndexMetaDataBuilder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ShardIterator shardIterator = routingTable.index("test1").shard(0).shardsIt(0);
        assertThat(shardIterator.size(), equalTo(2));
        ShardRouting firstRouting = shardIterator.firstOrNull();
        assertThat(shardIterator.firstOrNull(), notNullValue());
        assertThat(shardIterator.remaining(), equalTo(2));
        assertThat(shardIterator.firstOrNull(), sameInstance(shardIterator.firstOrNull()));
        assertThat(shardIterator.remaining(), equalTo(2));
        ShardRouting shardRouting1 = shardIterator.nextOrNull();
        assertThat(shardRouting1, sameInstance(firstRouting));
        assertThat(shardRouting1, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(1));
        ShardRouting shardRouting2 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardRouting2, not(sameInstance(shardRouting1)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(1);
        assertThat(shardIterator.size(), equalTo(2));
        firstRouting = shardIterator.firstOrNull();
        assertThat(shardIterator.firstOrNull(), notNullValue());
        assertThat(shardIterator.firstOrNull(), sameInstance(shardIterator.firstOrNull()));
        ShardRouting shardRouting3 = shardIterator.nextOrNull();
        assertThat(firstRouting, sameInstance(shardRouting3));
        assertThat(shardRouting1, notNullValue());
        ShardRouting shardRouting4 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        assertThat(shardRouting2, not(sameInstance(shardRouting1)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting1, not(sameInstance(shardRouting3)));
        assertThat(shardRouting2, not(sameInstance(shardRouting4)));
        assertThat(shardRouting1, sameInstance(shardRouting4));
        assertThat(shardRouting2, sameInstance(shardRouting3));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(2);
        assertThat(shardIterator.size(), equalTo(2));
        firstRouting = shardIterator.firstOrNull();
        assertThat(shardIterator.firstOrNull(), notNullValue());
        assertThat(shardIterator.firstOrNull(), sameInstance(shardIterator.firstOrNull()));
        ShardRouting shardRouting5 = shardIterator.nextOrNull();
        assertThat(shardRouting5, sameInstance(firstRouting));
        assertThat(shardRouting5, notNullValue());
        ShardRouting shardRouting6 = shardIterator.nextOrNull();
        assertThat(shardRouting6, notNullValue());
        assertThat(shardRouting6, not(sameInstance(shardRouting5)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting5, sameInstance(shardRouting1));
        assertThat(shardRouting6, sameInstance(shardRouting2));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(3);
        assertThat(shardIterator.size(), equalTo(2));
        firstRouting = shardIterator.firstOrNull();
        assertThat(shardIterator.firstOrNull(), notNullValue());
        assertThat(shardIterator.firstOrNull(), sameInstance(shardIterator.firstOrNull()));
        ShardRouting shardRouting7 = shardIterator.nextOrNull();
        assertThat(shardRouting7, sameInstance(firstRouting));
        assertThat(shardRouting7, notNullValue());
        ShardRouting shardRouting8 = shardIterator.nextOrNull();
        assertThat(shardRouting8, notNullValue());
        assertThat(shardRouting8, not(sameInstance(shardRouting7)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting7, sameInstance(shardRouting3));
        assertThat(shardRouting8, sameInstance(shardRouting4));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(4);
        assertThat(shardIterator.size(), equalTo(2));
        firstRouting = shardIterator.firstOrNull();
        assertThat(shardIterator.firstOrNull(), notNullValue());
        assertThat(shardIterator.firstOrNull(), sameInstance(shardIterator.firstOrNull()));
        ShardRouting shardRouting9 = shardIterator.nextOrNull();
        assertThat(shardRouting9, sameInstance(firstRouting));
        assertThat(shardRouting9, notNullValue());
        ShardRouting shardRouting10 = shardIterator.nextOrNull();
        assertThat(shardRouting10, notNullValue());
        assertThat(shardRouting10, not(sameInstance(shardRouting9)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting9, sameInstance(shardRouting5));
        assertThat(shardRouting10, sameInstance(shardRouting6));
    }

    @Test
    public void testRandomRouting() {
        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(newIndexMetaDataBuilder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ShardIterator shardIterator = routingTable.index("test1").shard(0).shardsRandomIt();
        ShardRouting shardRouting1 = shardIterator.nextOrNull();
        assertThat(shardRouting1, notNullValue());
        assertThat(shardIterator.nextOrNull(), notNullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        shardIterator = routingTable.index("test1").shard(0).shardsRandomIt();
        ShardRouting shardRouting2 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        ShardRouting shardRouting3 = shardIterator.nextOrNull();
        assertThat(shardRouting3, notNullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardRouting1, not(sameInstance(shardRouting2)));
        assertThat(shardRouting1, sameInstance(shardRouting3));
    }

    @Test
    public void testAttributePreferenceRouting() {
        AllocationService strategy = new AllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put("cluster.routing.allocation.allow_rebalance", "always")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id,zone")
                .build());

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .put(RoutingAllocationTests.newNode("node1", ImmutableMap.of("rack_id", "rack_1", "zone", "zone1")))
                .put(RoutingAllocationTests.newNode("node2", ImmutableMap.of("rack_id", "rack_2", "zone", "zone2")))
                .localNodeId("node1")
        ).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        // after all are started, check routing iteration
        ShardIterator shardIterator = clusterState.routingTable().index("test").shard(0).preferAttributesActiveInitializingShardsIt(new String[]{"rack_id"}, clusterState.nodes());
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node1"));
        shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node2"));

        shardIterator = clusterState.routingTable().index("test").shard(0).preferAttributesActiveInitializingShardsIt(new String[]{"rack_id"}, clusterState.nodes());
        shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node1"));
        shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node2"));
    }


    @Test
    public void testShardsAndPreferNodeRouting() {
        AllocationService strategy = new AllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .build());

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .put(RoutingAllocationTests.newNode("node1"))
                .put(RoutingAllocationTests.newNode("node2"))
                .localNodeId("node1")
        ).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        PlainOperationRouting operationRouting = new PlainOperationRouting(ImmutableSettings.Builder.EMPTY_SETTINGS, new DjbHashFunction(), new AwarenessAllocationDecider());

        GroupShardsIterator shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:0");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));

        shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:1");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(1));

        //check node preference, first without preference to see they switch
        shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:0;");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));
        String firstRoundNodeId = shardIterators.iterator().next().nextOrNull().currentNodeId();

        shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:0");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));
        assertThat(shardIterators.iterator().next().nextOrNull().currentNodeId(), not(equalTo(firstRoundNodeId)));

        shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:0;_prefer_node:node1");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));
        assertThat(shardIterators.iterator().next().nextOrNull().currentNodeId(), equalTo("node1"));

        shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:0;_prefer_node:node1");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));
        assertThat(shardIterators.iterator().next().nextOrNull().currentNodeId(), equalTo("node1"));
    }
}