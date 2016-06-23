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
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESAllocationTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class StartedShardsRoutingTests extends ESAllocationTestCase {
    public void testStartedShardsMatching() {
        AllocationService allocation = createAllocationService();

        logger.info("--> building initial cluster state");
        final IndexMetaData indexMetaData = IndexMetaData.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(3).numberOfReplicas(0)
                .build();
        final Index index = indexMetaData.getIndex();
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")))
                .metaData(MetaData.builder().put(indexMetaData, false));

        final ShardRouting initShard = TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.INITIALIZING);
        final ShardRouting startedShard = TestShardRouting.newShardRouting(new ShardId(index, 1), "node2", true, ShardRoutingState.STARTED);
        final ShardRouting relocatingShard = TestShardRouting.newShardRouting(new ShardId(index, 2), "node1", "node2", true, ShardRoutingState.RELOCATING);
        stateBuilder.routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(index)
                .addIndexShard(new IndexShardRoutingTable.Builder(initShard.shardId()).addShard(initShard).build())
                .addIndexShard(new IndexShardRoutingTable.Builder(startedShard.shardId()).addShard(startedShard).build())
                .addIndexShard(new IndexShardRoutingTable.Builder(relocatingShard.shardId()).addShard(relocatingShard).build())).build());

        ClusterState state = stateBuilder.build();

        logger.info("--> test starting of shard");

        RoutingAllocation.Result result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(initShard.shardId(), initShard.currentNodeId(), initShard.relocatingNodeId(), initShard.primary(),
                        ShardRoutingState.INITIALIZING, initShard.allocationId())), false);
        assertTrue("failed to start " + initShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());
        assertTrue(initShard + "isn't started \ncurrent routing table:" + result.routingTable().prettyPrint(),
                result.routingTable().index("test").shard(initShard.id()).allShardsStarted());


        logger.info("--> testing shard variants that shouldn't match the initializing shard");

        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(initShard.shardId(), initShard.currentNodeId(), initShard.relocatingNodeId(), initShard.primary(),
                        ShardRoutingState.INITIALIZING)), false);
        assertFalse("wrong allocation id flag shouldn't start shard " + initShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(initShard.shardId(), "some_node", initShard.currentNodeId(), initShard.primary(),
                        ShardRoutingState.INITIALIZING, AllocationId.newTargetRelocation(AllocationId.newRelocation(initShard.allocationId())))), false);
        assertFalse("relocating shard from node shouldn't start shard " + initShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());



        logger.info("--> testing double starting");

        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(startedShard.shardId(), startedShard.currentNodeId(), startedShard.relocatingNodeId(), startedShard.primary(),
                        ShardRoutingState.INITIALIZING, startedShard.allocationId())), false);
        assertFalse("duplicate starting of the same shard should be ignored \ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        logger.info("--> testing starting of relocating shards");
        final AllocationId targetAllocationId = AllocationId.newTargetRelocation(relocatingShard.allocationId());
        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(relocatingShard.shardId(), relocatingShard.relocatingNodeId(), relocatingShard.currentNodeId(), relocatingShard.primary(),
                        ShardRoutingState.INITIALIZING, targetAllocationId)), false);

        assertTrue("failed to start " + relocatingShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());
        ShardRouting shardRouting = result.routingTable().index("test").shard(relocatingShard.id()).getShards().get(0);
        assertThat(shardRouting.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(shardRouting.currentNodeId(), equalTo("node2"));
        assertThat(shardRouting.relocatingNodeId(), nullValue());

        logger.info("--> testing shard variants that shouldn't match the initializing relocating shard");

        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(relocatingShard.shardId(), relocatingShard.relocatingNodeId(), relocatingShard.currentNodeId(), relocatingShard.primary(),
                        ShardRoutingState.INITIALIZING)));
        assertFalse("wrong allocation id shouldn't start shard" + relocatingShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(relocatingShard.shardId(), relocatingShard.relocatingNodeId(), relocatingShard.currentNodeId(), relocatingShard.primary(),
                        ShardRoutingState.INITIALIZING, relocatingShard.allocationId())), false);
        assertFalse("wrong allocation id shouldn't start shard even if relocatingId==shard.id" + relocatingShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

    }
}
