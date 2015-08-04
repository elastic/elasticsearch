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
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.test.ESAllocationTestCase;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class StartedShardsRoutingTests extends ESAllocationTestCase {

    @Test
    public void tesStartedShardsMatching() {
        AllocationService allocation = createAllocationService();

        logger.info("--> building initial cluster state");
        final IndexMetaData indexMetaData = IndexMetaData.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(3).numberOfReplicas(0)
                .build();
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")))
                .metaData(MetaData.builder().put(indexMetaData, false));

        final ShardRouting initShard = TestShardRouting.newShardRouting("test", 0, "node1", randomBoolean(), ShardRoutingState.INITIALIZING, 1);
        final ShardRouting startedShard = TestShardRouting.newShardRouting("test", 1, "node2", randomBoolean(), ShardRoutingState.STARTED, 1);
        final ShardRouting relocatingShard = TestShardRouting.newShardRouting("test", 2, "node1", "node2", randomBoolean(), ShardRoutingState.RELOCATING, 1);
        stateBuilder.routingTable(RoutingTable.builder().add(IndexRoutingTable.builder("test")
                .addIndexShard(new IndexShardRoutingTable.Builder(initShard.shardId()).addShard(initShard).build())
                .addIndexShard(new IndexShardRoutingTable.Builder(startedShard.shardId()).addShard(startedShard).build())
                .addIndexShard(new IndexShardRoutingTable.Builder(relocatingShard.shardId()).addShard(relocatingShard).build())));

        ClusterState state = stateBuilder.build();

        logger.info("--> test starting of shard");

        RoutingAllocation.Result result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(initShard.index(), initShard.id(), initShard.currentNodeId(), initShard.relocatingNodeId(), initShard.primary(),
                        ShardRoutingState.INITIALIZING, initShard.allocationId(), randomInt())), false);
        assertTrue("failed to start " + initShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());
        assertTrue(initShard + "isn't started \ncurrent routing table:" + result.routingTable().prettyPrint(),
                result.routingTable().index("test").shard(initShard.id()).allShardsStarted());


        logger.info("--> testing shard variants that shouldn't match the initializing shard");

        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(initShard.index(), initShard.id(), initShard.currentNodeId(), initShard.relocatingNodeId(), initShard.primary(),
                        ShardRoutingState.INITIALIZING, 1)), false);
        assertFalse("wrong allocation id flag shouldn't start shard " + initShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(initShard.index(), initShard.id(), "some_node", initShard.currentNodeId(), initShard.primary(),
                        ShardRoutingState.INITIALIZING, AllocationId.newTargetRelocation(AllocationId.newRelocation(initShard.allocationId()))
                        , 1)), false);
        assertFalse("relocating shard from node shouldn't start shard " + initShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());



        logger.info("--> testing double starting");

        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(startedShard.index(), startedShard.id(), startedShard.currentNodeId(), startedShard.relocatingNodeId(), startedShard.primary(),
                        ShardRoutingState.INITIALIZING, startedShard.allocationId(), 1)), false);
        assertFalse("duplicate starting of the same shard should be ignored \ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        logger.info("--> testing starting of relocating shards");
        final AllocationId targetAllocationId = AllocationId.newTargetRelocation(relocatingShard.allocationId());
        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(relocatingShard.index(), relocatingShard.id(), relocatingShard.relocatingNodeId(), relocatingShard.currentNodeId(), relocatingShard.primary(),
                        ShardRoutingState.INITIALIZING, targetAllocationId, randomInt())), false);

        assertTrue("failed to start " + relocatingShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());
        ShardRouting shardRouting = result.routingTable().index("test").shard(relocatingShard.id()).getShards().get(0);
        assertThat(shardRouting.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(shardRouting.currentNodeId(), equalTo("node2"));
        assertThat(shardRouting.relocatingNodeId(), nullValue());

        logger.info("--> testing shard variants that shouldn't match the initializing relocating shard");

        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(relocatingShard.index(), relocatingShard.id(), relocatingShard.relocatingNodeId(), relocatingShard.currentNodeId(), relocatingShard.primary(),
                        ShardRoutingState.INITIALIZING, relocatingShard.version())));
        assertFalse("wrong allocation id shouldn't start shard" + relocatingShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        result = allocation.applyStartedShards(state, Arrays.asList(
                TestShardRouting.newShardRouting(relocatingShard.index(), relocatingShard.id(), relocatingShard.relocatingNodeId(), relocatingShard.currentNodeId(), relocatingShard.primary(),
                        ShardRoutingState.INITIALIZING, relocatingShard.allocationId(), randomInt())), false);
        assertFalse("wrong allocation id shouldn't start shard even if relocatingId==shard.id" + relocatingShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

    }
}
