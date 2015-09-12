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

package org.elasticsearch.gateway;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESAllocationTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.Matchers.*;

/**
 */
public class PrimaryShardAllocatorTests extends ESAllocationTestCase {

    private final ShardId shardId = new ShardId("test", 0);
    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    private final DiscoveryNode node3 = newNode("node3");
    private TestAllocator testAllocator;

    @Before
    public void buildTestAllocator() {
        this.testAllocator = new TestAllocator();
    }

    /**
     * Verifies that the canProcess method of primary allocation behaves correctly
     * and processes only the applicable shard.
     */
    @Test
    public void testNoProcessReplica() {
        ShardRouting shard = TestShardRouting.newShardRouting("test", 0, null, null, null, false, ShardRoutingState.UNASSIGNED, 0, new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null));
        assertThat(testAllocator.needToFindPrimaryCopy(shard), equalTo(false));
    }

    @Test
    public void testNoProcessPrimayNotAllcoatedBefore() {
        ShardRouting shard = TestShardRouting.newShardRouting("test", 0, null, null, null, true, ShardRoutingState.UNASSIGNED, 0, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        assertThat(testAllocator.needToFindPrimaryCopy(shard), equalTo(false));
    }

    /**
     * Tests that when async fetch returns that there is no data, the shard will not be allocated.
     */
    @Test
    public void testNoAsyncFetchData() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders());
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests when the node returns that no data was found for it (-1), it will be moved to ignore unassigned.
     */
    @Test
    public void testNoAllocationFound() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders());
        testAllocator.addData(node1, -1);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests when the node returns that no data was found for it (-1), it will be moved to ignore unassigned.
     */
    @Test
    public void testStoreException() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders());
        testAllocator.addData(node1, 3, new CorruptIndexException("test", "test"));
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests that when there is a node to allocate the shard to, it will be allocated to it.
     */
    @Test
    public void testFoundAllocationAndAllocating() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders());
        testAllocator.addData(node1, 10);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node1.id()));
    }

    /**
     * Tests that when there is a node to allocate to, but it is throttling (and it is the only one),
     * it will be moved to ignore unassigned until it can be allocated to.
     */
    @Test
    public void testFoundAllocationButThrottlingDecider() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(throttleAllocationDeciders());
        testAllocator.addData(node1, 10);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests that when there is a node to be allocated to, but it the decider said "no", we still
     * force the allocation to it.
     */
    @Test
    public void testFoundAllocationButNoDecider() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(noAllocationDeciders());
        testAllocator.addData(node1, 10);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node1.id()));
    }

    /**
     * Tests that the highest version node is chosed for allocation.
     */
    @Test
    public void testAllocateToTheHighestVersion() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders());
        testAllocator.addData(node1, 10).addData(node2, 12);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node2.id()));
    }

    /**
     * Tests that when restoring from snapshot, even if we didn't find any node to allocate on, the shard
     * will remain in the unassigned list to be allocated later.
     */
    @Test
    public void testRestoreIgnoresNoNodesToAllocate() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(shardId.getIndex()).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsRestore(metaData.index(shardId.getIndex()), new RestoreSource(new SnapshotId("test", "test"), Version.CURRENT, shardId.getIndex()))
                .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();
        RoutingAllocation allocation = new RoutingAllocation(yesAllocationDeciders(), state.getRoutingNodes(), state.nodes(), null);

        testAllocator.addData(node1, -1).addData(node2, -1);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
    }

    /**
     * Tests that only when enough copies of the shard exists we are going to allocate it. This test
     * verifies that with same version (1), and quorum allocation.
     */
    @Test
    public void testEnoughCopiesFoundForAllocation() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(shardId.getIndex()).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsRecovery(metaData.index(shardId.getIndex()))
                .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();

        RoutingAllocation allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node1, 1);
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null);
        changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node2, 1);
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null);
        changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(0));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), anyOf(equalTo(node2.id()), equalTo(node1.id())));
    }

    /**
     * Tests that only when enough copies of the shard exists we are going to allocate it. This test
     * verifies that even with different version, we treat different versions as a copy, and count them.
     */
    @Test
    public void testEnoughCopiesFoundForAllocationWithDifferentVersion() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(shardId.getIndex()).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsRecovery(metaData.index(shardId.getIndex()))
                .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();

        RoutingAllocation allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node1, 1);
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null);
        changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node2, 2);
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null);
        changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(0));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node2.id()));
    }

    @Test
    public void testAllocationOnAnyNodeWithSharedFs() {
        ShardRouting shard = TestShardRouting.newShardRouting("test", 0, null, null, null, false,
                ShardRoutingState.UNASSIGNED, 0,
                new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null));

        Map<DiscoveryNode, TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> data = new HashMap<>();
        data.put(node1, new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(node1, 1));
        data.put(node2, new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(node2, 5));
        data.put(node3, new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(node3, -1));
        AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetches =
                new AsyncShardFetch.FetchResult(shardId, data, new HashSet<>(), new HashSet<>());

        PrimaryShardAllocator.NodesAndVersions nAndV = testAllocator.buildNodesAndVersions(shard, false, new HashSet<String>(), fetches);
        assertThat(nAndV.allocationsFound, equalTo(2));
        assertThat(nAndV.highestVersion, equalTo(5L));
        assertThat(nAndV.nodes, contains(node2));

        nAndV = testAllocator.buildNodesAndVersions(shard, true, new HashSet<String>(), fetches);
        assertThat(nAndV.allocationsFound, equalTo(3));
        assertThat(nAndV.highestVersion, equalTo(5L));
        // All three nodes are potential candidates because shards can be recovered on any node
        assertThat(nAndV.nodes, contains(node2, node1, node3));
    }


    @Test
    public void testAllocationOnAnyNodeShouldPutNodesWithExceptionsLast() {
        ShardRouting shard = TestShardRouting.newShardRouting("test", 0, null, null, null, false,
                ShardRoutingState.UNASSIGNED, 0,
                new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null));

        Map<DiscoveryNode, TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> data = new HashMap<>();
        data.put(node1, new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(node1, 1));
        data.put(node2, new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(node2, 1));
        data.put(node3, new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(node3, 1, new IOException("I failed to open")));
        HashSet<String> ignoredNodes = new HashSet<>();
        ignoredNodes.add(node2.id());
        AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetches =
                new AsyncShardFetch.FetchResult(shardId, data, new HashSet<>(), ignoredNodes);

        PrimaryShardAllocator.NodesAndVersions nAndV = testAllocator.buildNodesAndVersions(shard, false, ignoredNodes, fetches);
        assertThat(nAndV.allocationsFound, equalTo(1));
        assertThat(nAndV.highestVersion, equalTo(1L));
        assertThat(nAndV.nodes, contains(node1));

        nAndV = testAllocator.buildNodesAndVersions(shard, true, ignoredNodes, fetches);
        assertThat(nAndV.allocationsFound, equalTo(2));
        assertThat(nAndV.highestVersion, equalTo(1L));
        // node3 should be last here
        assertThat(nAndV.nodes.size(), equalTo(2));
        assertThat(nAndV.nodes, contains(node1, node3));
    }

    private RoutingAllocation routingAllocationWithOnePrimaryNoReplicas(AllocationDeciders deciders) {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(shardId.getIndex()).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsRecovery(metaData.index(shardId.getIndex()))
                .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state.nodes(), null);
    }

    class TestAllocator extends PrimaryShardAllocator {

        private Map<DiscoveryNode, TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> data;

        public TestAllocator() {
            super(Settings.EMPTY);
        }

        public TestAllocator clear() {
            data = null;
            return this;
        }

        public TestAllocator addData(DiscoveryNode node, long version) {
            return addData(node, version, null);
        }

        public TestAllocator addData(DiscoveryNode node, long version, @Nullable Throwable storeException) {
            if (data == null) {
                data = new HashMap<>();
            }
            data.put(node, new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(node, version, storeException));
            return this;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            return new AsyncShardFetch.FetchResult<>(shardId, data, Collections.<String>emptySet(), Collections.<String>emptySet());
        }
    }
}
