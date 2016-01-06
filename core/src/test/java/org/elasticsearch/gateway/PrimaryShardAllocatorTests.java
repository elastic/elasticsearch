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
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESAllocationTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

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

    public void testNoProcessPrimaryNotAllocatedBefore() {
        final RoutingAllocation allocation;
        if (randomBoolean()) {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), randomBoolean(), Version.CURRENT);
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), true, Version.V_2_1_0);
        }
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().iterator().next().shardId(), equalTo(shardId));
    }

    /**
     * Tests that when async fetch returns that there is no data, the shard will not be allocated.
     */
    public void testNoAsyncFetchData() {
        final RoutingAllocation allocation;
        if (randomBoolean()) {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.CURRENT, "allocId");
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.V_2_1_0);
        }
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests when the node returns that no data was found for it (-1 for version and null for allocation id),
     * it will be moved to ignore unassigned.
     */
    public void testNoAllocationFound() {
        final RoutingAllocation allocation;
        if (randomBoolean()) {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.CURRENT, "allocId");
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.V_2_1_0);
        }
        testAllocator.addData(node1, -1, null);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests when the node returns data with a shard allocation id that does not match active allocation ids, it will be moved to ignore unassigned.
     */
    public void testNoMatchingAllocationIdFound() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.CURRENT, "id2");
        testAllocator.addData(node1, 1, "id1");
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests that when there is a node to allocate the shard to, and there are no active allocation ids, it will be allocated to it.
     * This is the case when we have old shards from pre-3.0 days.
     */
    public void testNoActiveAllocationIds() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.V_2_1_1);
        testAllocator.addData(node1, 1, null);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node1.id()));
    }

    /**
     * Tests when the node returns that no data was found for it, it will be moved to ignore unassigned.
     */
    public void testStoreException() {
        final RoutingAllocation allocation;
        if (randomBoolean()) {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, randomFrom(Version.V_2_0_0, Version.CURRENT), "allocId1");
            testAllocator.addData(node1, 1, "allocId1", new CorruptIndexException("test", "test"));
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.V_2_1_1);
            testAllocator.addData(node1, 3, null, new CorruptIndexException("test", "test"));
        }
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests that when there is a node to allocate the shard to, it will be allocated to it.
     */
    public void testFoundAllocationAndAllocating() {
        final RoutingAllocation allocation;
        if (randomBoolean()) {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, randomFrom(Version.V_2_0_0, Version.CURRENT), "allocId1");
            testAllocator.addData(node1, 1, "allocId1");
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.V_2_2_0);
            testAllocator.addData(node1, 3, null);
        }
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
    public void testFoundAllocationButThrottlingDecider() {
        final RoutingAllocation allocation;
        if (randomBoolean()) {
            allocation = routingAllocationWithOnePrimaryNoReplicas(throttleAllocationDeciders(), false, randomFrom(Version.V_2_0_0, Version.CURRENT), "allocId1");
            testAllocator.addData(node1, 1, "allocId1");
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(throttleAllocationDeciders(), false, Version.V_2_2_0);
            testAllocator.addData(node1, 3, null);
        }
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests that when there is a node to be allocated to, but it the decider said "no", we still
     * force the allocation to it.
     */
    public void testFoundAllocationButNoDecider() {
        final RoutingAllocation allocation;
        if (randomBoolean()) {
            allocation = routingAllocationWithOnePrimaryNoReplicas(noAllocationDeciders(), false, randomFrom(Version.V_2_0_0, Version.CURRENT), "allocId1");
            testAllocator.addData(node1, 1, "allocId1");
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(noAllocationDeciders(), false, Version.V_2_0_0);
            testAllocator.addData(node1, 3, null);
        }
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node1.id()));
    }

    /**
     * Tests that the highest version node is chosen for allocation.
     */
    public void testAllocateToTheHighestVersionOnLegacyIndex() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.V_2_0_0);
        testAllocator.addData(node1, 10, null).addData(node2, 12, null);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node2.id()));
    }

    /**
     * Tests that when restoring from a snapshot and we find a node with a shard copy and allocation
     * deciders say yes, we allocate to that node.
     */
    public void testRestore() {
        RoutingAllocation allocation = getRestoreRoutingAllocation(yesAllocationDeciders());
        testAllocator.addData(node1, 1, randomFrom(null, "allocId"));
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
    }

    /**
     * Tests that when restoring from a snapshot and we find a node with a shard copy and allocation
     * deciders say throttle, we add it to ignored shards.
     */
    public void testRestoreThrottle() {
        RoutingAllocation allocation = getRestoreRoutingAllocation(throttleAllocationDeciders());
        testAllocator.addData(node1, 1, randomFrom(null, "allocId"));
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(false));
    }

    /**
     * Tests that when restoring from a snapshot and we find a node with a shard copy but allocation
     * deciders say no, we still allocate to that node.
     */
    public void testRestoreForcesAllocateIfShardAvailable() {
        RoutingAllocation allocation = getRestoreRoutingAllocation(noAllocationDeciders());
        testAllocator.addData(node1, 1, randomFrom(null, "some allocId"));
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
    }

    /**
     * Tests that when restoring from a snapshot and we don't find a node with a shard copy, the shard will remain in
     * the unassigned list to be allocated later.
     */
    public void testRestoreDoesNotAssignIfNoShardAvailable() {
        RoutingAllocation allocation = getRestoreRoutingAllocation(yesAllocationDeciders());
        testAllocator.addData(node1, -1, null);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().size(), equalTo(1));
    }

    private RoutingAllocation getRestoreRoutingAllocation(AllocationDeciders allocationDeciders) {
        Version version = randomFrom(Version.CURRENT, Version.V_2_0_0);
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder(shardId.getIndex()).settings(settings(version)).numberOfShards(1).numberOfReplicas(0)
                .putActiveAllocationIds(0, version == Version.CURRENT ? new HashSet<>(Arrays.asList("allocId")) : Collections.emptySet()))
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsRestore(metaData.index(shardId.getIndex()), new RestoreSource(new SnapshotId("test", "test"), version, shardId.getIndex()))
            .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
            .metaData(metaData)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();
        return new RoutingAllocation(allocationDeciders, new RoutingNodes(state, false), state.nodes(), null, System.nanoTime());
    }

    /**
     * Tests that when recovering using "recover_on_any_node" and we find a node with a shard copy and allocation
     * deciders say yes, we allocate to that node.
     */
    public void testRecoverOnAnyNode() {
        RoutingAllocation allocation = getRecoverOnAnyNodeRoutingAllocation(yesAllocationDeciders());
        testAllocator.addData(node1, 1, randomFrom(null, "allocId"));
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
    }

    /**
     * Tests that when recovering using "recover_on_any_node" and we find a node with a shard copy and allocation
     * deciders say throttle, we add it to ignored shards.
     */
    public void testRecoverOnAnyNodeThrottle() {
        RoutingAllocation allocation = getRestoreRoutingAllocation(throttleAllocationDeciders());
        testAllocator.addData(node1, 1, randomFrom(null, "allocId"));
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(false));
    }

    /**
     * Tests that when recovering using "recover_on_any_node" and we find a node with a shard copy but allocation
     * deciders say no, we still allocate to that node.
     */
    public void testRecoverOnAnyNodeForcesAllocateIfShardAvailable() {
        RoutingAllocation allocation = getRecoverOnAnyNodeRoutingAllocation(noAllocationDeciders());
        testAllocator.addData(node1, 1, randomFrom(null, "allocId"));
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
    }

    /**
     * Tests that when recovering using "recover_on_any_node" and we don't find a node with a shard copy we let
     * BalancedShardAllocator assign the shard
     */
    public void testRecoverOnAnyNodeDoesNotAssignIfNoShardAvailable() {
        RoutingAllocation allocation = getRecoverOnAnyNodeRoutingAllocation(yesAllocationDeciders());
        testAllocator.addData(node1, -1, null);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().size(), equalTo(1));
    }

    private RoutingAllocation getRecoverOnAnyNodeRoutingAllocation(AllocationDeciders allocationDeciders) {
        Version version = randomFrom(Version.CURRENT, Version.V_2_0_0);
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder(shardId.getIndex()).settings(settings(version)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .put(IndexMetaData.SETTING_SHARED_FS_ALLOW_RECOVERY_ON_ANY_NODE, true))
                .numberOfShards(1).numberOfReplicas(0).putActiveAllocationIds(0, version == Version.CURRENT ? new HashSet<>(Arrays.asList("allocId")) : Collections.emptySet()))
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsRestore(metaData.index(shardId.getIndex()), new RestoreSource(new SnapshotId("test", "test"), Version.CURRENT, shardId.getIndex()))
            .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
            .metaData(metaData)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();
        return new RoutingAllocation(allocationDeciders, new RoutingNodes(state, false), state.nodes(), null, System.nanoTime());
    }

    /**
     * Tests that only when enough copies of the shard exists we are going to allocate it. This test
     * verifies that with same version (1), and quorum allocation.
     */
    public void testEnoughCopiesFoundForAllocationOnLegacyIndex() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(shardId.getIndex()).settings(settings(Version.V_2_0_0)).numberOfShards(1).numberOfReplicas(2))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsRecovery(metaData.index(shardId.getIndex()))
                .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();

        RoutingAllocation allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null, System.nanoTime());
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node1, 1, null);
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null, System.nanoTime());
        changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node2, 1, null);
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null, System.nanoTime());
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
    public void testEnoughCopiesFoundForAllocationOnLegacyIndexWithDifferentVersion() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(shardId.getIndex()).settings(settings(Version.V_2_0_0)).numberOfShards(1).numberOfReplicas(2))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsRecovery(metaData.index(shardId.getIndex()))
                .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();

        RoutingAllocation allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null, System.nanoTime());
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node1, 1, null);
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null, System.nanoTime());
        changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node2, 2, null);
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state.nodes(), null, System.nanoTime());
        changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(0));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node2.id()));
    }

    private RoutingAllocation routingAllocationWithOnePrimaryNoReplicas(AllocationDeciders deciders, boolean asNew, Version version, String... activeAllocationIds) {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(shardId.getIndex()).settings(settings(version))
                    .numberOfShards(1).numberOfReplicas(0).putActiveAllocationIds(0, new HashSet<>(Arrays.asList(activeAllocationIds))))
            .build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        if (asNew) {
            routingTableBuilder.addAsNew(metaData.index(shardId.getIndex()));
        } else {
            routingTableBuilder.addAsRecovery(metaData.index(shardId.getIndex()));
        }
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(routingTableBuilder.build())
                .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state.nodes(), null, System.nanoTime());
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

        public TestAllocator addData(DiscoveryNode node, long version, String allocationId) {
            return addData(node, version, allocationId, null);
        }

        public TestAllocator addData(DiscoveryNode node, long version, String allocationId, @Nullable Throwable storeException) {
            if (data == null) {
                data = new HashMap<>();
            }
            data.put(node, new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(node, version, allocationId, storeException));
            return this;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            return new AsyncShardFetch.FetchResult<>(shardId, data, Collections.<String>emptySet(), Collections.<String>emptySet());
        }
    }
}
