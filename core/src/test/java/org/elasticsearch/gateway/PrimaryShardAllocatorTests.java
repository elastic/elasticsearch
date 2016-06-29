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
import org.elasticsearch.snapshots.SnapshotId;
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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardStateMetaData;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.test.ESAllocationTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class PrimaryShardAllocatorTests extends ESAllocationTestCase {

    private final ShardId shardId = new ShardId("test", "_na_", 0);
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
     * Tests when the node returns that no data was found for it ({@link ShardStateMetaData#NO_VERSION} for version and null for allocation id),
     * it will be moved to ignore unassigned.
     */
    public void testNoAllocationFound() {
        final RoutingAllocation allocation;
        if (randomBoolean()) {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.CURRENT, "allocId");
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.V_2_1_0);
        }
        testAllocator.addData(node1, ShardStateMetaData.NO_VERSION, null, randomBoolean());
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
        testAllocator.addData(node1, ShardStateMetaData.NO_VERSION, "id1", randomBoolean());
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
        testAllocator.addData(node1, 1, null, randomBoolean());
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node1.getId()));
    }

    /**
     * Tests when the node returns that no data was found for it, it will be moved to ignore unassigned.
     */
    public void testStoreException() {
        final RoutingAllocation allocation;
        if (randomBoolean()) {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, randomFrom(Version.V_2_0_0, Version.CURRENT), "allocId1");
            testAllocator.addData(node1, ShardStateMetaData.NO_VERSION, "allocId1", randomBoolean(), new CorruptIndexException("test", "test"));
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.V_2_1_1);
            testAllocator.addData(node1, 3, null, randomBoolean(), new CorruptIndexException("test", "test"));
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
        boolean useAllocationIds = randomBoolean();
        if (useAllocationIds) {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, randomFrom(Version.V_2_0_0, Version.CURRENT), "allocId1");
            testAllocator.addData(node1, ShardStateMetaData.NO_VERSION, "allocId1", randomBoolean());
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.V_2_2_0);
            testAllocator.addData(node1, 3, null, randomBoolean());
        }
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node1.getId()));
        if (useAllocationIds) {
            // check that allocation id is reused
            assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).allocationId().getId(), equalTo("allocId1"));
        }
    }

    /**
     * Tests that when there was a node that previously had the primary, it will be allocated to that same node again.
     */
    public void testPreferAllocatingPreviousPrimary() {
        String primaryAllocId = UUIDs.randomBase64UUID();
        String replicaAllocId = UUIDs.randomBase64UUID();
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, randomFrom(Version.V_2_0_0, Version.CURRENT), primaryAllocId, replicaAllocId);
        boolean node1HasPrimaryShard = randomBoolean();
        testAllocator.addData(node1, ShardStateMetaData.NO_VERSION, node1HasPrimaryShard ? primaryAllocId : replicaAllocId, node1HasPrimaryShard);
        testAllocator.addData(node2, ShardStateMetaData.NO_VERSION, node1HasPrimaryShard ? replicaAllocId : primaryAllocId, !node1HasPrimaryShard);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        DiscoveryNode allocatedNode = node1HasPrimaryShard ? node1 : node2;
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(allocatedNode.getId()));
    }

    /**
     * Tests that when there is a node to allocate to, but it is throttling (and it is the only one),
     * it will be moved to ignore unassigned until it can be allocated to.
     */
    public void testFoundAllocationButThrottlingDecider() {
        final RoutingAllocation allocation;
        if (randomBoolean()) {
            allocation = routingAllocationWithOnePrimaryNoReplicas(throttleAllocationDeciders(), false, randomFrom(Version.V_2_0_0, Version.CURRENT), "allocId1");
            testAllocator.addData(node1, ShardStateMetaData.NO_VERSION, "allocId1", randomBoolean());
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(throttleAllocationDeciders(), false, Version.V_2_2_0);
            testAllocator.addData(node1, 3, null, randomBoolean());
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
            testAllocator.addData(node1, ShardStateMetaData.NO_VERSION, "allocId1", randomBoolean());
        } else {
            allocation = routingAllocationWithOnePrimaryNoReplicas(noAllocationDeciders(), false, Version.V_2_0_0);
            testAllocator.addData(node1, 3, null, randomBoolean());
        }
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node1.getId()));
    }

    /**
     * Tests that the highest version node is chosen for allocation.
     */
    public void testAllocateToTheHighestVersionOnLegacyIndex() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.V_2_0_0);
        testAllocator.addData(node1, 10, null, randomBoolean()).addData(node2, 12, null, randomBoolean());
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node2.getId()));
    }

    /**
     * Tests that shard with allocation id is chosen if such a shard is available in version-based allocation mode. This happens if a shard
     * was already selected in a 5.x cluster as primary for recovery, was initialized (and wrote a new state file) but did not make it to
     * STARTED state before the cluster crashed (otherwise list of active allocation ids would be non-empty and allocation id - based
     * allocation mode would be chosen).
     */
    public void testVersionBasedAllocationPrefersShardWithAllocationId() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), false, Version.V_2_0_0);
        testAllocator.addData(node1, 10, null, randomBoolean());
        testAllocator.addData(node2, ShardStateMetaData.NO_VERSION, "some allocId", randomBoolean());
        testAllocator.addData(node3, 12, null, randomBoolean());
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node2.getId()));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).allocationId().getId(), equalTo("some allocId"));
    }

    /**
     * Tests that when restoring from a snapshot and we find a node with a shard copy and allocation
     * deciders say yes, we allocate to that node.
     */
    public void testRestore() {
        boolean shardStateHasAllocationId = randomBoolean();
        String allocationId = shardStateHasAllocationId ? "some allocId" : null;
        long legacyVersion = shardStateHasAllocationId ? ShardStateMetaData.NO_VERSION : 1;
        boolean clusterHasActiveAllocationIds = shardStateHasAllocationId ? randomBoolean() : false;
        RoutingAllocation allocation = getRestoreRoutingAllocation(yesAllocationDeciders(), clusterHasActiveAllocationIds);
        testAllocator.addData(node1, legacyVersion, allocationId, randomBoolean());
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
        boolean shardStateHasAllocationId = randomBoolean();
        String allocationId = shardStateHasAllocationId ? "some allocId" : null;
        long legacyVersion = shardStateHasAllocationId ? ShardStateMetaData.NO_VERSION : 1;
        boolean clusterHasActiveAllocationIds = shardStateHasAllocationId ? randomBoolean() : false;
        RoutingAllocation allocation = getRestoreRoutingAllocation(throttleAllocationDeciders(), clusterHasActiveAllocationIds);
        testAllocator.addData(node1, legacyVersion, allocationId, randomBoolean());
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(false));
    }

    /**
     * Tests that when restoring from a snapshot and we find a node with a shard copy but allocation
     * deciders say no, we still allocate to that node.
     */
    public void testRestoreForcesAllocateIfShardAvailable() {
        boolean shardStateHasAllocationId = randomBoolean();
        String allocationId = shardStateHasAllocationId ? "some allocId" : null;
        long legacyVersion = shardStateHasAllocationId ? ShardStateMetaData.NO_VERSION : 1;
        boolean clusterHasActiveAllocationIds = shardStateHasAllocationId ? randomBoolean() : false;
        RoutingAllocation allocation = getRestoreRoutingAllocation(noAllocationDeciders(), clusterHasActiveAllocationIds);
        testAllocator.addData(node1, legacyVersion, allocationId, randomBoolean());
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
        RoutingAllocation allocation = getRestoreRoutingAllocation(yesAllocationDeciders(), randomBoolean());
        testAllocator.addData(node1, ShardStateMetaData.NO_VERSION, null, false);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().size(), equalTo(1));
    }

    private RoutingAllocation getRestoreRoutingAllocation(AllocationDeciders allocationDeciders, boolean hasActiveAllocation) {
        Version version = hasActiveAllocation ? Version.CURRENT : Version.V_2_0_0;
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder(shardId.getIndexName()).settings(settings(version)).numberOfShards(1).numberOfReplicas(0)
                .putActiveAllocationIds(0, hasActiveAllocation ? Sets.newHashSet("allocId") : Collections.emptySet()))
            .build();

        final Snapshot snapshot = new Snapshot("test", new SnapshotId("test", UUIDs.randomBase64UUID()));
        RoutingTable routingTable = RoutingTable.builder()
            .addAsRestore(metaData.index(shardId.getIndex()), new RestoreSource(snapshot, version, shardId.getIndexName()))
            .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metaData(metaData)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();
        return new RoutingAllocation(allocationDeciders, new RoutingNodes(state, false), state, null, System.nanoTime(), false);
    }

    /**
     * Tests that when recovering using "recover_on_any_node" and we find a node with a shard copy and allocation
     * deciders say yes, we allocate to that node.
     */
    public void testRecoverOnAnyNode() {
        boolean hasActiveAllocation = randomBoolean();
        String allocationId = hasActiveAllocation ? "allocId" : null;
        long legacyVersion = hasActiveAllocation ? ShardStateMetaData.NO_VERSION : 1;
        RoutingAllocation allocation = getRecoverOnAnyNodeRoutingAllocation(yesAllocationDeciders(), hasActiveAllocation);
        testAllocator.addData(node1, legacyVersion, allocationId, randomBoolean());
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
        boolean hasActiveAllocation = randomBoolean();
        String allocationId = hasActiveAllocation ? "allocId" : null;
        long legacyVersion = hasActiveAllocation ? ShardStateMetaData.NO_VERSION : 1;
        RoutingAllocation allocation = getRecoverOnAnyNodeRoutingAllocation(throttleAllocationDeciders(), hasActiveAllocation);
        testAllocator.addData(node1, legacyVersion, allocationId, randomBoolean());
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(false));
    }

    /**
     * Tests that when recovering using "recover_on_any_node" and we find a node with a shard copy but allocation
     * deciders say no, we still allocate to that node.
     */
    public void testRecoverOnAnyNodeForcesAllocateIfShardAvailable() {
        boolean hasActiveAllocation = randomBoolean();
        String allocationId = hasActiveAllocation ? "allocId" : null;
        long legacyVersion = hasActiveAllocation ? ShardStateMetaData.NO_VERSION : 1;
        RoutingAllocation allocation = getRecoverOnAnyNodeRoutingAllocation(noAllocationDeciders(), hasActiveAllocation);
        testAllocator.addData(node1, legacyVersion, allocationId, randomBoolean());
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
        RoutingAllocation allocation = getRecoverOnAnyNodeRoutingAllocation(yesAllocationDeciders(), randomBoolean());
        testAllocator.addData(node1, ShardStateMetaData.NO_VERSION, null, randomBoolean());
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().size(), equalTo(1));
    }

    private RoutingAllocation getRecoverOnAnyNodeRoutingAllocation(AllocationDeciders allocationDeciders, boolean hasActiveAllocation) {
        Version version = hasActiveAllocation ? Version.CURRENT : Version.V_2_0_0;
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder(shardId.getIndexName()).settings(settings(version)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .put(IndexMetaData.SETTING_SHARED_FS_ALLOW_RECOVERY_ON_ANY_NODE, true))
                .numberOfShards(1).numberOfReplicas(0).putActiveAllocationIds(0, hasActiveAllocation ? Sets.newHashSet("allocId") : Collections.emptySet()))
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsRestore(metaData.index(shardId.getIndex()), new RestoreSource(new Snapshot("test", new SnapshotId("test", UUIDs.randomBase64UUID())), Version.CURRENT, shardId.getIndexName()))
            .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metaData(metaData)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();
        return new RoutingAllocation(allocationDeciders, new RoutingNodes(state, false), state, null, System.nanoTime(), false);
    }

    /**
     * Tests that only when enough copies of the shard exists we are going to allocate it. This test
     * verifies that with same version (1), and quorum allocation.
     */
    public void testEnoughCopiesFoundForAllocationOnLegacyIndex() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(shardId.getIndexName()).settings(settings(Version.V_2_0_0)).numberOfShards(1).numberOfReplicas(2))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsRecovery(metaData.index(shardId.getIndex()))
                .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();

        RoutingAllocation allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state, null, System.nanoTime(), false);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node1, 1, null, randomBoolean());
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state, null, System.nanoTime(), false);
        changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node2, 1, null, randomBoolean());
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state, null, System.nanoTime(), false);
        changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(0));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), anyOf(equalTo(node2.getId()), equalTo(node1.getId())));
    }

    /**
     * Tests that only when enough copies of the shard exists we are going to allocate it. This test
     * verifies that even with different version, we treat different versions as a copy, and count them.
     */
    public void testEnoughCopiesFoundForAllocationOnLegacyIndexWithDifferentVersion() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(shardId.getIndexName()).settings(settings(Version.V_2_0_0)).numberOfShards(1).numberOfReplicas(2))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsRecovery(metaData.index(shardId.getIndex()))
                .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();

        RoutingAllocation allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state, null, System.nanoTime(), false);
        boolean changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node1, 1, null, randomBoolean());
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state, null, System.nanoTime(), false);
        changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas

        testAllocator.addData(node2, 2, null, randomBoolean());
        allocation = new RoutingAllocation(yesAllocationDeciders(), new RoutingNodes(state, false), state, null, System.nanoTime(), false);
        changed = testAllocator.allocateUnassigned(allocation);
        assertThat(changed, equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(0));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2)); // replicas
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(node2.getId()));
    }

    private RoutingAllocation routingAllocationWithOnePrimaryNoReplicas(AllocationDeciders deciders, boolean asNew, Version version, String... activeAllocationIds) {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(shardId.getIndexName()).settings(settings(version))
                    .numberOfShards(1).numberOfReplicas(0).putActiveAllocationIds(0, Sets.newHashSet(activeAllocationIds)))
            .build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        if (asNew) {
            routingTableBuilder.addAsNew(metaData.index(shardId.getIndex()));
        } else {
            routingTableBuilder.addAsRecovery(metaData.index(shardId.getIndex()));
        }
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(routingTableBuilder.build())
                .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state, null, System.nanoTime(), false);
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

        public TestAllocator addData(DiscoveryNode node, long version, String allocationId, boolean primary) {
            return addData(node, version, allocationId, primary, null);
        }

        public TestAllocator addData(DiscoveryNode node, long version, String allocationId, boolean primary, @Nullable Throwable storeException) {
            if (data == null) {
                data = new HashMap<>();
            }
            data.put(node, new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(node, version, allocationId, primary, storeException));
            return this;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            return new AsyncShardFetch.FetchResult<>(shardId, data, Collections.<String>emptySet(), Collections.<String>emptySet());
        }
    }
}
