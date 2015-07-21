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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class ReplicaShardAllocatorTests extends ElasticsearchAllocationTestCase {

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
     * Verifies that when we are still fetching data in an async manner, the replica shard moves to ignore unassigned.
     */
    @Test
    public void testNoAsyncFetchData() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.clean();
        testAllocator.allocateUnassigned(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that when there is a full match (syncId and files) we allocate it to matching node.
     */
    @Test
    public void testSimpleFullMatchAllocation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testAllocator.addData(node1, true, "MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"))
                .addData(nodeToMatch, false, "MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"));
        testAllocator.allocateUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(nodeToMatch.id()));
    }

    /**
     * Verifies that when there is a sync id match but no files match, we allocate it to matching node.
     */
    @Test
    public void testSyncIdMatch() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testAllocator.addData(node1, true, "MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"))
                .addData(nodeToMatch, false, "MATCH", new StoreFileMetaData("file1", 10, "NO_MATCH_CHECKSUM"));
        testAllocator.allocateUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(nodeToMatch.id()));
    }

    /**
     * Verifies that when there is no sync id match but files match, we allocate it to matching node.
     */
    @Test
    public void testFileChecksumMatch() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testAllocator.addData(node1, true, "MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"))
                .addData(nodeToMatch, false, "NO_MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"));
        testAllocator.allocateUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo(nodeToMatch.id()));
    }

    /**
     * When we can't find primary data, but still find replica data, we go ahead and keep it unassigned
     * to be allocated.
     * TODO: this might be the wrong decision here, and we should restart the fetching process maybe to really find a primary copy?
     */
    @Test
    public void testNoPrimaryData() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.addData(node2, true, "MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"));
        testAllocator.allocateUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that when there is primary data, but no data at all on other nodes, the shard keeps
     * unassigned to be allocated later on.
     */
    @Test
    public void testNoDataForReplicaOnAnyNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.addData(node1, true, "MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"));
        testAllocator.allocateUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that when there is primary data, but no matching data at all on other nodes, the shard keeps
     * unassigned to be allocated later on.
     */
    @Test
    public void testNoMatchingFilesForReplicaOnAnyNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.addData(node1, true, "MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"))
                .addData(node2, false, "NO_MATCH", new StoreFileMetaData("file1", 10, "NO_MATCH_CHECKSUM"));
        testAllocator.allocateUnassigned(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * When there is no decision or throttle decision across all nodes for the shard, make sure the shard
     * moves to the ignore unassigned list.
     */
    @Test
    public void testNoOrThrottleDecidersRemainsInUnassigned() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(randomBoolean() ? noAllocationDeciders() : throttleAllocationDeciders());
        testAllocator.addData(node1, true, "MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"))
                .addData(node2, false, "MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"));
        testAllocator.allocateUnassigned(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests when the node to allocate to due to matching is being throttled, we move the shard to ignored
     * to wait till throttling on it is done.
     */
    @Test
    public void testThrottleWhenAllocatingToMatchingNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(new AllocationDeciders(Settings.EMPTY,
                new AllocationDecider[]{new TestAllocateDecision(Decision.YES), new AllocationDecider(Settings.EMPTY) {
                    @Override
                    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                        if (node.node().equals(node2)) {
                            return Decision.THROTTLE;
                        }
                        return Decision.YES;
                    }
                }}));
        testAllocator.addData(node1, true, "MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"))
                .addData(node2, false, "MATCH", new StoreFileMetaData("file1", 10, "MATCH_CHECKSUM"));
        testAllocator.allocateUnassigned(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    private RoutingAllocation onePrimaryOnNode1And1Replica(AllocationDeciders deciders) {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(shardId.getIndex()).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .add(IndexRoutingTable.builder(shardId.getIndex())
                                .addIndexShard(new IndexShardRoutingTable.Builder(shardId)
                                        .addShard(TestShardRouting.newShardRouting(shardId.getIndex(), shardId.getId(), node1.id(), true, ShardRoutingState.STARTED, 10))
                                        .addShard(ShardRouting.newUnassigned(shardId.getIndex(), shardId.getId(), null, false, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)))
                                        .build())
                )
                .build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().put(node1).put(node2).put(node3)).build();
        return new RoutingAllocation(deciders, state.routingNodes(), state.nodes(), null);
    }

    class TestAllocator extends ReplicaShardAllocator {

        private Map<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> data = null;

        public TestAllocator() {
            super(Settings.EMPTY);
        }

        public void clean() {
            data = null;
        }

        public void cleanWithEmptyData() {
            data = new HashMap<>();
        }

        public TestAllocator addData(DiscoveryNode node, boolean allocated, String syncId, StoreFileMetaData... files) {
            if (data == null) {
                data = new HashMap<>();
            }
            Map<String, StoreFileMetaData> filesAsMap = new HashMap<>();
            for (StoreFileMetaData file : files) {
                filesAsMap.put(file.name(), file);
            }
            Map<String, String> commitData = new HashMap<>();
            if (syncId != null) {
                commitData.put(Engine.SYNC_COMMIT_ID, syncId);
            }
            data.put(node, new TransportNodesListShardStoreMetaData.StoreFilesMetaData(allocated, shardId, new Store.MetadataSnapshot(filesAsMap, commitData, randomInt())));
            return this;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            Map<DiscoveryNode, TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> tData = null;
            if (data != null) {
                tData = new HashMap<>();
                for (Map.Entry<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> entry : data.entrySet()) {
                    tData.put(entry.getKey(), new TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData(entry.getKey(), entry.getValue()));
                }
            }
            return new AsyncShardFetch.FetchResult<>(shardId, tData, Collections.<String>emptySet(), Collections.<String>emptySet());
        }
    }
}
