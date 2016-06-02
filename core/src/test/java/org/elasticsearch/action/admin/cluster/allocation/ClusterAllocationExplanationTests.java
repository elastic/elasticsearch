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

package org.elasticsearch.action.admin.cluster.allocation;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * Tests for the cluster allocation explanation
 */
public final class ClusterAllocationExplanationTests extends ESTestCase {

    private Index i = new Index("foo", "uuid");
    private ShardRouting primaryShard = ShardRouting.newUnassigned(new ShardId(i, 0), null, true,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
    private IndexMetaData indexMetaData = IndexMetaData.builder("foo")
            .settings(Settings.builder()
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetaData.SETTING_INDEX_UUID, "uuid"))
            .putActiveAllocationIds(0, Sets.newHashSet("aid1", "aid2"))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
    private DiscoveryNode node = new DiscoveryNode("node-0", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
    private static Decision.Multi yesDecision = new Decision.Multi();
    private static Decision.Multi noDecision = new Decision.Multi();

    static {
        yesDecision.add(Decision.single(Decision.Type.YES, "yes label", "yes please"));
        noDecision.add(Decision.single(Decision.Type.NO, "no label", "no thanks"));
    }

    private void assertExplanations(NodeExplanation ne, String finalExplanation, ClusterAllocationExplanation.FinalDecision finalDecision,
                                    ClusterAllocationExplanation.StoreCopy storeCopy) {
        assertEquals(finalExplanation, ne.getFinalExplanation());
        assertEquals(finalDecision, ne.getFinalDecision());
        assertEquals(storeCopy, ne.getStoreCopy());
    }

    public void testDecisionAndExplanation() {
        Exception e = new IOException("stuff's broke, yo");
        Exception corruptE = new CorruptIndexException("stuff's corrupt, yo", "");
        Float nodeWeight = randomFloat();
        Set<String> activeAllocationIds = new HashSet<>();
        activeAllocationIds.add("eggplant");
        ShardRouting primaryStartedShard = ShardRouting.newUnassigned(new ShardId(i, 0), null, true,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, "foo"));
        assertTrue(primaryStartedShard.allocatedPostIndexCreate(indexMetaData));
        ShardRouting replicaStartedShard = ShardRouting.newUnassigned(new ShardId(i, 0), null, false,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, "foo"));
        assertTrue(replicaStartedShard.allocatedPostIndexCreate(indexMetaData));

        IndicesShardStoresResponse.StoreStatus storeStatus = new IndicesShardStoresResponse.StoreStatus(node, 42, "eggplant",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, e);
        NodeExplanation ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryShard, indexMetaData, node,
                yesDecision, nodeWeight, storeStatus, "", activeAllocationIds, false);
        assertExplanations(ne, "the copy of the shard cannot be read",
                ClusterAllocationExplanation.FinalDecision.NO, ClusterAllocationExplanation.StoreCopy.IO_ERROR);

        ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryShard, indexMetaData, node, yesDecision, nodeWeight,
                null, "", activeAllocationIds, false);
        assertExplanations(ne, "the shard can be assigned",
                ClusterAllocationExplanation.FinalDecision.YES, ClusterAllocationExplanation.StoreCopy.NONE);

        ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryStartedShard, indexMetaData, node, yesDecision,
                nodeWeight, null, "", activeAllocationIds, false);
        assertExplanations(ne, "there is no copy of the shard available",
                ClusterAllocationExplanation.FinalDecision.NO, ClusterAllocationExplanation.StoreCopy.NONE);

        ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryShard, indexMetaData, node, noDecision, nodeWeight,
                null, "", activeAllocationIds, false);
        assertExplanations(ne, "the shard cannot be assigned because one or more allocation decider returns a 'NO' decision",
                ClusterAllocationExplanation.FinalDecision.NO, ClusterAllocationExplanation.StoreCopy.NONE);

        storeStatus = new IndicesShardStoresResponse.StoreStatus(node, 42, "eggplant",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null);
        ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryShard, indexMetaData, node, noDecision, nodeWeight,
                storeStatus, "", activeAllocationIds, false);
        assertExplanations(ne, "the shard cannot be assigned because one or more allocation decider returns a 'NO' decision",
                ClusterAllocationExplanation.FinalDecision.NO, ClusterAllocationExplanation.StoreCopy.AVAILABLE);

        storeStatus = new IndicesShardStoresResponse.StoreStatus(node, 42, "eggplant",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, corruptE);
        ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryShard, indexMetaData, node, yesDecision, nodeWeight,
                storeStatus, "", activeAllocationIds, false);
        assertExplanations(ne, "the copy of the shard is corrupt",
                ClusterAllocationExplanation.FinalDecision.NO, ClusterAllocationExplanation.StoreCopy.CORRUPT);

        storeStatus = new IndicesShardStoresResponse.StoreStatus(node, 42, "banana",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null);
        ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryShard, indexMetaData, node, yesDecision, nodeWeight,
                storeStatus, "", activeAllocationIds, false);
        assertExplanations(ne, "the shard can be assigned",
                ClusterAllocationExplanation.FinalDecision.YES, ClusterAllocationExplanation.StoreCopy.STALE);

        storeStatus = new IndicesShardStoresResponse.StoreStatus(node, 42, "banana",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null);
        ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryStartedShard, indexMetaData, node, yesDecision,
                nodeWeight, storeStatus, "", activeAllocationIds, false);
        assertExplanations(ne, "the copy of the shard is stale, allocation ids do not match",
                ClusterAllocationExplanation.FinalDecision.NO, ClusterAllocationExplanation.StoreCopy.STALE);

        storeStatus = new IndicesShardStoresResponse.StoreStatus(node, 42, "eggplant",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null);
        ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryShard, indexMetaData, node, yesDecision, nodeWeight,
                storeStatus, "node-0", activeAllocationIds, false);
        assertExplanations(ne, "the shard is already assigned to this node",
                ClusterAllocationExplanation.FinalDecision.ALREADY_ASSIGNED, ClusterAllocationExplanation.StoreCopy.AVAILABLE);

        storeStatus = new IndicesShardStoresResponse.StoreStatus(node, 42, "eggplant",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null);
        ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryShard, indexMetaData, node, yesDecision, nodeWeight,
                storeStatus, "", activeAllocationIds, false);
        assertExplanations(ne, "the shard can be assigned and the node contains a valid copy of the shard data",
                ClusterAllocationExplanation.FinalDecision.YES, ClusterAllocationExplanation.StoreCopy.AVAILABLE);

        storeStatus = new IndicesShardStoresResponse.StoreStatus(node, 42, "eggplant",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null);
        ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryStartedShard, indexMetaData, node, yesDecision,
                nodeWeight, storeStatus, "", activeAllocationIds, true);
        assertExplanations(ne, "the shard's state is still being fetched so it cannot be allocated",
                ClusterAllocationExplanation.FinalDecision.NO, ClusterAllocationExplanation.StoreCopy.AVAILABLE);

        storeStatus = new IndicesShardStoresResponse.StoreStatus(node, 42, "eggplant",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA, null);
        ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(replicaStartedShard, indexMetaData, node, noDecision,
                nodeWeight, storeStatus, "", activeAllocationIds, true);
        assertExplanations(ne, "the shard cannot be assigned because allocation deciders return a NO " +
                        "decision and the shard's state is still being fetched",
                ClusterAllocationExplanation.FinalDecision.NO, ClusterAllocationExplanation.StoreCopy.AVAILABLE);
    }

    public void testDecisionEquality() {
        Decision.Multi d = new Decision.Multi();
        Decision.Multi d2 = new Decision.Multi();
        d.add(Decision.single(Decision.Type.NO, "no label", "because I said no"));
        d.add(Decision.single(Decision.Type.YES, "yes label", "yes please"));
        d.add(Decision.single(Decision.Type.THROTTLE, "throttle label", "wait a sec"));
        d2.add(Decision.single(Decision.Type.NO, "no label", "because I said no"));
        d2.add(Decision.single(Decision.Type.YES, "yes label", "yes please"));
        d2.add(Decision.single(Decision.Type.THROTTLE, "throttle label", "wait a sec"));
        assertEquals(d, d2);
    }

    public void testExplanationSerialization() throws Exception {
        ShardId shard = new ShardId("test", "uuid", 0);
        long allocationDelay = randomIntBetween(0, 500);
        long remainingDelay = randomIntBetween(0, 500);
        Map<DiscoveryNode, NodeExplanation> nodeExplanations = new HashMap<>(1);
        Float nodeWeight = randomFloat();
        Set<String> activeAllocationIds = new HashSet<>();
        activeAllocationIds.add("eggplant");

        IndicesShardStoresResponse.StoreStatus storeStatus = new IndicesShardStoresResponse.StoreStatus(node, 42, "eggplant",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null);
        NodeExplanation ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryShard, indexMetaData, node,
                yesDecision, nodeWeight, storeStatus, "", activeAllocationIds, false);
        nodeExplanations.put(ne.getNode(), ne);
        ClusterAllocationExplanation cae = new ClusterAllocationExplanation(shard, true,
                "assignedNode", allocationDelay, remainingDelay, null, false, nodeExplanations);
        BytesStreamOutput out = new BytesStreamOutput();
        cae.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes());
        ClusterAllocationExplanation cae2 = new ClusterAllocationExplanation(in);
        assertEquals(shard, cae2.getShard());
        assertTrue(cae2.isPrimary());
        assertTrue(cae2.isAssigned());
        assertEquals("assignedNode", cae2.getAssignedNodeId());
        assertNull(cae2.getUnassignedInfo());
        assertEquals(allocationDelay, cae2.getAllocationDelayMillis());
        assertEquals(remainingDelay, cae2.getRemainingDelayMillis());
        for (Map.Entry<DiscoveryNode, NodeExplanation> entry : cae2.getNodeExplanations().entrySet()) {
            DiscoveryNode node = entry.getKey();
            NodeExplanation explanation = entry.getValue();
            IndicesShardStoresResponse.StoreStatus status = explanation.getStoreStatus();
            assertNotNull(explanation.getStoreStatus());
            assertNotNull(explanation.getDecision());
            assertEquals(nodeWeight, explanation.getWeight());
        }
    }

    public void testExplanationToXContent() throws Exception {
        ShardId shardId = new ShardId("foo", "uuid", 0);
        Decision.Multi d = new Decision.Multi();
        d.add(Decision.single(Decision.Type.NO, "no label", "because I said no"));
        d.add(Decision.single(Decision.Type.YES, "yes label", "yes please"));
        d.add(Decision.single(Decision.Type.THROTTLE, "throttle label", "wait a sec"));
        Float nodeWeight = 1.5f;
        Set<String> allocationIds = new HashSet<>();
        allocationIds.add("bar");
        IndicesShardStoresResponse.StoreStatus storeStatus = new IndicesShardStoresResponse.StoreStatus(node, 42, "eggplant",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, new ElasticsearchException("stuff's broke, yo"));
        NodeExplanation ne = TransportClusterAllocationExplainAction.calculateNodeExplanation(primaryShard, indexMetaData, node,
                d, nodeWeight, storeStatus, "node-0", allocationIds, false);
        Map<DiscoveryNode, NodeExplanation> nodeExplanations = new HashMap<>(1);
        nodeExplanations.put(ne.getNode(), ne);
        ClusterAllocationExplanation cae = new ClusterAllocationExplanation(shardId, true,
                "assignedNode", 42, 42, null, false, nodeExplanations);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        cae.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"shard\":{\"index\":\"foo\",\"index_uuid\":\"uuid\",\"id\":0,\"primary\":true},\"assigned\":true," +
                        "\"assigned_node_id\":\"assignedNode\",\"shard_state_fetch_pending\":false,\"nodes\":{\"node-0\":" +
                        "{\"node_name\":\"\",\"node_attributes\":{},\"store\":{\"shard_copy\":\"IO_ERROR\",\"store_except" +
                        "ion\":\"ElasticsearchException[stuff's broke, yo]\"},\"final_decision\":\"ALREADY_ASSIGNED\",\"f" +
                        "inal_explanation\":\"the shard is already assigned to this node\",\"weight\":1.5,\"decisions\":[" +
                        "{\"decider\":\"no label\",\"decision\":\"NO\",\"explanation\":\"because I said no\"},{\"decider" +
                        "\":\"yes label\",\"decision\":\"YES\",\"explanation\":\"yes please\"},{\"decider\":\"throttle la" +
                        "bel\",\"decision\":\"THROTTLE\",\"explanation\":\"wait a sec\"}]}}}",
                builder.string());
    }
}
