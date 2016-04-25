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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * Tests for the cluster allocation explanation
 */
public final class ClusterAllocationExplanationTests extends ESTestCase {

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
        Map<DiscoveryNode, Decision> nodeToDecisions = new HashMap<>();
        Map<DiscoveryNode, Float> nodeToWeight = new HashMap<>();
        for (int i = randomIntBetween(2, 5); i > 0; i--) {
            DiscoveryNode dn = new DiscoveryNode("node-" + i, DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
            Decision.Multi d = new Decision.Multi();
            d.add(Decision.single(Decision.Type.NO, "no label", "because I said no"));
            d.add(Decision.single(Decision.Type.YES, "yes label", "yes please"));
            d.add(Decision.single(Decision.Type.THROTTLE, "throttle label", "wait a sec"));
            nodeToDecisions.put(dn, d);
            nodeToWeight.put(dn, randomFloat());
        }

        long remainingDelay = randomIntBetween(0, 500);
        DiscoveryNode nodeWithStore = new DiscoveryNode("node-1", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        IndicesShardStoresResponse.StoreStatus storeStatus = new IndicesShardStoresResponse.StoreStatus(nodeWithStore, 42, "eggplant",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, new ElasticsearchException("stuff's broke, yo"));
        List<IndicesShardStoresResponse.StoreStatus> storeStatusList = Collections.singletonList(storeStatus);
        Set<String> allocationIds = new HashSet<>();
        allocationIds.add("eggplant");
        allocationIds.add("potato");
        ClusterAllocationExplanation cae = new ClusterAllocationExplanation(shard, true, "assignedNode", null,
                nodeToDecisions, nodeToWeight, remainingDelay, storeStatusList, allocationIds);
        BytesStreamOutput out = new BytesStreamOutput();
        cae.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes());
        ClusterAllocationExplanation cae2 = new ClusterAllocationExplanation(in);
        assertEquals(shard, cae2.getShard());
        assertTrue(cae2.isPrimary());
        assertTrue(cae2.isAssigned());
        assertEquals("assignedNode", cae2.getAssignedNodeId());
        assertNull(cae2.getUnassignedInfo());
        assertEquals(remainingDelay, cae2.getRemainingDelayNanos());
        assertEquals(allocationIds, cae2.getActiveAllocationIds());
        for (Map.Entry<DiscoveryNode, ClusterAllocationExplanation.NodeExplanation> entry : cae2.getNodeExplanations().entrySet()) {
            DiscoveryNode node = entry.getKey();
            ClusterAllocationExplanation.NodeExplanation explanation = entry.getValue();
            IndicesShardStoresResponse.StoreStatus status = explanation.getStoreStatus();
            if (status != null) {
                assertEquals(nodeWithStore, node);
                assertEquals(storeStatus.getLegacyVersion(), status.getLegacyVersion());
                assertEquals(storeStatus.getAllocationId(), status.getAllocationId());
                assertEquals(storeStatus.getAllocationStatus(), status.getAllocationStatus());
                assertEquals(ExceptionsHelper.detailedMessage(storeStatus.getStoreException()),
                        ExceptionsHelper.detailedMessage(status.getStoreException()));
            }

            assertEquals(nodeToDecisions.get(node), explanation.getDecision());
            assertEquals(nodeToWeight.get(node), explanation.getWeight());
        }
    }

    public void testStaleShardExplanation() throws Exception {
        ShardId shard = new ShardId("test", "uuid", 0);
        Map<DiscoveryNode, Decision> nodeToDecisions = new HashMap<>();
        Map<DiscoveryNode, Float> nodeToWeight = new HashMap<>();
        DiscoveryNode dn = new DiscoveryNode("node1", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        Decision.Multi d = new Decision.Multi();
        d.add(Decision.single(Decision.Type.NO, "no label", "because I said no"));
        d.add(Decision.single(Decision.Type.YES, "yes label", "yes please"));
        d.add(Decision.single(Decision.Type.THROTTLE, "throttle label", "wait a sec"));
        nodeToDecisions.put(dn, d);
        nodeToWeight.put(dn, 1.5f);

        long remainingDelay = 42;
        DiscoveryNode nodeWithStore = new DiscoveryNode("node1", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        IndicesShardStoresResponse.StoreStatus storeStatus = new IndicesShardStoresResponse.StoreStatus(nodeWithStore, 42, "eggplant",
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null);
        List<IndicesShardStoresResponse.StoreStatus> storeStatusList = Collections.singletonList(storeStatus);
        Set<String> allocationIds = new HashSet<>();
        allocationIds.add("potato");
        ClusterAllocationExplanation cae = new ClusterAllocationExplanation(shard, true, "assignedNode", null,
                nodeToDecisions, nodeToWeight, remainingDelay, storeStatusList, allocationIds);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        cae.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"shard\":{\"index\":\"test\",\"index_uuid\":\"uuid\",\"id\":0,\"primary\":true}," +
                     "\"assigned\":true,\"assigned_node_id\":\"assignedNode\"," +
                     "\"nodes\":{\"node1\":{\"node_name\":\"\",\"node_attributes\":{},\"store\":{\"shard_copy\":\"STALE\"}," +
                     "\"final_decision\":\"NO\",\"final_explanation\":\"the copy of the shard is stale, allocation ids do not match\"" +
                     ",\"weight\":1.5,\"decisions\":[{\"decider\":\"no label\",\"decision\":\"NO\"," +
                     "\"explanation\":\"because I said no\"},{\"decider\":\"yes label\",\"decision\":\"YES\"," +
                     "\"explanation\":\"yes please\"},{\"decider\":\"throttle label\",\"decision\":\"THROTTLE\"," +
                     "\"explanation\":\"wait a sec\"}]}}}",
                builder.string());
    }
}
