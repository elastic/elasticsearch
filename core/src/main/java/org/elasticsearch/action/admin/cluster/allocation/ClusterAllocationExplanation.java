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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@code ClusterAllocationExplanation} is an explanation of why a shard may or may not be allocated to nodes. It also includes weights
 * for where the shard is likely to be assigned. It is an immutable class
 */
public final class ClusterAllocationExplanation implements ToXContent, Writeable<ClusterAllocationExplanation> {

    private final ShardId shard;
    private final boolean primary;
    private final String assignedNodeId;
    private final Map<DiscoveryNode, Decision> nodeToDecision;
    private final Map<DiscoveryNode, Float> nodeWeights;
    private final UnassignedInfo unassignedInfo;

    public ClusterAllocationExplanation(StreamInput in) throws IOException {
        this.shard = ShardId.readShardId(in);
        this.primary = in.readBoolean();
        this.assignedNodeId = in.readOptionalString();
        this.unassignedInfo = in.readOptionalWriteable(UnassignedInfo::new);

        Map<DiscoveryNode, Decision> ntd = null;
        int size = in.readVInt();
        ntd = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            DiscoveryNode dn = DiscoveryNode.readNode(in);
            Decision decision = Decision.readFrom(in);
            ntd.put(dn, decision);
        }
        this.nodeToDecision = ntd;

        Map<DiscoveryNode, Float> ntw = null;
        size = in.readVInt();
        ntw = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            DiscoveryNode dn = DiscoveryNode.readNode(in);
            float weight = in.readFloat();
            ntw.put(dn, weight);
        }
        this.nodeWeights = ntw;
    }

    public ClusterAllocationExplanation(ShardId shard, boolean primary, @Nullable String assignedNodeId,
                                        UnassignedInfo unassignedInfo, Map<DiscoveryNode, Decision> nodeToDecision,
                                        Map<DiscoveryNode, Float> nodeWeights) {
        this.shard = shard;
        this.primary = primary;
        this.assignedNodeId = assignedNodeId;
        this.unassignedInfo = unassignedInfo;
        this.nodeToDecision = nodeToDecision == null ? Collections.emptyMap() : nodeToDecision;
        this.nodeWeights = nodeWeights == null ? Collections.emptyMap() : nodeWeights;
    }

    public ShardId getShard() {
        return this.shard;
    }

    public boolean isPrimary() {
        return this.primary;
    }

    /** Return turn if the shard is assigned to a node */
    public boolean isAssigned() {
        return this.assignedNodeId != null;
    }

    /** Return the assigned node id or null if not assigned */
    @Nullable
    public String getAssignedNodeId() {
        return this.assignedNodeId;
    }

    /** Return the unassigned info for the shard or null if the shard is assigned */
    @Nullable
    public UnassignedInfo getUnassignedInfo() {
        return this.unassignedInfo;
    }

    /** Return a map of node to decision for shard allocation */
    public Map<DiscoveryNode, Decision> getNodeDecisions() {
        return this.nodeToDecision;
    }

    /**
     * Return a map of node to balancer "weight" for allocation. Higher weights mean the balancer wants to allocated the shard to that node
     * more
     */
    public Map<DiscoveryNode, Float> getNodeWeights() {
        return this.nodeWeights;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(); {
            builder.startObject("shard"); {
                builder.field("index", shard.getIndexName());
                builder.field("index_uuid", shard.getIndex().getUUID());
                builder.field("id", shard.getId());
                builder.field("primary", primary);
            }
            builder.endObject(); // end shard
            builder.field("assigned", this.assignedNodeId != null);
            // If assigned, show the node id of the node it's assigned to
            if (assignedNodeId != null) {
                builder.field("assigned_node_id", this.assignedNodeId);
            }
            // If we have unassigned info, show that
            if (unassignedInfo != null) {
                unassignedInfo.toXContent(builder, params);
            }
            builder.startObject("nodes");
            for (Map.Entry<DiscoveryNode, Float> entry : nodeWeights.entrySet()) {
                DiscoveryNode node = entry.getKey();
                builder.startObject(node.getId()); {
                    builder.field("node_name", node.getName());
                    builder.startObject("node_attributes"); {
                        for (ObjectObjectCursor<String, String> attrKV : node.attributes()) {
                            builder.field(attrKV.key, attrKV.value);
                        }
                    }
                    builder.endObject(); // end attributes
                    Decision d = nodeToDecision.get(node);
                    if (node.getId().equals(assignedNodeId)) {
                        builder.field("final_decision", "CURRENTLY_ASSIGNED");
                    } else {
                        builder.field("final_decision", d.type().toString());
                    }
                    builder.field("weight", entry.getValue());
                    d.toXContent(builder, params);
                }
                builder.endObject(); // end node <uuid>
            }
            builder.endObject(); // end nodes
        }
        builder.endObject(); // end wrapping object
        return builder;
    }

    @Override
    public ClusterAllocationExplanation readFrom(StreamInput in) throws IOException {
        return new ClusterAllocationExplanation(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.getShard().writeTo(out);
        out.writeBoolean(this.isPrimary());
        out.writeOptionalString(this.getAssignedNodeId());
        out.writeOptionalWriteable(this.getUnassignedInfo());

        Map<DiscoveryNode, Decision> ntd = this.getNodeDecisions();
        out.writeVInt(ntd.size());
        for (Map.Entry<DiscoveryNode, Decision> entry : ntd.entrySet()) {
            entry.getKey().writeTo(out);
            Decision.writeTo(entry.getValue(), out);
        }
        Map<DiscoveryNode, Float> ntw = this.getNodeWeights();
        out.writeVInt(ntw.size());
        for (Map.Entry<DiscoveryNode, Float> entry : ntw.entrySet()) {
            entry.getKey().writeTo(out);
            out.writeFloat(entry.getValue());
        }
    }
}
