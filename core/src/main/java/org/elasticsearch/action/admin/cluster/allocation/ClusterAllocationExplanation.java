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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardStateMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@code ClusterAllocationExplanation} is an explanation of why a shard may or may not be allocated to nodes. It also includes weights
 * for where the shard is likely to be assigned. It is an immutable class
 */
public final class ClusterAllocationExplanation implements ToXContent, Writeable {

    private final ShardId shard;
    private final boolean primary;
    private final String assignedNodeId;
    private final Map<DiscoveryNode, Decision> nodeToDecision;
    private final Map<DiscoveryNode, Float> nodeWeights;
    private final UnassignedInfo unassignedInfo;
    private final long remainingDelayNanos;
    private final List<IndicesShardStoresResponse.StoreStatus> shardStores;
    private final Set<String> activeAllocationIds;

    private Map<DiscoveryNode, IndicesShardStoresResponse.StoreStatus> nodeStoreStatus = null;

    public ClusterAllocationExplanation(ShardId shard, boolean primary, @Nullable String assignedNodeId,
                                        UnassignedInfo unassignedInfo, Map<DiscoveryNode, Decision> nodeToDecision,
                                        Map<DiscoveryNode, Float> nodeWeights, long remainingDelayNanos,
                                        List<IndicesShardStoresResponse.StoreStatus> shardStores, Set<String> activeAllocationIds) {
        this.shard = shard;
        this.primary = primary;
        this.assignedNodeId = assignedNodeId;
        this.unassignedInfo = unassignedInfo;
        this.nodeToDecision = nodeToDecision == null ? Collections.emptyMap() : nodeToDecision;
        this.nodeWeights = nodeWeights == null ? Collections.emptyMap() : nodeWeights;
        this.remainingDelayNanos = remainingDelayNanos;
        this.shardStores = shardStores;
        this.activeAllocationIds = activeAllocationIds;
    }

    public ClusterAllocationExplanation(StreamInput in) throws IOException {
        this.shard = ShardId.readShardId(in);
        this.primary = in.readBoolean();
        this.assignedNodeId = in.readOptionalString();
        this.unassignedInfo = in.readOptionalWriteable(UnassignedInfo::new);

        Map<DiscoveryNode, Decision> ntd = null;
        int size = in.readVInt();
        ntd = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            DiscoveryNode dn = new DiscoveryNode(in);
            Decision decision = Decision.readFrom(in);
            ntd.put(dn, decision);
        }
        this.nodeToDecision = ntd;

        Map<DiscoveryNode, Float> ntw = null;
        size = in.readVInt();
        ntw = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            DiscoveryNode dn = new DiscoveryNode(in);
            float weight = in.readFloat();
            ntw.put(dn, weight);
        }
        this.nodeWeights = ntw;
        remainingDelayNanos = in.readVLong();
        size = in.readVInt();
        List<IndicesShardStoresResponse.StoreStatus> stores = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            stores.add(IndicesShardStoresResponse.StoreStatus.readStoreStatus(in));
        }
        this.shardStores = stores;
        size = in.readVInt();
        Set<String> activeIds = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            activeIds.add(in.readString());
        }
        this.activeAllocationIds = activeIds;
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

    /** Return the remaining allocation delay for this shard in nanoseconds */
    public long getRemainingDelayNanos() {
        return this.remainingDelayNanos;
    }

    /** Return a map of {@code DiscoveryNode} to store status for the explained shard */
    public Map<DiscoveryNode, IndicesShardStoresResponse.StoreStatus> getNodeStoreStatus() {
        if (nodeStoreStatus == null) {
            Map<DiscoveryNode, IndicesShardStoresResponse.StoreStatus> nodeToStatus = new HashMap<>(shardStores.size());
            for (IndicesShardStoresResponse.StoreStatus status : shardStores) {
                nodeToStatus.put(status.getNode(), status);
            }
            nodeStoreStatus = nodeToStatus;
        }
        return nodeStoreStatus;
    }

    /** Return a set of the active allocation ids for this shard */
    public Set<String> getActiveAllocationIds() {
        return this.activeAllocationIds;
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
                long delay = unassignedInfo.getLastComputedLeftDelayNanos();
                builder.field("allocation_delay", TimeValue.timeValueNanos(delay));
                builder.field("allocation_delay_ms", TimeValue.timeValueNanos(delay).millis());
                builder.field("remaining_delay", TimeValue.timeValueNanos(remainingDelayNanos));
                builder.field("remaining_delay_ms", TimeValue.timeValueNanos(remainingDelayNanos).millis());
            }
            builder.startObject("nodes");
            for (Map.Entry<DiscoveryNode, Float> entry : nodeWeights.entrySet()) {
                DiscoveryNode node = entry.getKey();
                builder.startObject(node.getId()); {
                    builder.field("node_name", node.getName());
                    builder.startObject("node_attributes"); {
                        for (Map.Entry<String, String> attrEntry : node.getAttributes().entrySet()) {
                            builder.field(attrEntry.getKey(), attrEntry.getValue());
                        }
                    }
                    builder.endObject(); // end attributes
                    Decision d = nodeToDecision.get(node);
                    String finalDecision = node.getId().equals(assignedNodeId) ? "CURRENTLY_ASSIGNED" : d.type().toString();
                    IndicesShardStoresResponse.StoreStatus storeStatus = getNodeStoreStatus().get(node);
                    builder.startObject("store"); {
                        if (storeStatus != null) {
                            final Throwable storeErr = storeStatus.getStoreException();
                            if (storeErr != null) {
                                builder.field("store_exception", ExceptionsHelper.detailedMessage(storeErr));
                                // Cannot allocate, final decision is "STORE_ERROR"
                                finalDecision = "STORE_ERROR";
                            }
                            if (activeAllocationIds.isEmpty() || activeAllocationIds.contains(storeStatus.getAllocationId())) {
                                // If either we don't have allocation IDs, or they contain the store allocation id, show the allocation
                                // status
                                builder.field("shard_copy", storeStatus.getAllocationStatus());
                            } else{
                                // Otherwise, this is a stale copy of the data (allocation ids don't match)
                                builder.field("shard_copy", "STALE_COPY");
                                // Cannot allocate, final decision is "STORE_STALE"
                                finalDecision = "STORE_STALE";
                            }
                        }
                    }
                    builder.endObject(); // end store
                    builder.field("final_decision", finalDecision);
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
        out.writeVLong(remainingDelayNanos);
        out.writeVInt(shardStores.size());
        for (IndicesShardStoresResponse.StoreStatus status : shardStores) {
            status.writeTo(out);
        }
        out.writeVInt(activeAllocationIds.size());
        for (String id : activeAllocationIds) {
            out.writeString(id);
        }
    }
}
