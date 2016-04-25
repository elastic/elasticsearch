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
    private final UnassignedInfo unassignedInfo;
    private final long remainingDelayNanos;
    private final Set<String> activeAllocationIds;
    private final Map<DiscoveryNode, NodeExplanation> nodeExplanations;

    public ClusterAllocationExplanation(ShardId shard, boolean primary, @Nullable String assignedNodeId,
                                        UnassignedInfo unassignedInfo, Map<DiscoveryNode, Decision> nodeToDecision,
                                        Map<DiscoveryNode, Float> nodeToWeight, long remainingDelayNanos,
                                        List<IndicesShardStoresResponse.StoreStatus> shardStores, Set<String> activeAllocationIds) {
        this.shard = shard;
        this.primary = primary;
        this.assignedNodeId = assignedNodeId;
        this.unassignedInfo = unassignedInfo;
        this.remainingDelayNanos = remainingDelayNanos;
        this.activeAllocationIds = activeAllocationIds;
        final Map<DiscoveryNode, Decision> nodeDecisions = nodeToDecision == null ? Collections.emptyMap() : nodeToDecision;
        final Map<DiscoveryNode, Float> nodeWeights = nodeToWeight == null ? Collections.emptyMap() : nodeToWeight;
        assert nodeDecisions.size() == nodeWeights.size() : "decision and weight list should be the same size";
        final Map<DiscoveryNode, IndicesShardStoresResponse.StoreStatus> storeStatuses = calculateStoreStatuses(shardStores);

        this.nodeExplanations = new HashMap<>(nodeDecisions.size());
        for (Map.Entry<DiscoveryNode, Decision> entry : nodeDecisions.entrySet()) {
            final DiscoveryNode node = entry.getKey();
            final Decision decision = entry.getValue();
            final NodeExplanation nodeExplanation = calculateNodeExplanation(node, decision, nodeWeights.get(node),
                    storeStatuses.get(node), assignedNodeId, activeAllocationIds);
            nodeExplanations.put(node, nodeExplanation);
        }
    }

    public ClusterAllocationExplanation(StreamInput in) throws IOException {
        this.shard = ShardId.readShardId(in);
        this.primary = in.readBoolean();
        this.assignedNodeId = in.readOptionalString();
        this.unassignedInfo = in.readOptionalWriteable(UnassignedInfo::new);
        this.remainingDelayNanos = in.readVLong();

        int allocIdSize = in.readVInt();
        Set<String> activeIds = new HashSet<>(allocIdSize);
        for (int i = 0; i < allocIdSize; i++) {
            activeIds.add(in.readString());
        }
        this.activeAllocationIds = activeIds;

        int mapSize = in.readVInt();
        Map<DiscoveryNode, NodeExplanation> nodeToExplanation = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            NodeExplanation nodeExplanation = new NodeExplanation(in);
            nodeToExplanation.put(nodeExplanation.getNode(), nodeExplanation);
        }
        this.nodeExplanations = nodeToExplanation;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.getShard().writeTo(out);
        out.writeBoolean(this.isPrimary());
        out.writeOptionalString(this.getAssignedNodeId());
        out.writeOptionalWriteable(this.getUnassignedInfo());
        out.writeVLong(remainingDelayNanos);
        out.writeVInt(activeAllocationIds.size());
        for (String id : activeAllocationIds) {
            out.writeString(id);
        }

        out.writeVInt(this.nodeExplanations.size());
        for (NodeExplanation explanation : this.nodeExplanations.values()) {
            explanation.writeTo(out);
        }
    }

    private NodeExplanation calculateNodeExplanation(DiscoveryNode node,
                                                     Decision nodeDecision,
                                                     Float nodeWeight,
                                                     IndicesShardStoresResponse.StoreStatus storeStatus,
                                                     String assignedNodeId,
                                                     Set<String> activeAllocationIds) {
        FinalDecision finalDecision;
        StoreCopy storeCopy;
        String finalExplanation;

        if (node.getId().equals(assignedNodeId)) {
            finalDecision = FinalDecision.ALREADY_ASSIGNED;
            finalExplanation = "the shard is already assigned to this node";
        } else if (nodeDecision.type() == Decision.Type.NO) {
            finalDecision = FinalDecision.NO;
            finalExplanation = "the shard cannot be assigned because one or more allocation decider returns a 'NO' decision";
        } else {
            finalDecision = FinalDecision.YES;
            finalExplanation = "the shard can be assigned";
        }

        if (storeStatus != null) {
            final Throwable storeErr = storeStatus.getStoreException();
            if (storeErr != null) {
                finalDecision = FinalDecision.NO;
                if (ExceptionsHelper.unwrapCause(storeErr) instanceof IOException) {
                    storeCopy = StoreCopy.IO_ERROR;
                    finalExplanation = "there was an IO error reading from data in the shard store";
                } else {
                    storeCopy = StoreCopy.CORRUPT;
                    finalExplanation = "the copy of data in the shard store is corrupt";
                }
            } else if (activeAllocationIds.isEmpty() || activeAllocationIds.contains(storeStatus.getAllocationId())) {
                // If either we don't have allocation IDs, or they contain the store allocation id, show the allocation
                // status
                storeCopy = StoreCopy.AVAILABLE;
                if (finalDecision != FinalDecision.NO) {
                    finalExplanation = "the shard can be assigned and the node contains a valid copy of the shard data";
                }
            } else {
                // Otherwise, this is a stale copy of the data (allocation ids don't match)
                storeCopy = StoreCopy.STALE;
                finalExplanation = "the copy of the shard is stale, allocation ids do not match";
                finalDecision = FinalDecision.NO;
            }
        } else {
            // No copies of the data, so deciders are what influence the decision and explanation
            storeCopy = StoreCopy.NONE;
        }
        return new NodeExplanation(node, nodeDecision, nodeWeight, storeStatus, finalDecision, finalExplanation, storeCopy);
    }

    private static Map<DiscoveryNode, IndicesShardStoresResponse.StoreStatus> calculateStoreStatuses(
            List<IndicesShardStoresResponse.StoreStatus> shardStores) {
        Map<DiscoveryNode, IndicesShardStoresResponse.StoreStatus> nodeToStatus = new HashMap<>(shardStores.size());
        for (IndicesShardStoresResponse.StoreStatus status : shardStores) {
            nodeToStatus.put(status.getNode(), status);
        }
        return nodeToStatus;
    }

    /** Return the shard that the explanation is about */
    public ShardId getShard() {
        return this.shard;
    }

    /** Return true if the explained shard is primary, false otherwise */
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

    /** Return the remaining allocation delay for this shard in nanoseconds */
    public long getRemainingDelayNanos() {
        return this.remainingDelayNanos;
    }

    /** Return a set of the active allocation ids for this shard */
    public Set<String> getActiveAllocationIds() {
        return this.activeAllocationIds;
    }

    /** Return a map of node to the explanation for that node */
    public Map<DiscoveryNode, NodeExplanation> getNodeExplanations() {
        return this.nodeExplanations;
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
            for (NodeExplanation explanation : nodeExplanations.values()) {
                explanation.toXContent(builder, params);
            }
            builder.endObject(); // end nodes
        }
        builder.endObject(); // end wrapping object
        return builder;
    }

    /** The cluster allocation explanation for a single node */
    public class NodeExplanation implements Writeable, ToXContent {
        private final DiscoveryNode node;
        private final Decision nodeDecision;
        private final Float nodeWeight;
        private final IndicesShardStoresResponse.StoreStatus storeStatus;
        private final FinalDecision finalDecision;
        private final String finalExplanation;
        private final StoreCopy storeCopy;

        public NodeExplanation(final DiscoveryNode node, final Decision nodeDecision, final Float nodeWeight,
                               final @Nullable IndicesShardStoresResponse.StoreStatus storeStatus,
                               final FinalDecision finalDecision, final String finalExplanation, final StoreCopy storeCopy) {
            this.node = node;
            this.nodeDecision = nodeDecision;
            this.nodeWeight = nodeWeight;
            this.storeStatus = storeStatus;
            this.finalDecision = finalDecision;
            this.finalExplanation = finalExplanation;
            this.storeCopy = storeCopy;
        }

        public NodeExplanation(StreamInput in) throws IOException {
            this.node = new DiscoveryNode(in);
            this.nodeDecision = Decision.readFrom(in);
            this.nodeWeight = in.readFloat();
            if (in.readBoolean()) {
                this.storeStatus = IndicesShardStoresResponse.StoreStatus.readStoreStatus(in);
            } else {
                this.storeStatus = null;
            }
            this.finalDecision = FinalDecision.readFrom(in);
            this.finalExplanation = in.readString();
            this.storeCopy = StoreCopy.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            node.writeTo(out);
            Decision.writeTo(nodeDecision, out);
            out.writeFloat(nodeWeight);
            if (storeStatus == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                storeStatus.writeTo(out);
            }
            finalDecision.writeTo(out);
            out.writeString(finalExplanation);
            storeCopy.writeTo(out);
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(node.getId()); {
                builder.field("node_name", node.getName());
                builder.startObject("node_attributes"); {
                    for (Map.Entry<String, String> attrEntry : node.getAttributes().entrySet()) {
                        builder.field(attrEntry.getKey(), attrEntry.getValue());
                    }
                }
                builder.endObject(); // end attributes
                builder.startObject("store"); {
                    builder.field("shard_copy", storeCopy.toString());
                    if (storeStatus != null) {
                        final Throwable storeErr = storeStatus.getStoreException();
                        if (storeErr != null) {
                            builder.field("store_exception", ExceptionsHelper.detailedMessage(storeErr));
                        }
                    }
                }
                builder.endObject(); // end store
                builder.field("final_decision", finalDecision.toString());
                builder.field("final_explanation", finalExplanation.toString());
                builder.field("weight", nodeWeight);
                nodeDecision.toXContent(builder, params);
            }
            builder.endObject(); // end node <uuid>
            return builder;
        }

        public DiscoveryNode getNode() {
            return this.node;
        }

        public Decision getDecision() {
            return this.nodeDecision;
        }

        public Float getWeight() {
            return this.nodeWeight;
        }

        @Nullable
        public IndicesShardStoresResponse.StoreStatus getStoreStatus() {
            return this.storeStatus;
        }

        public FinalDecision getFinalDecision() {
            return this.finalDecision;
        }

        public String getFinalExplanation() {
            return this.finalExplanation;
        }

        public StoreCopy getStoreCopy() {
            return this.storeCopy;
        }
    }

    /** An Enum representing the final decision for a shard allocation on a node */
    public enum FinalDecision {
        // Yes, the shard can be assigned
        YES((byte) 0),
        // No, the shard cannot be assigned
        NO((byte) 1),
        // The shard is already assigned to this node
        ALREADY_ASSIGNED((byte) 2);

        private final byte id;

        FinalDecision (byte id) {
            this.id = id;
        }

        private static FinalDecision fromId(byte id) {
            switch (id) {
                case 0: return YES;
                case 1: return NO;
                case 2: return ALREADY_ASSIGNED;
                default:
                    throw new IllegalArgumentException("unknown id for final decision: [" + id + "]");
            }
        }

        @Override
        public String toString() {
            switch (id) {
                case 0: return "YES";
                case 1: return "NO";
                case 2: return "ALREADY_ASSIGNED";
                default:
                    throw new IllegalArgumentException("unknown id for final decision: [" + id + "]");
            }
        }

        static FinalDecision readFrom(StreamInput in) throws IOException {
            return fromId(in.readByte());
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }
    }

    /** An Enum representing the state of the shard store's copy of the data on a node */
    public enum StoreCopy {
        // No data for this shard is on the node
        NONE((byte) 0),
        // A copy of the data is available on this node
        AVAILABLE((byte) 1),
        // The copy of the data on the node is corrupt
        CORRUPT((byte) 2),
        // There was an error reading this node's copy of the data
        IO_ERROR((byte) 3),
        // The copy of the data on the node is stale
        STALE((byte) 4);

        private final byte id;

        StoreCopy (byte id) {
            this.id = id;
        }

        private static StoreCopy fromId(byte id) {
            switch (id) {
                case 0: return NONE;
                case 1: return AVAILABLE;
                case 2: return CORRUPT;
                case 3: return IO_ERROR;
                case 4: return STALE;
                default:
                    throw new IllegalArgumentException("unknown id for store copy: [" + id + "]");
            }
        }

        @Override
        public String toString() {
            switch (id) {
                case 0: return "NONE";
                case 1: return "AVAILABLE";
                case 2: return "CORRUPT";
                case 3: return "IO_ERROR";
                case 4: return "STALE";
                default:
                    throw new IllegalArgumentException("unknown id for store copy: [" + id + "]");
            }
        }

        static StoreCopy readFrom(StreamInput in) throws IOException {
            return fromId(in.readByte());
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }
    }
}
