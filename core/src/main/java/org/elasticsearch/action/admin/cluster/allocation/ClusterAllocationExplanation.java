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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@code ClusterAllocationExplanation} is an explanation of why a shard may or may not be allocated to nodes. It also includes weights
 * for where the shard is likely to be assigned. It is an immutable class
 */
public final class ClusterAllocationExplanation implements ToXContent, Writeable {

    private final ShardId shard;
    private final boolean primary;
    private final boolean hasPendingAsyncFetch;
    private final String assignedNodeId;
    private final UnassignedInfo unassignedInfo;
    private final long allocationDelayMillis;
    private final long remainingDelayMillis;
    private final Map<DiscoveryNode, NodeExplanation> nodeExplanations;

    public ClusterAllocationExplanation(ShardId shard, boolean primary, @Nullable String assignedNodeId, long allocationDelayMillis,
                                        long remainingDelayMillis, @Nullable UnassignedInfo unassignedInfo, boolean hasPendingAsyncFetch,
                                        Map<DiscoveryNode, NodeExplanation> nodeExplanations) {
        this.shard = shard;
        this.primary = primary;
        this.hasPendingAsyncFetch = hasPendingAsyncFetch;
        this.assignedNodeId = assignedNodeId;
        this.unassignedInfo = unassignedInfo;
        this.allocationDelayMillis = allocationDelayMillis;
        this.remainingDelayMillis = remainingDelayMillis;
        this.nodeExplanations = nodeExplanations;
    }

    public ClusterAllocationExplanation(StreamInput in) throws IOException {
        this.shard = ShardId.readShardId(in);
        this.primary = in.readBoolean();
        this.hasPendingAsyncFetch = in.readBoolean();
        this.assignedNodeId = in.readOptionalString();
        this.unassignedInfo = in.readOptionalWriteable(UnassignedInfo::new);
        this.allocationDelayMillis = in.readVLong();
        this.remainingDelayMillis = in.readVLong();

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
        out.writeBoolean(this.isStillFetchingShardData());
        out.writeOptionalString(this.getAssignedNodeId());
        out.writeOptionalWriteable(this.getUnassignedInfo());
        out.writeVLong(allocationDelayMillis);
        out.writeVLong(remainingDelayMillis);

        out.writeVInt(this.nodeExplanations.size());
        for (NodeExplanation explanation : this.nodeExplanations.values()) {
            explanation.writeTo(out);
        }
    }

    /** Return the shard that the explanation is about */
    public ShardId getShard() {
        return this.shard;
    }

    /** Return true if the explained shard is primary, false otherwise */
    public boolean isPrimary() {
        return this.primary;
    }

    /** Return turn if shard data is still being fetched for the allocation */
    public boolean isStillFetchingShardData() {
        return this.hasPendingAsyncFetch;
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

    /** Return the configured delay before the shard can be allocated in milliseconds */
    public long getAllocationDelayMillis() {
        return this.allocationDelayMillis;
    }

    /** Return the remaining allocation delay for this shard in milliseconds */
    public long getRemainingDelayMillis() {
        return this.remainingDelayMillis;
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
            builder.field("shard_state_fetch_pending", this.hasPendingAsyncFetch);
            // If we have unassigned info, show that
            if (unassignedInfo != null) {
                unassignedInfo.toXContent(builder, params);
                builder.timeValueField("allocation_delay_in_millis", "allocation_delay", TimeValue.timeValueMillis(allocationDelayMillis));
                builder.timeValueField("remaining_delay_in_millis", "remaining_delay", TimeValue.timeValueMillis(remainingDelayMillis));
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
        STALE((byte) 4),
        // It's unknown what the copy of the data is
        UNKNOWN((byte) 5);

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
                case 5: return UNKNOWN;
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
                case 5: return "UNKNOWN";
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
