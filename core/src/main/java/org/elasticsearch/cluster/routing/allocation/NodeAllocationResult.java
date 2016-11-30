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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision.discoveryNodeToXContent;

/**
 * This class represents the shard allocation decision and its explanation for a single node.
 */
public class NodeAllocationResult implements ToXContent, Writeable {

    private final DiscoveryNode node;
    @Nullable
    private final ShardStoreInfo shardStoreInfo;
    private final Decision canAllocateDecision;
    private final int weightRanking;

    public NodeAllocationResult(DiscoveryNode node, ShardStoreInfo shardStoreInfo, Decision decision) {
        this.node = node;
        this.shardStoreInfo = shardStoreInfo;
        this.canAllocateDecision = decision;
        this.weightRanking = 0;
    }

    public NodeAllocationResult(DiscoveryNode node, Decision decision, int weightRanking) {
        this.node = node;
        this.shardStoreInfo = null;
        this.canAllocateDecision = decision;
        this.weightRanking = weightRanking;
    }

    public NodeAllocationResult(StreamInput in) throws IOException {
        node = new DiscoveryNode(in);
        shardStoreInfo = in.readOptionalWriteable(ShardStoreInfo::new);
        canAllocateDecision = Decision.readFrom(in);
        weightRanking = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
        out.writeOptionalWriteable(shardStoreInfo);
        canAllocateDecision.writeTo(out);
        out.writeVInt(weightRanking);
    }

    /**
     * Get the node that this decision is for.
     */
    public DiscoveryNode getNode() {
        return node;
    }

    /**
     * Get the shard store information for the node, if it exists.
     */
    @Nullable
    public ShardStoreInfo getShardStoreInfo() {
        return shardStoreInfo;
    }

    /**
     * The decision for allocating to the node.
     */
    public Decision getCanAllocateDecision() {
        return canAllocateDecision;
    }

    /**
     * Is the weight assigned for the node?
     */
    public boolean isWeightRanked() {
        return weightRanking > 0;
    }

    /**
     * The weight ranking for allocating a shard to the node.  Each node will have
     * a unique weight ranking that is relative to the other nodes against which the
     * deciders ran.  For example, suppose there are 3 nodes which the allocation deciders
     * decided upon: node1, node2, and node3.  If node2 had the best weight for holding the
     * shard, followed by node3, followed by node1, then node2's weight will be 1, node3's
     * weight will be 2, and node1's weight will be 1.  A value of 0 means the weight was
     * not calculated or factored into the decision.
     */
    public int getWeightRanking() {
        return weightRanking;
    }

    /**
     * Gets the decision type for allocating to this node.
     */
    public Decision.Type getNodeDecisionType() {
        return canAllocateDecision.type();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(node.getId());
        {
            builder.startObject("node_info");
            {
                discoveryNodeToXContent(builder, params, node);
            }
            builder.endObject(); // end node_info
            builder.field("node_decision", getNodeDecisionType());
            if (shardStoreInfo != null) {
                shardStoreInfo.toXContent(builder, params);
            }
            if (isWeightRanked()) {
                builder.field("weight_ranking", getWeightRanking());
            }
            innerToXContent(builder, params);
            getCanAllocateDecision().toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Sub-classes should override this to add any extra x-content.
     */
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
    }

    /** A class that captures metadata about a shard store on a node. */
    public static final class ShardStoreInfo implements ToXContent, Writeable {
        private final StoreStatus storeStatus;
        @Nullable
        private final String allocationId;
        private final long matchingBytes;
        @Nullable
        private final Exception storeException;

        public ShardStoreInfo(StoreStatus storeStatus, String allocationId, Exception storeException) {
            this.storeStatus = storeStatus;
            this.allocationId = allocationId;
            this.matchingBytes = -1;
            this.storeException = storeException;
        }

        public ShardStoreInfo(StoreStatus storeStatus, long matchingBytes) {
            this.storeStatus = storeStatus;
            this.allocationId = null;
            this.matchingBytes = matchingBytes;
            this.storeException = null;
        }

        public ShardStoreInfo(StreamInput in) throws IOException {
            this.storeStatus = StoreStatus.readFrom(in);
            this.allocationId = in.readOptionalString();
            this.matchingBytes = in.readLong();
            this.storeException = in.readException();
        }

        /**
         * Gets the store status for the shard copy.
         */
        public StoreStatus getStoreStatus() {
            return storeStatus;
        }

        /**
         * Gets the allocation id for the shard copy, if it exists.
         */
        @Nullable
        public String getAllocationId() {
            return allocationId;
        }

        /**
         * Gets the number of matching bytes the shard copy has with the primary shard.
         * Returns -1 if not applicable (this value only applies to assigning replica shards).
         */
        public long getMatchingBytes() {
            return matchingBytes;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            storeStatus.writeTo(out);
            out.writeOptionalString(allocationId);
            out.writeLong(matchingBytes);
            out.writeException(storeException);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("store");
            {
                builder.field("status", storeStatus.toString());
                if (allocationId != null) {
                    builder.field("allocation_id", allocationId);
                }
                if (matchingBytes >= 0) {
                    builder.byteSizeField("matching_size_in_bytes", "matching_size", matchingBytes);
                }
                if (storeException != null) {
                    builder.startObject("store_exception");
                    ElasticsearchException.toXContent(builder, params, storeException);
                    builder.endObject();
                }
            }
            builder.endObject();
            return builder;
        }
    }

    /** An enum representing the state of the shard store's copy of the data on a node */
    public enum StoreStatus implements Writeable {
        // A current and valid copy of the data is available on this node
        IN_SYNC((byte) 0),
        // The copy of the data on the node is stale
        STALE((byte) 1),
        // The copy matches sync ids with the primary
        MATCHING_SYNC_ID((byte) 2),
        // It's unknown what the copy of the data is
        UNKNOWN((byte) 3);

        private final byte id;

        StoreStatus(byte id) {
            this.id = id;
        }

        private static StoreStatus fromId(byte id) {
            switch (id) {
                case 0: return IN_SYNC;
                case 1: return STALE;
                case 2: return MATCHING_SYNC_ID;
                case 3: return UNKNOWN;
                default:
                    throw new IllegalArgumentException("unknown id for store status: [" + id + "]");
            }
        }

        @Override
        public String toString() {
            switch (id) {
                case 0: return "IN_SYNC";
                case 1: return "STALE";
                case 2: return "MATCHING_SYNC_ID";
                case 3: return "UNKNOWN";
                default:
                    throw new IllegalArgumentException("unknown id for store status: [" + id + "]");
            }
        }

        static StoreStatus readFrom(StreamInput in) throws IOException {
            return fromId(in.readByte());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }
    }
}
