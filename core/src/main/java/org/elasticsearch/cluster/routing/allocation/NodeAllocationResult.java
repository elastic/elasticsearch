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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * This class represents the shard allocation decision and its explanation for a single node.
 */
public class NodeAllocationResult implements ToXContent, Writeable {

    private final DiscoveryNode node;
    @Nullable
    private final ShardStore shardStore;
    private final Decision canAllocateDecision;
    private final float weight;

    public NodeAllocationResult(DiscoveryNode node, ShardStore shardStore, Decision decision) {
        this.node = node;
        this.shardStore = shardStore;
        this.canAllocateDecision = decision;
        this.weight = Float.POSITIVE_INFINITY;
    }

    public NodeAllocationResult(DiscoveryNode node, Decision decision, float weight) {
        this.node = node;
        this.shardStore = null;
        this.canAllocateDecision = decision;
        this.weight = weight;
    }

    public NodeAllocationResult(StreamInput in) throws IOException {
        node = new DiscoveryNode(in);
        if (in.readBoolean()) {
            shardStore = new ShardStore(in);
        } else {
            shardStore = null;
        }
        canAllocateDecision = Decision.readFrom(in);
        weight = in.readFloat();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
        if (shardStore != null) {
            out.writeBoolean(true);
            shardStore.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        Decision.writeTo(canAllocateDecision, out);
        out.writeFloat(weight);
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
    public ShardStore getShardStore() {
        return shardStore;
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
    public boolean isWeightAssigned() {
        return weight != Float.POSITIVE_INFINITY;
    }

    /**
     * The calculated weight for allocating a shard to the node.  A value of {@link Float#POSITIVE_INFINITY}
     * means the weight was not calculated or factored into the decision.
     */
    public float getWeight() {
        return weight;
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
            builder.field("node_name", node.getName());
            builder.startObject("node_attributes");
            {
                for (Map.Entry<String, String> attrEntry : node.getAttributes().entrySet()) {
                    builder.field(attrEntry.getKey(), attrEntry.getValue());
                }
            }
            builder.endObject(); // end attributes
            builder.field("node_decision", getNodeDecisionType());
            if (shardStore != null) {
                shardStore.toXContent(builder, params);
            }
            if (isWeightAssigned()) {
                builder.field("weight", String.format(Locale.ROOT, "%.4f", getWeight()));
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
    public static final class ShardStore implements ToXContent, Writeable {
        private final StoreStatus storeStatus;
        @Nullable
        private final String allocationId;
        private final long version;
        private final long matchingBytes;
        @Nullable
        private final Exception storeException;

        public ShardStore(StoreStatus storeStatus, String allocationId, long version, Exception storeException) {
            this.storeStatus = storeStatus;
            this.allocationId = allocationId;
            this.version = version;
            this.matchingBytes = -1;
            this.storeException = storeException;
        }

        public ShardStore(StoreStatus storeStatus, long matchingBytes) {
            this.storeStatus = storeStatus;
            this.allocationId = null;
            this.version = -1;
            this.matchingBytes = matchingBytes;
            this.storeException = null;
        }

        public ShardStore(StreamInput in) throws IOException {
            this.storeStatus = StoreStatus.readFrom(in);
            this.allocationId = in.readOptionalString();
            this.version = in.readLong();
            this.matchingBytes = in.readLong();
            if (in.readBoolean()) {
                this.storeException = in.readException();
            } else {
                this.storeException = null;
            }
        }

        /** Gets the store status for the shard copy. */
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
         * Gets the Elasticsearch version number with which the shard store was created,
         * returns -1 if unknown.
         */
        public long getVersion() {
            return version;
        }

        /**
         * Gets the number of matching bytes the shard copy has with the primary shard.
         * Returns -1 if not applicable (this value only applies to assigning replica shards).
         */
        public long getMatchingBytes() {
            return matchingBytes;
        }

        /**
         * Gets the store exception, if one exists.  Otherwise, {@code null} is returned.  A store
         * exception will only exist if {@link ShardStore#getStoreStatus()} returns
         * {@link StoreStatus#CORRUPT} or {@link StoreStatus#IO_ERROR}.
         */
        public Exception getStoreException() {
            return storeException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            storeStatus.writeTo(out);
            out.writeOptionalString(allocationId);
            out.writeLong(version);
            out.writeLong(matchingBytes);
            if (storeException != null) {
                out.writeBoolean(true);
                out.writeException(storeException);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("store");
            {
                builder.field("status", storeStatus.toString());
                if (allocationId != null) {
                    builder.field("allocation_id", allocationId);
                }
                if (version > 0) {
                    builder.field("version", Version.fromId((int) version));
                }
                if (matchingBytes >= 0) {
                    builder.field("matching_bytes", new ByteSizeValue(matchingBytes).toString());
                }
                if (storeException != null) {
                    builder.field("store_exception", ExceptionsHelper.detailedMessage(storeException));
                }
            }
            builder.endObject();
            return builder;
        }
    }

    /** An enum representing the state of the shard store's copy of the data on a node */
    public enum StoreStatus {
        // A copy of the data is available on this node
        AVAILABLE((byte) 0),
        // The copy of the data on the node is corrupt
        CORRUPT((byte) 1),
        // There was an error reading this node's copy of the data
        IO_ERROR((byte) 2),
        // The copy of the data on the node is stale
        STALE((byte) 3),
        // The copy matches sync ids with the primary
        MATCHING_SYNC_ID((byte) 4),
        // It's unknown what the copy of the data is
        UNKNOWN((byte) 5);

        private final byte id;

        StoreStatus(byte id) {
            this.id = id;
        }

        private static StoreStatus fromId(byte id) {
            switch (id) {
                case 0: return AVAILABLE;
                case 1: return CORRUPT;
                case 2: return IO_ERROR;
                case 3: return STALE;
                case 4: return MATCHING_SYNC_ID;
                case 5: return UNKNOWN;
                default:
                    throw new IllegalArgumentException("unknown id for store status: [" + id + "]");
            }
        }

        @Override
        public String toString() {
            switch (id) {
                case 0: return "AVAILABLE";
                case 1: return "CORRUPT";
                case 2: return "IO_ERROR";
                case 3: return "STALE";
                case 4: return "MATCHING_SYNC_ID";
                case 5: return "UNKNOWN";
                default:
                    throw new IllegalArgumentException("unknown id for store copy: [" + id + "]");
            }
        }

        static StoreStatus readFrom(StreamInput in) throws IOException {
            return fromId(in.readByte());
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }
    }
}
