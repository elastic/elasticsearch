/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Comparator;

import static org.elasticsearch.cluster.routing.allocation.AbstractAllocationDecision.discoveryNodeToXContent;

/**
 * This class represents the shard allocation decision and its explanation for a single node.
 */
public class NodeAllocationResult implements ToXContentObject, Writeable, Comparable<NodeAllocationResult> {

    private static final Comparator<NodeAllocationResult> nodeResultComparator = Comparator.comparing(NodeAllocationResult::getNodeDecision)
        .thenComparingInt(NodeAllocationResult::getWeightRanking)
        .thenComparing(r -> r.getNode().getId());

    private final DiscoveryNode node;
    @Nullable
    private final ShardStoreInfo shardStoreInfo;
    private final AllocationDecision nodeDecision;
    @Nullable
    private final Decision canAllocateDecision;
    private final int weightRanking;

    public NodeAllocationResult(DiscoveryNode node, ShardStoreInfo shardStoreInfo, @Nullable Decision decision) {
        this.node = node;
        this.shardStoreInfo = shardStoreInfo;
        this.canAllocateDecision = decision;
        this.nodeDecision = decision != null ? AllocationDecision.fromDecisionType(canAllocateDecision.type()) : AllocationDecision.NO;
        this.weightRanking = 0;
    }

    public NodeAllocationResult(DiscoveryNode node, AllocationDecision nodeDecision, Decision canAllocate, int weightRanking) {
        this.node = node;
        this.shardStoreInfo = null;
        this.canAllocateDecision = canAllocate;
        this.nodeDecision = nodeDecision;
        this.weightRanking = weightRanking;
    }

    public NodeAllocationResult(DiscoveryNode node, Decision decision, int weightRanking) {
        this.node = node;
        this.shardStoreInfo = null;
        this.canAllocateDecision = decision;
        this.nodeDecision = AllocationDecision.fromDecisionType(decision.type());
        this.weightRanking = weightRanking;
    }

    public NodeAllocationResult(StreamInput in) throws IOException {
        node = new DiscoveryNode(in);
        shardStoreInfo = in.readOptionalWriteable(ShardStoreInfo::new);
        canAllocateDecision = in.readOptionalWriteable(Decision::readFrom);
        nodeDecision = AllocationDecision.readFrom(in);
        weightRanking = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
        out.writeOptionalWriteable(shardStoreInfo);
        out.writeOptionalWriteable(canAllocateDecision);
        nodeDecision.writeTo(out);
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
     * The decision details for allocating to this node.  Returns {@code null} if
     * no allocation decision was taken on the node; in this case, {@link #getNodeDecision()}
     * will return {@link AllocationDecision#NO}.
     */
    @Nullable
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
     * Gets the {@link AllocationDecision} for allocating to this node.
     */
    public AllocationDecision getNodeDecision() {
        return nodeDecision;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            discoveryNodeToXContent(node, false, builder);
            builder.field("node_decision", nodeDecision);
            if (shardStoreInfo != null) {
                shardStoreInfo.toXContent(builder, params);
            }
            if (isWeightRanked()) {
                builder.field("weight_ranking", getWeightRanking());
            }
            if (canAllocateDecision != null && canAllocateDecision.getDecisions().isEmpty() == false) {
                builder.startArray("deciders");
                canAllocateDecision.toXContent(builder, params);
                builder.endArray();
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int compareTo(NodeAllocationResult other) {
        return nodeResultComparator.compare(this, other);
    }

    /** A class that captures metadata about a shard store on a node. */
    public static final class ShardStoreInfo implements ToXContentFragment, Writeable {
        private final boolean inSync;
        @Nullable
        private final String allocationId;
        private final long matchingBytes;
        @Nullable
        private final Exception storeException;

        public ShardStoreInfo(String allocationId, boolean inSync, Exception storeException) {
            this.inSync = inSync;
            this.allocationId = allocationId;
            this.matchingBytes = -1;
            this.storeException = storeException;
        }

        public ShardStoreInfo(long matchingBytes) {
            this.inSync = false;
            this.allocationId = null;
            this.matchingBytes = matchingBytes;
            this.storeException = null;
        }

        public ShardStoreInfo(StreamInput in) throws IOException {
            this.inSync = in.readBoolean();
            this.allocationId = in.readOptionalString();
            this.matchingBytes = in.readLong();
            this.storeException = in.readException();
        }

        /**
         * Returns {@code true} if the shard copy is in-sync and contains the latest data.
         * Returns {@code false} if the shard copy is stale or if the shard copy being examined
         * is for a replica shard allocation.
         */
        public boolean isInSync() {
            return inSync;
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

        /**
         * Gets the store exception when trying to read the store, if there was an error.  If
         * there was no error, returns {@code null}.
         */
        @Nullable
        public Exception getStoreException() {
            return storeException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(inSync);
            out.writeOptionalString(allocationId);
            out.writeLong(matchingBytes);
            out.writeException(storeException);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("store");
            {
                if (matchingBytes < 0) {
                    // dealing with a primary shard
                    if (allocationId == null && storeException == null) {
                        // there was no information we could obtain of any shard data on the node
                        builder.field("found", false);
                    } else {
                        builder.field("in_sync", inSync);
                    }
                }
                if (allocationId != null) {
                    builder.field("allocation_id", allocationId);
                }
                if (matchingBytes >= 0) {
                    builder.humanReadableField("matching_size_in_bytes", "matching_size", ByteSizeValue.ofBytes(matchingBytes));
                }
                if (storeException != null) {
                    builder.startObject("store_exception");
                    ElasticsearchException.generateThrowableXContent(builder, params, storeException);
                    builder.endObject();
                }
            }
            builder.endObject();
            return builder;
        }
    }

}
