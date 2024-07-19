/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;

import static org.elasticsearch.cluster.routing.allocation.AbstractAllocationDecision.discoveryNodeToXContent;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.singleChunk;

/**
 * A {@code ClusterAllocationExplanation} is an explanation of why a shard is unassigned,
 * or if it is not unassigned, then which nodes it could possibly be relocated to.
 * It is an immutable class.
 */
public final class ClusterAllocationExplanation implements ChunkedToXContentObject, Writeable {

    static final String NO_SHARD_SPECIFIED_MESSAGE = Strings.format(
        """
            No shard was specified in the explain API request, so this response explains a randomly chosen unassigned shard. There may be \
            other unassigned shards in this cluster which cannot be assigned for different reasons. It may not be possible to assign this \
            shard until one of the other shards is assigned correctly. To explain the allocation of other shards (whether assigned or \
            unassigned) you must specify the target shard in the request to this API. See %s for more information.""",
        ReferenceDocs.ALLOCATION_EXPLAIN_API
    );

    private final boolean specificShard;
    private final ShardRouting shardRouting;
    private final DiscoveryNode currentNode;
    private final DiscoveryNode relocationTargetNode;
    private final ClusterInfo clusterInfo;
    private final ShardAllocationDecision shardAllocationDecision;

    public ClusterAllocationExplanation(
        boolean specificShard,
        ShardRouting shardRouting,
        @Nullable DiscoveryNode currentNode,
        @Nullable DiscoveryNode relocationTargetNode,
        @Nullable ClusterInfo clusterInfo,
        ShardAllocationDecision shardAllocationDecision
    ) {

        this.specificShard = specificShard;
        this.shardRouting = shardRouting;
        this.currentNode = currentNode;
        this.relocationTargetNode = relocationTargetNode;
        this.clusterInfo = clusterInfo;
        this.shardAllocationDecision = shardAllocationDecision;
    }

    public ClusterAllocationExplanation(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_15_0)) {
            this.specificShard = in.readBoolean();
        } else {
            this.specificShard = true; // suppress "this is a random shard" warning in BwC situations
        }
        this.shardRouting = new ShardRouting(in);
        this.currentNode = in.readOptionalWriteable(DiscoveryNode::new);
        this.relocationTargetNode = in.readOptionalWriteable(DiscoveryNode::new);
        this.clusterInfo = in.readOptionalWriteable(ClusterInfo::new);
        this.shardAllocationDecision = new ShardAllocationDecision(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_15_0)) {
            out.writeBoolean(specificShard);
        } // else suppress "this is a random shard" warning in BwC situations
        shardRouting.writeTo(out);
        out.writeOptionalWriteable(currentNode);
        out.writeOptionalWriteable(relocationTargetNode);
        out.writeOptionalWriteable(clusterInfo);
        shardAllocationDecision.writeTo(out);
    }

    public boolean isSpecificShard() {
        return specificShard;
    }

    /**
     * Returns the shard that the explanation is about.
     */
    public ShardId getShard() {
        return shardRouting.shardId();
    }

    /**
     * Returns {@code true} if the explained shard is primary, {@code false} otherwise.
     */
    public boolean isPrimary() {
        return shardRouting.primary();
    }

    /**
     * Returns the current {@link ShardRoutingState} of the shard.
     */
    public ShardRoutingState getShardState() {
        return shardRouting.state();
    }

    /**
     * Returns the currently assigned node, or {@code null} if the shard is unassigned.
     */
    @Nullable
    public DiscoveryNode getCurrentNode() {
        return currentNode;
    }

    /**
     * Returns the relocating target node, or {@code null} if the shard is not in the {@link ShardRoutingState#RELOCATING} state.
     */
    @Nullable
    public DiscoveryNode getRelocationTargetNode() {
        return relocationTargetNode;
    }

    /**
     * Returns the unassigned info for the shard, or {@code null} if the shard is active.
     */
    @Nullable
    public UnassignedInfo getUnassignedInfo() {
        return shardRouting.unassignedInfo();
    }

    /**
     * Returns the cluster disk info for the cluster, or {@code null} if none available.
     */
    @Nullable
    public ClusterInfo getClusterInfo() {
        return this.clusterInfo;
    }

    /**
     * Returns the shard allocation decision for attempting to assign or move the shard.
     */
    public ShardAllocationDecision getShardAllocationDecision() {
        return shardAllocationDecision;
    }

    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(singleChunk((builder, p) -> {
            builder.startObject();

            if (isSpecificShard() == false) {
                builder.field("note", NO_SHARD_SPECIFIED_MESSAGE);
            }
            builder.field("index", shardRouting.getIndexName());
            builder.field("shard", shardRouting.getId());
            builder.field("primary", shardRouting.primary());
            builder.field("current_state", shardRouting.state().toString().toLowerCase(Locale.ROOT));
            if (shardRouting.unassignedInfo() != null) {
                unassignedInfoToXContent(shardRouting.unassignedInfo(), builder);
            }

            if (this.currentNode != null) {
                builder.startObject("current_node");
                discoveryNodeToXContent(this.currentNode, true, builder);
                if (shardAllocationDecision.getMoveDecision().isDecisionTaken()
                    && shardAllocationDecision.getMoveDecision().getCurrentNodeRanking() > 0) {
                    builder.field("weight_ranking", shardAllocationDecision.getMoveDecision().getCurrentNodeRanking());
                }
                builder.endObject();
            }

            return builder;
        }),
            this.clusterInfo != null
                ? Iterators.concat(
                    ChunkedToXContentHelper.startObject("cluster_info"),
                    this.clusterInfo.toXContentChunked(params),
                    ChunkedToXContentHelper.endObject()
                )
                : Collections.emptyIterator(),
            getShardAllocationDecisionChunked(params),
            Iterators.single((builder, p) -> builder.endObject())
        );
    }

    private Iterator<? extends ToXContent> getShardAllocationDecisionChunked(ToXContent.Params params) {
        if (shardAllocationDecision.isDecisionTaken()) {
            return shardAllocationDecision.toXContentChunked(params);
        } else {
            String explanation;
            if (shardRouting.state() == ShardRoutingState.RELOCATING) {
                explanation = Strings.format(
                    "the shard is in the process of relocating from node [%s] to node [%s], wait until relocation has completed",
                    currentNode.getName(),
                    relocationTargetNode.getName()
                );
            } else {
                assert shardRouting.state() == ShardRoutingState.INITIALIZING;
                explanation = Strings.format(
                    "the shard is in the process of initializing on node [%s], wait until initialization has completed",
                    currentNode.getName()
                );
            }
            return Iterators.single((builder, p) -> builder.field("explanation", explanation));
        }
    }

    private static void unassignedInfoToXContent(UnassignedInfo unassignedInfo, XContentBuilder builder) throws IOException {
        builder.startObject("unassigned_info");
        builder.field("reason", unassignedInfo.reason());
        builder.field("at", UnassignedInfo.DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(unassignedInfo.unassignedTimeMillis())));
        if (unassignedInfo.failedAllocations() > 0) {
            builder.field("failed_allocation_attempts", unassignedInfo.failedAllocations());
        }
        String details = unassignedInfo.details();
        if (details != null) {
            builder.field("details", details);
        }
        builder.field("last_allocation_status", AllocationDecision.fromAllocationStatus(unassignedInfo.lastAllocationStatus()));
        builder.endObject();
    }
}
