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

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;

import static org.elasticsearch.cluster.routing.allocation.AbstractAllocationDecision.discoveryNodeToXContent;

/**
 * A {@code ClusterAllocationExplanation} is an explanation of why a shard is unassigned,
 * or if it is not unassigned, then which nodes it could possibly be relocated to.
 * It is an immutable class.
 */
public final class ClusterAllocationExplanation implements ToXContentObject, Writeable {

    private final ShardRouting shardRouting;
    private final DiscoveryNode currentNode;
    private final DiscoveryNode relocationTargetNode;
    private final ClusterInfo clusterInfo;
    private final ShardAllocationDecision shardAllocationDecision;

    public ClusterAllocationExplanation(ShardRouting shardRouting, @Nullable DiscoveryNode currentNode,
                                        @Nullable DiscoveryNode relocationTargetNode, @Nullable ClusterInfo clusterInfo,
                                        ShardAllocationDecision shardAllocationDecision) {
        this.shardRouting = shardRouting;
        this.currentNode = currentNode;
        this.relocationTargetNode = relocationTargetNode;
        this.clusterInfo = clusterInfo;
        this.shardAllocationDecision = shardAllocationDecision;
    }

    public ClusterAllocationExplanation(StreamInput in) throws IOException {
        this.shardRouting = new ShardRouting(in);
        this.currentNode = in.readOptionalWriteable(DiscoveryNode::new);
        this.relocationTargetNode = in.readOptionalWriteable(DiscoveryNode::new);
        this.clusterInfo = in.readOptionalWriteable(ClusterInfo::new);
        this.shardAllocationDecision = new ShardAllocationDecision(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        out.writeOptionalWriteable(currentNode);
        out.writeOptionalWriteable(relocationTargetNode);
        out.writeOptionalWriteable(clusterInfo);
        shardAllocationDecision.writeTo(out);
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

    /** \
     * Returns the shard allocation decision for attempting to assign or move the shard.
     */
    public ShardAllocationDecision getShardAllocationDecision() {
        return shardAllocationDecision;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(); {
            builder.field("index", shardRouting.getIndexName());
            builder.field("shard", shardRouting.getId());
            builder.field("primary", shardRouting.primary());
            builder.field("current_state", shardRouting.state().toString().toLowerCase(Locale.ROOT));
            if (shardRouting.unassignedInfo() != null) {
                unassignedInfoToXContent(shardRouting.unassignedInfo(), builder);
            }
            if (currentNode != null) {
                builder.startObject("current_node");
                {
                    discoveryNodeToXContent(currentNode, true, builder);
                    if (shardAllocationDecision.getMoveDecision().isDecisionTaken()
                            && shardAllocationDecision.getMoveDecision().getCurrentNodeRanking() > 0) {
                        builder.field("weight_ranking", shardAllocationDecision.getMoveDecision().getCurrentNodeRanking());
                    }
                }
                builder.endObject();
            }
            if (this.clusterInfo != null) {
                builder.startObject("cluster_info"); {
                    this.clusterInfo.toXContent(builder, params);
                }
                builder.endObject(); // end "cluster_info"
            }
            if (shardAllocationDecision.isDecisionTaken()) {
                shardAllocationDecision.toXContent(builder, params);
            } else {
                String explanation;
                if (shardRouting.state() == ShardRoutingState.RELOCATING) {
                    explanation = "the shard is in the process of relocating from node [" + currentNode.getName() + "] " +
                                  "to node [" + relocationTargetNode.getName() + "], wait until relocation has completed";
                } else {
                    assert shardRouting.state() == ShardRoutingState.INITIALIZING;
                    explanation = "the shard is in the process of initializing on node [" + currentNode.getName() + "], " +
                                  "wait until initialization has completed";
                }
                builder.field("explanation", explanation);
            }
        }
        builder.endObject(); // end wrapping object
        return builder;
    }

    private XContentBuilder unassignedInfoToXContent(UnassignedInfo unassignedInfo, XContentBuilder builder)
        throws IOException {

        builder.startObject("unassigned_info");
        builder.field("reason", unassignedInfo.getReason());
        builder.field("at",
            UnassignedInfo.DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(unassignedInfo.getUnassignedTimeInMillis())));
        if (unassignedInfo.getNumFailedAllocations() >  0) {
            builder.field("failed_allocation_attempts", unassignedInfo.getNumFailedAllocations());
        }
        String details = unassignedInfo.getDetails();
        if (details != null) {
            builder.field("details", details);
        }
        builder.field("last_allocation_status", AllocationDecision.fromAllocationStatus(unassignedInfo.getLastAllocationStatus()));
        builder.endObject();
        return builder;
    }
}
