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
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

import static org.elasticsearch.cluster.routing.allocation.AbstractAllocationDecision.discoveryNodeToXContent;

/**
 * A {@code ClusterAllocationExplanation} is an explanation of why a shard is unassigned,
 * or if it is not unassigned, then which nodes it could possibly be relocated to.
 * It is an immutable class.
 */
public final class ClusterAllocationExplanation implements ToXContent, Writeable {

    private final ShardId shard;
    private final boolean primary;
    private final DiscoveryNode currentNode;
    private final UnassignedInfo unassignedInfo;
    private final ClusterInfo clusterInfo;
    private final ShardAllocationDecision shardAllocationDecision;

    public ClusterAllocationExplanation(ShardId shard, boolean primary, @Nullable DiscoveryNode currentNode,
                                        @Nullable UnassignedInfo unassignedInfo, @Nullable ClusterInfo clusterInfo,
                                        ShardAllocationDecision shardAllocationDecision) {
        this.shard = shard;
        this.primary = primary;
        this.currentNode = currentNode;
        this.unassignedInfo = unassignedInfo;
        this.clusterInfo = clusterInfo;
        this.shardAllocationDecision = shardAllocationDecision;
    }

    public ClusterAllocationExplanation(StreamInput in) throws IOException {
        this.shard = ShardId.readShardId(in);
        this.primary = in.readBoolean();
        this.currentNode = in.readOptionalWriteable(DiscoveryNode::new);
        this.unassignedInfo = in.readOptionalWriteable(UnassignedInfo::new);
        this.clusterInfo = in.readOptionalWriteable(ClusterInfo::new);
        this.shardAllocationDecision = new ShardAllocationDecision(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.getShard().writeTo(out);
        out.writeBoolean(this.isPrimary());
        out.writeOptionalWriteable(currentNode);
        out.writeOptionalWriteable(this.getUnassignedInfo());
        out.writeOptionalWriteable(this.getClusterInfo());
        shardAllocationDecision.writeTo(out);
    }

    /** Return the shard that the explanation is about */
    public ShardId getShard() {
        return this.shard;
    }

    /** Return true if the explained shard is primary, false otherwise */
    public boolean isPrimary() {
        return this.primary;
    }

    /**
     * Returns {@code true} if the explained shard is currently assigned to a node, returns {@code false} otherwise.
     */
    public boolean isAssigned() {
        return currentNode != null;
    }

    /** Return the currently assigned node, or null if the shard is unassigned */
    @Nullable
    public DiscoveryNode getCurrentNode() {
        return currentNode;
    }

    /** Return the unassigned info for the shard or null if the shard is assigned */
    @Nullable
    public UnassignedInfo getUnassignedInfo() {
        return this.unassignedInfo;
    }

    /** Return the cluster disk info for the cluster or null if none available */
    @Nullable
    public ClusterInfo getClusterInfo() {
        return this.clusterInfo;
    }

    /** Return the shard allocation decision for attempting to assign or move the shard. */
    public ShardAllocationDecision getShardAllocationDecision() {
        return shardAllocationDecision;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(); {
            builder.field("index_name", shard.getIndexName());
            builder.field("shard", shard.getId());
            builder.field("primary", primary);
            builder.field("assigned_to_node", this.currentNode != null);
            if (unassignedInfo != null) {
                unassignedInfoToXContent(unassignedInfo, builder, params);
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
            shardAllocationDecision.toXContent(builder, params);
        }
        builder.endObject(); // end wrapping object
        return builder;
    }

    private XContentBuilder unassignedInfoToXContent(UnassignedInfo unassignedInfo, XContentBuilder builder, Params params)
        throws IOException {

        builder.startObject("unassigned_info");
        builder.field("reason", unassignedInfo.getReason());
        builder.field("at", UnassignedInfo.DATE_TIME_FORMATTER.printer().print(unassignedInfo.getUnassignedTimeInMillis()));
        if (unassignedInfo.getNumFailedAllocations() >  0) {
            builder.field("failed_allocation_attempts", unassignedInfo.getNumFailedAllocations());
        }
        if (primary == false) {
            // delayed allocation only makes sense when explaining a replica shard
            builder.field("delayed", unassignedInfo.isDelayed());
        }
        String details = unassignedInfo.getDetails();
        if (details != null) {
            builder.field("details", details);
        }
        builder.field("last_allocation_status", unassignedInfo.getLastAllocationStatus().value());
        builder.endObject();
        return builder;
    }
}
