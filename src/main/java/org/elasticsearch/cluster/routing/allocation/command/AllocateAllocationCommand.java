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

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Iterator;

/**
 * Allocates an unassigned shard to a specific node. Note, primary allocation will "force"
 * allocation which might mean one will loose data if using local gateway..., use with care
 * with the <tt>allowPrimary</tt> flag.
 */
public class AllocateAllocationCommand implements AllocationCommand {

    public static final String NAME = "allocate";

    public static class Factory implements AllocationCommand.Factory<AllocateAllocationCommand> {

        @Override
        public AllocateAllocationCommand readFrom(StreamInput in) throws IOException {
            return new AllocateAllocationCommand(ShardId.readShardId(in), in.readString(), in.readBoolean());
        }

        @Override
        public void writeTo(AllocateAllocationCommand command, StreamOutput out) throws IOException {
            command.shardId().writeTo(out);
            out.writeString(command.node());
            out.writeBoolean(command.allowPrimary());
        }

        @Override
        public AllocateAllocationCommand fromXContent(XContentParser parser) throws IOException {
            String index = null;
            int shardId = -1;
            String nodeId = null;
            boolean allowPrimary = false;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("index".equals(currentFieldName)) {
                        index = parser.text();
                    } else if ("shard".equals(currentFieldName)) {
                        shardId = parser.intValue();
                    } else if ("node".equals(currentFieldName)) {
                        nodeId = parser.text();
                    } else if ("allow_primary".equals(currentFieldName) || "allowPrimary".equals(currentFieldName)) {
                        allowPrimary = parser.booleanValue();
                    } else {
                        throw new ElasticsearchParseException("[allocate] command does not support field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ElasticsearchParseException("[allocate] command does not support complex json tokens [" + token + "]");
                }
            }
            if (index == null) {
                throw new ElasticsearchParseException("[allocate] command missing the index parameter");
            }
            if (shardId == -1) {
                throw new ElasticsearchParseException("[allocate] command missing the shard parameter");
            }
            if (nodeId == null) {
                throw new ElasticsearchParseException("[allocate] command missing the node parameter");
            }
            return new AllocateAllocationCommand(new ShardId(index, shardId), nodeId, allowPrimary);
        }

        @Override
        public void toXContent(AllocateAllocationCommand command, XContentBuilder builder, ToXContent.Params params, String objectName) throws IOException {
            if (objectName == null) {
                builder.startObject();
            } else {
                builder.startObject(objectName);
            }
            builder.field("index", command.shardId().index().name());
            builder.field("shard", command.shardId().id());
            builder.field("node", command.node());
            builder.field("allow_primary", command.allowPrimary());
            builder.endObject();
        }
    }

    private final ShardId shardId;
    private final String node;
    private final boolean allowPrimary;

    /**
     * Create a new {@link AllocateAllocationCommand}
     *
     * @param shardId      {@link ShardId} of the shrad to assign
     * @param node         Node to assign the shard to
     * @param allowPrimary should the node be allow to allocate the shard as primary
     */
    public AllocateAllocationCommand(ShardId shardId, String node, boolean allowPrimary) {
        this.shardId = shardId;
        this.node = node;
        this.allowPrimary = allowPrimary;
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * Get the shards id
     *
     * @return id of the shard
     */
    public ShardId shardId() {
        return this.shardId;
    }

    /**
     * Get the id of the Node
     *
     * @return id of the Node
     */
    public String node() {
        return this.node;
    }

    /**
     * Determine if primary allocation is allowed
     *
     * @return <code>true</code> if primary allocation is allowed. Otherwise <code>false</code>
     */
    public boolean allowPrimary() {
        return this.allowPrimary;
    }

    @Override
    public RerouteExplanation execute(RoutingAllocation allocation, boolean explain) throws ElasticsearchException {
        DiscoveryNode discoNode = allocation.nodes().resolveNode(node);

        MutableShardRouting shardRouting = null;
        for (MutableShardRouting routing : allocation.routingNodes().unassigned()) {
            if (routing.shardId().equals(shardId)) {
                // prefer primaries first to allocate
                if (shardRouting == null || routing.primary()) {
                    shardRouting = routing;
                }
            }
        }

        if (shardRouting == null) {
            if (explain) {
                return new RerouteExplanation(this, allocation.decision(Decision.NO, "allocate_allocation_command",
                        "failed to find " + shardId + " on the list of unassigned shards"));
            }
            throw new ElasticsearchIllegalArgumentException("[allocate] failed to find " + shardId + " on the list of unassigned shards");
        }

        if (shardRouting.primary() && !allowPrimary) {
            if (explain) {
                return new RerouteExplanation(this, allocation.decision(Decision.NO, "allocate_allocation_command",
                        "trying to allocate a primary shard " + shardId + ", which is disabled"));
            }
            throw new ElasticsearchIllegalArgumentException("[allocate] trying to allocate a primary shard " + shardId + ", which is disabled");
        }

        RoutingNode routingNode = allocation.routingNodes().node(discoNode.id());
        if (routingNode == null) {
            if (!discoNode.dataNode()) {
                if (explain) {
                    return new RerouteExplanation(this, allocation.decision(Decision.NO, "allocate_allocation_command",
                            "Allocation can only be done on data nodes, not [" + node + "]"));
                }
                throw new ElasticsearchIllegalArgumentException("Allocation can only be done on data nodes, not [" + node + "]");
            } else {
                if (explain) {
                    return new RerouteExplanation(this, allocation.decision(Decision.NO, "allocate_allocation_command",
                            "Could not find [" + node + "] among the routing nodes"));
                }
                throw new ElasticsearchIllegalStateException("Could not find [" + node + "] among the routing nodes");
            }
        }

        Decision decision = allocation.deciders().canAllocate(shardRouting, routingNode, allocation);
        if (decision.type() == Decision.Type.NO) {
            if (explain) {
                return new RerouteExplanation(this, decision);
            }
            throw new ElasticsearchIllegalArgumentException("[allocate] allocation of " + shardId + " on node " + discoNode + " is not allowed, reason: " + decision);
        }
        // go over and remove it from the unassigned
        for (Iterator<MutableShardRouting> it = allocation.routingNodes().unassigned().iterator(); it.hasNext(); ) {
            if (it.next() != shardRouting) {
                continue;
            }
            it.remove();
            allocation.routingNodes().assign(shardRouting, routingNode.nodeId());
            if (shardRouting.primary()) {
                // we need to clear the post allocation flag, since its an explicit allocation of the primary shard
                // and we want to force allocate it (and create a new index for it)
                allocation.routingNodes().addClearPostAllocationFlag(shardRouting.shardId());
            }
            break;
        }
        return new RerouteExplanation(this, decision);
    }
}
