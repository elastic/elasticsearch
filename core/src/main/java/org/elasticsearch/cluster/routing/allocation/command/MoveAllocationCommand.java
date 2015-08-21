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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
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

/**
 * A command that moves a shard from a specific node to another node.<br />
 * <b>Note:</b> The shard needs to be in the state
 * {@link ShardRoutingState#STARTED} in order to be moved.
 */
public class MoveAllocationCommand implements AllocationCommand {

    public static final String NAME = "move";

    public static class Factory implements AllocationCommand.Factory<MoveAllocationCommand> {

        @Override
        public MoveAllocationCommand readFrom(StreamInput in) throws IOException {
            return new MoveAllocationCommand(ShardId.readShardId(in), in.readString(), in.readString());
        }

        @Override
        public void writeTo(MoveAllocationCommand command, StreamOutput out) throws IOException {
            command.shardId().writeTo(out);
            out.writeString(command.fromNode());
            out.writeString(command.toNode());
        }

        @Override
        public MoveAllocationCommand fromXContent(XContentParser parser) throws IOException {
            String index = null;
            int shardId = -1;
            String fromNode = null;
            String toNode = null;

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
                    } else if ("from_node".equals(currentFieldName) || "fromNode".equals(currentFieldName)) {
                        fromNode = parser.text();
                    } else if ("to_node".equals(currentFieldName) || "toNode".equals(currentFieldName)) {
                        toNode = parser.text();
                    } else {
                        throw new ElasticsearchParseException("[{}] command does not support field [{}]", NAME, currentFieldName);
                    }
                } else {
                    throw new ElasticsearchParseException("[{}] command does not support complex json tokens [{}]", NAME, token);
                }
            }
            if (index == null) {
                throw new ElasticsearchParseException("[{}] command missing the index parameter", NAME);
            }
            if (shardId == -1) {
                throw new ElasticsearchParseException("[{}] command missing the shard parameter", NAME);
            }
            if (fromNode == null) {
                throw new ElasticsearchParseException("[{}] command missing the from_node parameter", NAME);
            }
            if (toNode == null) {
                throw new ElasticsearchParseException("[{}] command missing the to_node parameter", NAME);
            }
            return new MoveAllocationCommand(new ShardId(index, shardId), fromNode, toNode);
        }

        @Override
        public void toXContent(MoveAllocationCommand command, XContentBuilder builder, ToXContent.Params params, String objectName) throws IOException {
            if (objectName == null) {
                builder.startObject();
            } else {
                builder.startObject(objectName);
            }
            builder.field("index", command.shardId().index().name());
            builder.field("shard", command.shardId().id());
            builder.field("from_node", command.fromNode());
            builder.field("to_node", command.toNode());
            builder.endObject();
        }
    }

    private final ShardId shardId;
    private final String fromNode;
    private final String toNode;

    public MoveAllocationCommand(ShardId shardId, String fromNode, String toNode) {
        this.shardId = shardId;
        this.fromNode = fromNode;
        this.toNode = toNode;
    }

    @Override
    public String name() {
        return NAME;
    }

    public ShardId shardId() {
        return this.shardId;
    }

    public String fromNode() {
        return this.fromNode;
    }

    public String toNode() {
        return this.toNode;
    }

    @Override
    public RerouteExplanation execute(RoutingAllocation allocation, boolean explain) {
        DiscoveryNode fromDiscoNode = allocation.nodes().resolveNode(fromNode);
        DiscoveryNode toDiscoNode = allocation.nodes().resolveNode(toNode);
        Decision decision = null;

        boolean found = false;
        for (ShardRouting shardRouting : allocation.routingNodes().node(fromDiscoNode.id())) {
            if (!shardRouting.shardId().equals(shardId)) {
                continue;
            }
            found = true;

            // TODO we can possibly support also relocating cases, where we cancel relocation and move...
            if (!shardRouting.started()) {
                if (explain) {
                    return new RerouteExplanation(this, allocation.decision(Decision.NO, "move_allocation_command",
                            "shard " + shardId + " has not been started"));
                }
                throw new IllegalArgumentException("[move_allocation] can't move " + shardId +
                        ", shard is not started (state = " + shardRouting.state() + "]");
            }

            RoutingNode toRoutingNode = allocation.routingNodes().node(toDiscoNode.id());
            decision = allocation.deciders().canAllocate(shardRouting, toRoutingNode, allocation);
            if (decision.type() == Decision.Type.NO) {
                if (explain) {
                    return new RerouteExplanation(this, decision);
                }
                throw new IllegalArgumentException("[move_allocation] can't move " + shardId + ", from " + fromDiscoNode + ", to " + toDiscoNode + ", since its not allowed, reason: " + decision);
            }
            if (decision.type() == Decision.Type.THROTTLE) {
                // its being throttled, maybe have a flag to take it into account and fail? for now, just do it since the "user" wants it...
            }
            allocation.routingNodes().relocate(shardRouting, toRoutingNode.nodeId(), allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE));
        }

        if (!found) {
            if (explain) {
                return new RerouteExplanation(this, allocation.decision(Decision.NO,
                        "move_allocation_command", "shard " + shardId + " not found"));
            }
            throw new IllegalArgumentException("[move_allocation] can't move " + shardId + ", failed to find it on node " + fromDiscoNode);
        }
        return new RerouteExplanation(this, decision);
    }
}
