/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A command that moves a shard from a specific node to another node.<br>
 * <b>Note:</b> The shard needs to be in the state
 * {@link ShardRoutingState#STARTED} in order to be moved.
 */
public class MoveAllocationCommand implements AllocationCommand {

    public static final String NAME = "move";
    public static final ParseField COMMAND_NAME_FIELD = new ParseField(NAME);

    private final String index;
    private final int shardId;
    private final String fromNode;
    private final String toNode;

    public MoveAllocationCommand(String index, int shardId, String fromNode, String toNode) {
        this.index = index;
        this.shardId = shardId;
        this.fromNode = fromNode;
        this.toNode = toNode;
    }

    /**
     * Read from a stream.
     */
    public MoveAllocationCommand(StreamInput in) throws IOException {
        index = in.readString();
        shardId = in.readVInt();
        fromNode = in.readString();
        toNode = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeString(fromNode);
        out.writeString(toNode);
    }

    @Override
    public String name() {
        return NAME;
    }

    public String index() {return index; }

    public int shardId() {
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
        RoutingNode fromRoutingNode = allocation.routingNodes().node(fromDiscoNode.getId());
        if (fromRoutingNode == null && fromDiscoNode.canContainData() == false) {
            throw new IllegalArgumentException("[move_allocation] can't move [" + index + "][" + shardId + "] from "
                + fromDiscoNode + " to " + toDiscoNode + ": source [" +  fromDiscoNode.getName()
                + "] is not a data node.");
        }
        RoutingNode toRoutingNode = allocation.routingNodes().node(toDiscoNode.getId());
        if (toRoutingNode == null && toDiscoNode.canContainData() == false) {
            throw new IllegalArgumentException("[move_allocation] can't move [" + index + "][" + shardId + "] from "
                + fromDiscoNode + " to " + toDiscoNode + ": source [" +  toDiscoNode.getName()
                + "] is not a data node.");
        }

        for (ShardRouting shardRouting : fromRoutingNode) {
            if (shardRouting.shardId().getIndexName().equals(index) == false) {
                continue;
            }
            if (shardRouting.shardId().id() != shardId) {
                continue;
            }
            found = true;

            // TODO we can possibly support also relocating cases, where we cancel relocation and move...
            if (shardRouting.started() == false) {
                if (explain) {
                    return new RerouteExplanation(this, allocation.decision(Decision.NO, "move_allocation_command",
                            "shard " + shardId + " has not been started"));
                }
                throw new IllegalArgumentException("[move_allocation] can't move " + shardId +
                        ", shard is not started (state = " + shardRouting.state() + "]");
            }

            decision = allocation.deciders().canAllocate(shardRouting, toRoutingNode, allocation);
            if (decision.type() == Decision.Type.NO) {
                if (explain) {
                    return new RerouteExplanation(this, decision);
                }
                throw new IllegalArgumentException("[move_allocation] can't move " + shardId + ", from " + fromDiscoNode + ", to " +
                    toDiscoNode + ", since its not allowed, reason: " + decision);
            }
            if (decision.type() == Decision.Type.THROTTLE) {
                // its being throttled, maybe have a flag to take it into account and fail? for now, just do it since the "user" wants it...
            }
            allocation.routingNodes().relocateShard(shardRouting, toRoutingNode.nodeId(),
                allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE), allocation.changes());
        }

        if (found == false) {
            if (explain) {
                return new RerouteExplanation(this, allocation.decision(Decision.NO,
                        "move_allocation_command", "shard " + shardId + " not found"));
            }
            throw new IllegalArgumentException("[move_allocation] can't move " + shardId + ", failed to find it on node " + fromDiscoNode);
        }
        return new RerouteExplanation(this, decision);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index());
        builder.field("shard", shardId());
        builder.field("from_node", fromNode());
        builder.field("to_node", toNode());
        return builder.endObject();
    }

    public static MoveAllocationCommand fromXContent(XContentParser parser) throws IOException {
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
        return new MoveAllocationCommand(index, shardId, fromNode, toNode);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MoveAllocationCommand other = (MoveAllocationCommand) obj;
        // Override equals and hashCode for testing
        return Objects.equals(index, other.index) &&
                Objects.equals(shardId, other.shardId) &&
                Objects.equals(fromNode, other.fromNode) &&
                Objects.equals(toNode, other.toNode);
    }

    @Override
    public int hashCode() {
        // Override equals and hashCode for testing
        return Objects.hash(index, shardId, fromNode, toNode);
    }
}
