/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Allocates an unassigned replica shard to a specific node. Checks if allocation deciders allow allocation.
 */
public class AllocateReplicaAllocationCommand extends AbstractAllocateAllocationCommand {
    public static final String NAME = "allocate_replica";
    public static final ParseField COMMAND_NAME_FIELD = new ParseField(NAME);

    private static final ObjectParser<AllocateReplicaAllocationCommand.Builder, Void> REPLICA_PARSER = createAllocateParser(NAME);

    /**
     * Creates a new {@link AllocateReplicaAllocationCommand}
     *
     * @param index          index of the shard to assign
     * @param shardId        id of the shard to assign
     * @param node           node id of the node to assign the shard to
     */
    public AllocateReplicaAllocationCommand(String index, int shardId, String node) {
        super(index, shardId, node);
    }

    /**
     * Read from a stream.
     */
    public AllocateReplicaAllocationCommand(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String name() {
        return NAME;
    }

    public static AllocateReplicaAllocationCommand fromXContent(XContentParser parser) throws IOException {
        return new Builder().parse(parser).build();
    }

    protected static class Builder extends AbstractAllocateAllocationCommand.Builder<AllocateReplicaAllocationCommand> {

        @Override
        public Builder parse(XContentParser parser) throws IOException {
            return REPLICA_PARSER.parse(parser, this, null);
        }

        @Override
        public AllocateReplicaAllocationCommand build() {
            validate();
            return new AllocateReplicaAllocationCommand(index, shard, node);
        }
    }

    @Override
    public RerouteExplanation execute(RoutingAllocation allocation, boolean explain) {
        final DiscoveryNode discoNode;
        try {
            discoNode = allocation.nodes().resolveNode(node);
        } catch (IllegalArgumentException e) {
            return explainOrThrowRejectedCommand(explain, allocation, e);
        }
        final RoutingNodes routingNodes = allocation.routingNodes();
        RoutingNode routingNode = routingNodes.node(discoNode.getId());
        if (routingNode == null) {
            return explainOrThrowMissingRoutingNode(allocation, explain, discoNode);
        }

        try {
            allocation.routingTable().shardRoutingTable(index, shardId).primaryShard();
        } catch (IndexNotFoundException | ShardNotFoundException e) {
            return explainOrThrowRejectedCommand(explain, allocation, e);
        }

        ShardRouting primaryShardRouting = null;
        for (RoutingNode node : allocation.routingNodes()) {
            for (ShardRouting shard : node) {
                if (shard.getIndexName().equals(index) && shard.getId() == shardId && shard.primary()) {
                    primaryShardRouting = shard;
                    break;
                }
            }
        }
        if (primaryShardRouting == null) {
            return explainOrThrowRejectedCommand(explain, allocation,
                "trying to allocate a replica shard [" + index + "][" + shardId +
                    "], while corresponding primary shard is still unassigned");
        }

        List<ShardRouting> replicaShardRoutings = new ArrayList<>();
        for (ShardRouting shard : allocation.routingNodes().unassigned()) {
            if (shard.getIndexName().equals(index) && shard.getId() == shardId && shard.primary() == false) {
                replicaShardRoutings.add(shard);
            }
        }

        ShardRouting shardRouting;
        if (replicaShardRoutings.isEmpty()) {
            return explainOrThrowRejectedCommand(explain, allocation,
                "all copies of [" + index + "][" + shardId + "] are already assigned. Use the move allocation command instead");
        } else {
            shardRouting = replicaShardRoutings.get(0);
        }

        Decision decision = allocation.deciders().canAllocate(shardRouting, routingNode, allocation);
        if (decision.type() == Decision.Type.NO) {
            // don't use explainOrThrowRejectedCommand to keep the original "NO" decision
            if (explain) {
                return new RerouteExplanation(this, decision);
            }
            throw new IllegalArgumentException("[" + name() + "] allocation of [" + index + "][" + shardId + "] on node " + discoNode +
                " is not allowed, reason: " + decision);
        }

        initializeUnassignedShard(allocation, routingNodes, routingNode, shardRouting);
        return new RerouteExplanation(this, decision);
    }
}
