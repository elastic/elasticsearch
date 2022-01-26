/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Optional;

/**
 * Allocates an unassigned stale primary shard to a specific node. Use with extreme care as this will result in data loss.
 * Allocation deciders are ignored.
 */
public class AllocateStalePrimaryAllocationCommand extends BasePrimaryAllocationCommand {
    public static final String NAME = "allocate_stale_primary";
    public static final ParseField COMMAND_NAME_FIELD = new ParseField(NAME);

    private static final ObjectParser<Builder, Void> STALE_PRIMARY_PARSER = BasePrimaryAllocationCommand.createAllocatePrimaryParser(NAME);

    /**
     * Creates a new {@link AllocateStalePrimaryAllocationCommand}
     *
     * @param index          index of the shard to assign
     * @param shardId        id of the shard to assign
     * @param node           node id of the node to assign the shard to
     * @param acceptDataLoss whether the user agrees to data loss
     */
    public AllocateStalePrimaryAllocationCommand(String index, int shardId, String node, boolean acceptDataLoss) {
        super(index, shardId, node, acceptDataLoss);
    }

    /**
     * Read from a stream.
     */
    public AllocateStalePrimaryAllocationCommand(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Optional<String> getMessage() {
        return Optional.of("allocated a stale primary for [" + index + "][" + shardId + "] on node [" + node + "] from user command");
    }

    public static AllocateStalePrimaryAllocationCommand fromXContent(XContentParser parser) throws IOException {
        return new Builder().parse(parser).build();
    }

    public static class Builder extends BasePrimaryAllocationCommand.Builder<AllocateStalePrimaryAllocationCommand> {

        @Override
        public Builder parse(XContentParser parser) throws IOException {
            return STALE_PRIMARY_PARSER.parse(parser, this, null);
        }

        @Override
        public AllocateStalePrimaryAllocationCommand build() {
            validate();
            return new AllocateStalePrimaryAllocationCommand(index, shard, node, acceptDataLoss);
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

        ShardRouting shardRouting = null;
        for (ShardRouting shard : allocation.routingNodes().unassigned()) {
            if (shard.getIndexName().equals(index) && shard.getId() == shardId && shard.primary()) {
                shardRouting = shard;
                break;
            }
        }
        if (shardRouting == null) {
            return explainOrThrowRejectedCommand(explain, allocation, "primary [" + index + "][" + shardId + "] is already assigned");
        }

        if (acceptDataLoss == false) {
            String dataLossWarning = "allocating an empty primary for ["
                + index
                + "]["
                + shardId
                + "] can result in data loss. Please "
                + "confirm by setting the accept_data_loss parameter to true";
            return explainOrThrowRejectedCommand(explain, allocation, dataLossWarning);
        }

        if (shardRouting.recoverySource().getType() != RecoverySource.Type.EXISTING_STORE) {
            return explainOrThrowRejectedCommand(
                explain,
                allocation,
                "trying to allocate an existing primary shard [" + index + "][" + shardId + "], while no such shard has ever been active"
            );
        }

        initializeUnassignedShard(
            allocation,
            routingNodes,
            routingNode,
            shardRouting,
            null,
            RecoverySource.ExistingStoreRecoverySource.FORCE_STALE_PRIMARY_INSTANCE
        );
        return new RerouteExplanation(this, allocation.decision(Decision.YES, name() + " (allocation command)", "ignore deciders"));
    }

}
