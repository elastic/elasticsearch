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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

/**
 * Allocates an unassigned empty primary shard to a specific node. Use with extreme care as this will result in data loss.
 * Allocation deciders are ignored.
 */
public class AllocateEmptyPrimaryAllocationCommand extends BasePrimaryAllocationCommand {
    public static final String NAME = "allocate_empty_primary";
    public static final ParseField COMMAND_NAME_FIELD = new ParseField(NAME);

    private static final ObjectParser<Builder, Void> EMPTY_PRIMARY_PARSER = BasePrimaryAllocationCommand.createAllocatePrimaryParser(NAME);

    /**
     * Creates a new {@link AllocateEmptyPrimaryAllocationCommand}
     *
     * @param shardId        {@link ShardId} of the shard to assign
     * @param node           node id of the node to assign the shard to
     * @param acceptDataLoss whether the user agrees to data loss
     */
    public AllocateEmptyPrimaryAllocationCommand(String index, int shardId, String node, boolean acceptDataLoss) {
        super(index, shardId, node, acceptDataLoss);
    }

    /**
     * Read from a stream.
     */
    public AllocateEmptyPrimaryAllocationCommand(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Optional<String> getMessage() {
        return Optional.of("allocated an empty primary for [" + index + "][" + shardId + "] on node [" + node + "] from user command");
    }

    public static AllocateEmptyPrimaryAllocationCommand fromXContent(XContentParser parser) throws IOException {
        return new Builder().parse(parser).build();
    }

    public static class Builder extends BasePrimaryAllocationCommand.Builder<AllocateEmptyPrimaryAllocationCommand> {

        @Override
        public Builder parse(XContentParser parser) throws IOException {
            return EMPTY_PRIMARY_PARSER.parse(parser, this, null);
        }

        @Override
        public AllocateEmptyPrimaryAllocationCommand build() {
            validate();
            return new AllocateEmptyPrimaryAllocationCommand(index, shard, node, acceptDataLoss);
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

        if (shardRouting.recoverySource().getType() != RecoverySource.Type.EMPTY_STORE && acceptDataLoss == false) {
            String dataLossWarning = "allocating an empty primary for [" + index + "][" + shardId +
                "] can result in data loss. Please confirm by setting the accept_data_loss parameter to true";
            return explainOrThrowRejectedCommand(explain, allocation, dataLossWarning);
        }

        UnassignedInfo unassignedInfoToUpdate = null;
        if (shardRouting.unassignedInfo().getReason() != UnassignedInfo.Reason.FORCED_EMPTY_PRIMARY) {
            String unassignedInfoMessage = "force empty allocation from previous reason " + shardRouting.unassignedInfo().getReason() +
                ", " + shardRouting.unassignedInfo().getMessage();
            unassignedInfoToUpdate = new UnassignedInfo(UnassignedInfo.Reason.FORCED_EMPTY_PRIMARY, unassignedInfoMessage,
                shardRouting.unassignedInfo().getFailure(), 0, System.nanoTime(), System.currentTimeMillis(), false,
                shardRouting.unassignedInfo().getLastAllocationStatus(), Collections.emptySet());
        }

        initializeUnassignedShard(allocation, routingNodes, routingNode, shardRouting, unassignedInfoToUpdate,
            EmptyStoreRecoverySource.INSTANCE);

        return new RerouteExplanation(this, allocation.decision(Decision.YES, name() + " (allocation command)", "ignore deciders"));
    }
}
