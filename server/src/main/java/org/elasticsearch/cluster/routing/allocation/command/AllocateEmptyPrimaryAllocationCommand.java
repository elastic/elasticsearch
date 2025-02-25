/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

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

    private static final ObjectParser<Builder, ProjectId> EMPTY_PRIMARY_PARSER = BasePrimaryAllocationCommand.createAllocatePrimaryParser(
        NAME
    );

    /**
     * Creates a new {@link AllocateEmptyPrimaryAllocationCommand}
     *
     * @param shardId        {@link ShardId} of the shard to assign
     * @param node           node id of the node to assign the shard to
     * @param acceptDataLoss whether the user agrees to data loss
     * @param projectId      the project-id that this index belongs to
     */
    public AllocateEmptyPrimaryAllocationCommand(String index, int shardId, String node, boolean acceptDataLoss, ProjectId projectId) {
        super(index, shardId, node, acceptDataLoss, projectId);
    }

    @FixForMultiProject(description = "Should be removed since a ProjectId must always be available")
    @Deprecated(forRemoval = true)
    public AllocateEmptyPrimaryAllocationCommand(String index, int shardId, String node, boolean acceptDataLoss) {
        this(index, shardId, node, acceptDataLoss, Metadata.DEFAULT_PROJECT_ID);
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

    @FixForMultiProject(description = "projectId should not be null once multi-project is fully in place")
    public static AllocateEmptyPrimaryAllocationCommand fromXContent(XContentParser parser, Object projectId) throws IOException {
        assert projectId == null || projectId instanceof ProjectId : projectId;
        return new Builder((ProjectId) projectId).parse(parser).build();
    }

    public static class Builder extends BasePrimaryAllocationCommand.Builder<AllocateEmptyPrimaryAllocationCommand> {

        Builder(ProjectId projectId) {
            super(projectId);
        }

        private Builder parse(XContentParser parser) throws IOException {
            return EMPTY_PRIMARY_PARSER.parse(parser, this, null);
        }

        @Override
        public AllocateEmptyPrimaryAllocationCommand build() {
            validate();
            return new AllocateEmptyPrimaryAllocationCommand(index, shard, node, acceptDataLoss, projectId);
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
            allocation.globalRoutingTable().routingTable(projectId).shardRoutingTable(index, shardId).primaryShard();
        } catch (IndexNotFoundException | ShardNotFoundException e) {
            return explainOrThrowRejectedCommand(explain, allocation, e);
        }

        ShardRouting shardRouting = null;
        for (ShardRouting shard : allocation.routingNodes().unassigned()) {
            if (shard.getIndexName().equals(index)
                && shard.getId() == shardId
                && shard.primary()
                && projectId.equals(allocation.metadata().projectFor(shard.index()).id())) {
                shardRouting = shard;
                break;
            }
        }
        if (shardRouting == null) {
            return explainOrThrowRejectedCommand(explain, allocation, "primary [" + index + "][" + shardId + "] is already assigned");
        }

        if (shardRouting.recoverySource().getType() != RecoverySource.Type.EMPTY_STORE && acceptDataLoss == false) {
            String dataLossWarning = "allocating an empty primary for ["
                + index
                + "]["
                + shardId
                + "] can result in data loss. Please confirm by setting the accept_data_loss parameter to true";
            return explainOrThrowRejectedCommand(explain, allocation, dataLossWarning);
        }

        UnassignedInfo unassignedInfoToUpdate = null;
        if (shardRouting.unassignedInfo().reason() != UnassignedInfo.Reason.FORCED_EMPTY_PRIMARY) {
            String unassignedInfoMessage = "force empty allocation from previous reason "
                + shardRouting.unassignedInfo().reason()
                + ", "
                + shardRouting.unassignedInfo().message();
            unassignedInfoToUpdate = new UnassignedInfo(
                UnassignedInfo.Reason.FORCED_EMPTY_PRIMARY,
                unassignedInfoMessage,
                shardRouting.unassignedInfo().failure(),
                0,
                System.nanoTime(),
                System.currentTimeMillis(),
                false,
                shardRouting.unassignedInfo().lastAllocationStatus(),
                Collections.emptySet(),
                null
            );
        }

        initializeUnassignedShard(
            allocation,
            routingNodes,
            routingNode,
            shardRouting,
            unassignedInfoToUpdate,
            EmptyStoreRecoverySource.INSTANCE
        );

        return new RerouteExplanation(this, allocation.decision(Decision.YES, name() + " (allocation command)", "ignore deciders"));
    }
}
