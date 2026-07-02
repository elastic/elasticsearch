/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RecoverySource;
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
import java.util.Optional;

public class AllocateReshardSplitTargetPrimaryCommand extends BasePrimaryAllocationCommand {
    public static final String NAME = "allocate_reshard_split_target_primary";
    public static final ParseField COMMAND_NAME_FIELD = new ParseField(NAME);

    private static final ObjectParser<AllocateReshardSplitTargetPrimaryCommand.Builder, ProjectId> PARSER = BasePrimaryAllocationCommand
        .createAllocatePrimaryParser(NAME);

    public AllocateReshardSplitTargetPrimaryCommand(String index, int shardId, String node, boolean acceptDataLoss, ProjectId projectId) {
        super(index, shardId, node, acceptDataLoss, projectId);
    }

    @FixForMultiProject(description = "Should be removed since a ProjectId must always be available")
    @Deprecated(forRemoval = true)
    public AllocateReshardSplitTargetPrimaryCommand(String index, int shardId, String node, boolean acceptDataLoss) {
        this(index, shardId, node, acceptDataLoss, Metadata.DEFAULT_PROJECT_ID);
    }

    public AllocateReshardSplitTargetPrimaryCommand(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String name() {
        return NAME;
    }

    @FixForMultiProject(description = "projectId should not be null once multi-project is fully in place")
    public static AllocateReshardSplitTargetPrimaryCommand fromXContent(XContentParser parser, Object projectId) throws IOException {
        assert projectId == null || projectId instanceof ProjectId : projectId;
        return new AllocateReshardSplitTargetPrimaryCommand.Builder((ProjectId) projectId).parse(parser).build();
    }

    public static class Builder extends BasePrimaryAllocationCommand.Builder<AllocateReshardSplitTargetPrimaryCommand> {

        Builder(ProjectId projectId) {
            super(projectId);
        }

        private AllocateReshardSplitTargetPrimaryCommand.Builder parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, this, null);
        }

        @Override
        public AllocateReshardSplitTargetPrimaryCommand build() {
            validate();
            return new AllocateReshardSplitTargetPrimaryCommand(index, shard, node, acceptDataLoss, projectId);
        }
    }

    @Override
    public RerouteExplanation execute(RoutingAllocation allocation, boolean explain) {
        ShardRouting shardRouting;
        try {
            shardRouting = allocation.globalRoutingTable().routingTable(projectId).shardRoutingTable(index, shardId).primaryShard();
        } catch (IndexNotFoundException | ShardNotFoundException e) {
            return explainOrThrowRejectedCommand(explain, allocation, e);
        }

        if (shardRouting.unassigned() == false || shardRouting.recoverySource().getType() != RecoverySource.Type.EMPTY_STORE) {
            return explainOrThrowRejectedCommand(explain, allocation, "Requested shard is not in unassigned state " + shardRouting);
        }

        Optional<IndexMetadata> maybeIndexMetadata = allocation.metadata().findIndex(shardRouting.index());
        if (maybeIndexMetadata.isEmpty()) {
            return explainOrThrowRejectedCommand(explain, allocation, "Requested index does not exist");
        }
        IndexMetadata indexMetadata = maybeIndexMetadata.get();

        if (indexMetadata.getReshardingMetadata() == null || indexMetadata.getReshardingMetadata().isSplit() == false) {
            return explainOrThrowRejectedCommand(explain, allocation, "Requested index is not being split");
        }

        if (indexMetadata.getReshardingMetadata().getSplit().isTargetShard(shardId) == false) {
            return explainOrThrowRejectedCommand(explain, allocation, "Requested shard is not a split target shard");
        }

        for (RoutingNodes.UnassignedShards.UnassignedIterator it = allocation.routingNodes().unassigned().iterator(); it.hasNext();) {
            ShardRouting unassigned = it.next();
            if (unassigned.equalsIgnoringMetadata(shardRouting) == false) {
                continue;
            }

            var unassignedInfo = new UnassignedInfo(
                UnassignedInfo.Reason.RESHARD_ADDED,
                "force allocation of resharding split target shard"
            );

            var recoverySource = new RecoverySource.ReshardSplitRecoverySource(
                new ShardId(shardRouting.index(), indexMetadata.getReshardingMetadata().getSplit().sourceShard(shardId))
            );

            it.updateUnassigned(unassignedInfo, recoverySource, allocation.changes());
            break;
        }

        return new RerouteExplanation(this, allocation.decision(Decision.YES, name() + " (allocation command)", "ignore deciders"));
    }
}
