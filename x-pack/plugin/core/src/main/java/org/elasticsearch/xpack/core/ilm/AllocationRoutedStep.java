/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

/**
 * Checks whether all shards have been correctly routed in response to an update to the allocation rules for an index.
 */
public class AllocationRoutedStep extends ClusterStateWaitStep {
    public static final String NAME = "check-allocation";

    private static final Logger logger = LogManager.getLogger(AllocationRoutedStep.class);

    private static final AllocationDeciders ALLOCATION_DECIDERS = new AllocationDeciders(Collections.singletonList(
            new FilterAllocationDecider(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))));

    AllocationRoutedStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetadata idxMeta = clusterState.metadata().index(index);
        if (idxMeta == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            return new Result(false, null);
        }
        if (ActiveShardCount.ALL.enoughShardsActive(clusterState, index.getName()) == false) {
            logger.debug("[{}] lifecycle action for index [{}] cannot make progress because not all shards are active",
                    getKey().getAction(), index.getName());
            return new Result(false, new Info(idxMeta.getNumberOfReplicas(), -1, false));
        }
        // All the allocation attributes are already set so just need to check
        // if the allocation has happened
        RoutingAllocation allocation = new RoutingAllocation(ALLOCATION_DECIDERS, clusterState.getRoutingNodes(), clusterState, null,
                System.nanoTime());

        int allocationPendingAllShards = 0;

        ImmutableOpenIntMap<IndexShardRoutingTable> allShards = clusterState.getRoutingTable().index(index).getShards();
        for (ObjectCursor<IndexShardRoutingTable> shardRoutingTable : allShards.values()) {
            for (ShardRouting shardRouting : shardRoutingTable.value.shards()) {
                String currentNodeId = shardRouting.currentNodeId();
                boolean canRemainOnCurrentNode = ALLOCATION_DECIDERS
                        .canRemain(shardRouting, clusterState.getRoutingNodes().node(currentNodeId), allocation)
                        .type() == Decision.Type.YES;
                if (canRemainOnCurrentNode == false || shardRouting.started() == false) {
                    allocationPendingAllShards++;
                }
            }
        }

        if (allocationPendingAllShards > 0) {
            logger.debug("{} lifecycle action [{}] waiting for [{}] shards to be allocated to nodes matching the given filters",
                index, getKey().getAction(), allocationPendingAllShards);
            return new Result(false, new Info(idxMeta.getNumberOfReplicas(), allocationPendingAllShards, true));
        } else {
            logger.debug("{} lifecycle action for [{}] complete", index, getKey().getAction());
            return new Result(true, null);
        }
    }

    @Override
    public int hashCode() {
        return 611;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return super.equals(obj);
    }

    public static final class Info implements ToXContentObject {

        private final long actualReplicas;
        private final long numberShardsLeftToAllocate;
        private final boolean allShardsActive;
        private final String message;

        static final ParseField ACTUAL_REPLICAS = new ParseField("actual_replicas");
        static final ParseField SHARDS_TO_ALLOCATE = new ParseField("shards_left_to_allocate");
        static final ParseField ALL_SHARDS_ACTIVE = new ParseField("all_shards_active");
        static final ParseField MESSAGE = new ParseField("message");
        static final ConstructingObjectParser<Info, Void> PARSER = new ConstructingObjectParser<>("allocation_routed_step_info",
                a -> new Info((long) a[0], (long) a[1], (boolean) a[2]));
        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), ACTUAL_REPLICAS);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SHARDS_TO_ALLOCATE);
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ALL_SHARDS_ACTIVE);
            PARSER.declareString((i, s) -> {}, MESSAGE);
        }

        public Info(long actualReplicas, long numberShardsLeftToAllocate, boolean allShardsActive) {
            this.actualReplicas = actualReplicas;
            this.numberShardsLeftToAllocate = numberShardsLeftToAllocate;
            this.allShardsActive = allShardsActive;
            if (allShardsActive == false) {
                message = "Waiting for all shard copies to be active";
            } else {
                message = "Waiting for [" + numberShardsLeftToAllocate + "] shards "
                        + "to be allocated to nodes matching the given filters";
            }
        }

        public long getActualReplicas() {
            return actualReplicas;
        }

        public long getNumberShardsLeftToAllocate() {
            return numberShardsLeftToAllocate;
        }

        public boolean allShardsActive() {
            return allShardsActive;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE.getPreferredName(), message);
            builder.field(SHARDS_TO_ALLOCATE.getPreferredName(), numberShardsLeftToAllocate);
            builder.field(ALL_SHARDS_ACTIVE.getPreferredName(), allShardsActive);
            builder.field(ACTUAL_REPLICAS.getPreferredName(), actualReplicas);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(actualReplicas, numberShardsLeftToAllocate, allShardsActive);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Info other = (Info) obj;
            return Objects.equals(actualReplicas, other.actualReplicas) &&
                Objects.equals(numberShardsLeftToAllocate, other.numberShardsLeftToAllocate) &&
                Objects.equals(allShardsActive, other.allShardsActive);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
