/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ilm.step.info.SingleMessageFieldInfo;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * This step is used prior to running a shrink step in order to ensure that the index being shrunk
 * has a copy of each shard allocated on one particular node (the node used by the require
 * parameter) and that the shards are not relocating.
 */
public class CheckShrinkReadyStep extends ClusterStateWaitStep {
    public static final String NAME = "check-shrink-allocation";

    private static final Logger logger = LogManager.getLogger(CheckShrinkReadyStep.class);
    private boolean completable = true;

    CheckShrinkReadyStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public boolean isCompletable() {
        return completable;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetadata idxMeta = clusterState.metadata().index(index);

        if (idxMeta == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().action(), index.getName());
            return new Result(false, null);
        }

        // How many shards the node should have
        int expectedShardCount = idxMeta.getNumberOfShards();

        // The id of the node the shards should be on
        final String idShardsShouldBeOn = idxMeta.getSettings().get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._id");
        if (idShardsShouldBeOn == null) {
            throw new IllegalStateException("Cannot check shrink allocation as there are no allocation rules by _id");
        }

        var shutdown = clusterState.metadata().nodeShutdowns().get(idShardsShouldBeOn);
        boolean nodeBeingRemoved = shutdown != null && shutdown.getType() != SingleNodeShutdownMetadata.Type.RESTART;

        final IndexRoutingTable routingTable = clusterState.getRoutingTable().index(index);
        int foundShards = 0;
        for (ShardRouting shard : routingTable.shardsWithState(ShardRoutingState.STARTED)) {
            final String currentNodeId = shard.currentNodeId();
            if (idShardsShouldBeOn.equals(currentNodeId) && shard.relocating() == false) {
                foundShards++;
            }
        }

        logger.trace(
            "{} checking for shrink readiness on [{}], found {} shards and need {}",
            index,
            idShardsShouldBeOn,
            foundShards,
            expectedShardCount
        );

        if (foundShards == expectedShardCount) {
            logger.trace(
                "{} successfully found {} allocated shards for shrink readiness on node [{}] ({})",
                index,
                expectedShardCount,
                idShardsShouldBeOn,
                getKey().action()
            );
            return new Result(true, null);
        } else {
            if (nodeBeingRemoved) {
                completable = false;
                return new Result(
                    false,
                    new SingleMessageFieldInfo("node with id [" + idShardsShouldBeOn + "] is currently marked as shutting down for removal")
                );
            }

            logger.trace(
                "{} failed to find {} allocated shards (found {}) on node [{}] for shrink readiness ({})",
                index,
                expectedShardCount,
                foundShards,
                idShardsShouldBeOn,
                getKey().action()
            );
            return new Result(
                false,
                new CheckShrinkReadyStep.Info(idShardsShouldBeOn, expectedShardCount, expectedShardCount - foundShards)
            );
        }
    }

    @Override
    public int hashCode() {
        return 612;
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

        private final String nodeId;
        private final long actualReplicas;
        private final long numberShardsLeftToAllocate;
        private final String message;

        static final ParseField NODE_ID = new ParseField("node_id");
        static final ParseField EXPECTED_SHARDS = new ParseField("expected_shards");
        static final ParseField SHARDS_TO_ALLOCATE = new ParseField("shards_left_to_allocate");
        static final ParseField MESSAGE = new ParseField("message");
        static final ConstructingObjectParser<CheckShrinkReadyStep.Info, Void> PARSER = new ConstructingObjectParser<>(
            "check_shrink_ready_step_info",
            a -> new CheckShrinkReadyStep.Info((String) a[0], (long) a[1], (long) a[2])
        );
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), EXPECTED_SHARDS);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SHARDS_TO_ALLOCATE);
            PARSER.declareString((i, s) -> {}, MESSAGE);
        }

        public Info(String nodeId, long expectedShards, long numberShardsLeftToAllocate) {
            this.nodeId = nodeId;
            this.actualReplicas = expectedShards;
            this.numberShardsLeftToAllocate = numberShardsLeftToAllocate;
            if (numberShardsLeftToAllocate < 0) {
                this.message = "Waiting for all shards to become active";
            } else {
                this.message = String.format(
                    Locale.ROOT,
                    "Waiting for node [%s] to contain [%d] shards, found [%d], remaining [%d]",
                    nodeId,
                    expectedShards,
                    expectedShards - numberShardsLeftToAllocate,
                    numberShardsLeftToAllocate
                );
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE.getPreferredName(), message);
            builder.field(NODE_ID.getPreferredName(), nodeId);
            builder.field(SHARDS_TO_ALLOCATE.getPreferredName(), numberShardsLeftToAllocate);
            builder.field(EXPECTED_SHARDS.getPreferredName(), actualReplicas);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, actualReplicas, numberShardsLeftToAllocate);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            CheckShrinkReadyStep.Info other = (CheckShrinkReadyStep.Info) obj;
            return Objects.equals(actualReplicas, other.actualReplicas)
                && Objects.equals(numberShardsLeftToAllocate, other.numberShardsLeftToAllocate)
                && Objects.equals(nodeId, other.nodeId);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
