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
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.step.info.SingleMessageFieldInfo;

import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Wait Step for index based on color. Optionally derives the index name using the provided prefix (if any).
 */
class WaitForIndexColorStep extends ClusterStateWaitStep {

    static final String NAME = "wait-for-index-color";

    private static final Logger logger = LogManager.getLogger(WaitForIndexColorStep.class);

    private final ClusterHealthStatus color;

    private final BiFunction<String, LifecycleExecutionState, String> indexNameSupplier;

    WaitForIndexColorStep(StepKey key, StepKey nextStepKey, ClusterHealthStatus color) {
        this(key, nextStepKey, color, (index, lifecycleState) -> index);
    }

    WaitForIndexColorStep(StepKey key, StepKey nextStepKey, ClusterHealthStatus color, @Nullable String indexNamePrefix) {
        this(key, nextStepKey, color, (index, lifecycleState) -> indexNamePrefix + index);
    }

    WaitForIndexColorStep(
        StepKey key,
        StepKey nextStepKey,
        ClusterHealthStatus color,
        BiFunction<String, LifecycleExecutionState, String> indexNameSupplier
    ) {
        super(key, nextStepKey);
        this.color = color;
        this.indexNameSupplier = indexNameSupplier;
    }

    public ClusterHealthStatus getColor() {
        return this.color;
    }

    BiFunction<String, LifecycleExecutionState, String> getIndexNameSupplier() {
        return indexNameSupplier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.color, this.indexNameSupplier);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        WaitForIndexColorStep other = (WaitForIndexColorStep) obj;
        return super.equals(obj)
            && Objects.equals(this.color, other.color)
            && Objects.equals(this.indexNameSupplier, other.indexNameSupplier);
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        LifecycleExecutionState lifecycleExecutionState = clusterState.metadata().index(index.getName()).getLifecycleExecutionState();
        String indexName = indexNameSupplier.apply(index.getName(), lifecycleExecutionState);
        IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        // check if the (potentially) derived index exists
        if (indexMetadata == null) {
            String errorMessage = String.format(
                Locale.ROOT,
                "[%s] lifecycle action for index [%s] executed but the target index [%s] " + "does not exist",
                getKey().getAction(),
                index.getName(),
                indexName
            );
            logger.debug(errorMessage);
            return new Result(false, new SingleMessageFieldInfo(errorMessage));
        }

        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexMetadata.getIndex());
        Result result = switch (this.color) {
            case GREEN -> waitForGreen(indexRoutingTable);
            case YELLOW -> waitForYellow(indexRoutingTable);
            case RED -> waitForRed(indexRoutingTable);
        };
        return result;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    private static Result waitForRed(IndexRoutingTable indexRoutingTable) {
        if (indexRoutingTable == null) {
            return new Result(true, new SingleMessageFieldInfo("index is red"));
        }
        return new Result(false, new SingleMessageFieldInfo("index is not red"));
    }

    private static Result waitForYellow(IndexRoutingTable indexRoutingTable) {
        if (indexRoutingTable == null) {
            return new Result(false, new SingleMessageFieldInfo("index is red; no indexRoutingTable"));
        }

        boolean indexIsAtLeastYellow = indexRoutingTable.allPrimaryShardsActive();
        if (indexIsAtLeastYellow) {
            return new Result(true, null);
        } else {
            return new Result(false, new SingleMessageFieldInfo("index is red; not all primary shards are active"));
        }
    }

    private static Result waitForGreen(IndexRoutingTable indexRoutingTable) {
        if (indexRoutingTable == null) {
            return new Result(false, new SingleMessageFieldInfo("index is red; no indexRoutingTable"));
        }

        if (indexRoutingTable.allPrimaryShardsActive()) {
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                boolean replicaIndexIsGreen = indexRoutingTable.shard(i).replicaShards().stream().allMatch(ShardRouting::active);
                if (replicaIndexIsGreen == false) {
                    return new Result(false, new SingleMessageFieldInfo("index is yellow; not all replica shards are active"));
                }
            }
            return new Result(true, null);
        }

        return new Result(false, new SingleMessageFieldInfo("index is not green; not all shards are active"));
    }
}
