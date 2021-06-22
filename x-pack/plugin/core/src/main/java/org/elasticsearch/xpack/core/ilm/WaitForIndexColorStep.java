/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Wait Step for index based on color. Optionally derives the index name using the provided prefix (if any).
 */
class WaitForIndexColorStep extends ClusterStateWaitStep {

    static final String NAME = "wait-for-index-color";

    private static final Logger logger = LogManager.getLogger(WaitForIndexColorStep.class);

    private final ClusterHealthStatus color;
    @Nullable
    private final String indexNamePrefix;

    WaitForIndexColorStep(StepKey key, StepKey nextStepKey, ClusterHealthStatus color) {
        this(key, nextStepKey, color, null);
    }

    WaitForIndexColorStep(StepKey key, StepKey nextStepKey, ClusterHealthStatus color, @Nullable String indexNamePrefix) {
        super(key, nextStepKey);
        this.color = color;
        this.indexNamePrefix = indexNamePrefix;
    }

    public ClusterHealthStatus getColor() {
        return this.color;
    }

    public String getIndexNamePrefix() {
        return indexNamePrefix;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.color, this.indexNamePrefix);
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
        return super.equals(obj) && Objects.equals(this.color, other.color) && Objects.equals(this.indexNamePrefix, other.indexNamePrefix);
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        String indexName = indexNamePrefix != null ? indexNamePrefix + index.getName() : index.getName();
        IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        // check if the (potentially) derived index exists
        if (indexMetadata == null) {
            String errorMessage = String.format(Locale.ROOT, "[%s] lifecycle action for index [%s] executed but the target index [%s] " +
                    "does not exist",
                getKey().getAction(), index.getName(), indexName);
            logger.debug(errorMessage);
            return new Result(false, new Info(errorMessage));
        }

        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexMetadata.getIndex());
        Result result;
        switch (this.color) {
            case GREEN:
                result = waitForGreen(indexRoutingTable);
                break;
            case YELLOW:
                result = waitForYellow(indexRoutingTable);
                break;
            case RED:
                result = waitForRed(indexRoutingTable);
                break;
            default:
                result = new Result(false, new Info("no index color match"));
                break;
        }
        return result;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    private Result waitForRed(IndexRoutingTable indexRoutingTable) {
        if (indexRoutingTable == null) {
            return new Result(true, new Info("index is red"));
        }
        return new Result(false, new Info("index is not red"));
    }

    private Result waitForYellow(IndexRoutingTable indexRoutingTable) {
        if (indexRoutingTable == null) {
            return new Result(false, new Info("index is red; no indexRoutingTable"));
        }

        boolean indexIsAtLeastYellow = indexRoutingTable.allPrimaryShardsActive();
        if (indexIsAtLeastYellow) {
            return new Result(true, null);
        } else {
            return new Result(false, new Info("index is red; not all primary shards are active"));
        }
    }

    private Result waitForGreen(IndexRoutingTable indexRoutingTable) {
        if (indexRoutingTable == null) {
            return new Result(false, new Info("index is red; no indexRoutingTable"));
        }

        if (indexRoutingTable.allPrimaryShardsActive()) {
            for (ObjectCursor<IndexShardRoutingTable> shardRouting : indexRoutingTable.getShards().values()) {
                boolean replicaIndexIsGreen = shardRouting.value.replicaShards().stream().allMatch(ShardRouting::active);
                if (replicaIndexIsGreen == false) {
                    return new Result(false, new Info("index is yellow; not all replica shards are active"));
                }
            }
            return new Result(true, null);
        }

        return new Result(false, new Info("index is not green; not all shards are active"));
    }

    static final class Info implements ToXContentObject {

        static final ParseField MESSAGE_FIELD = new ParseField("message");

        private final String message;

        Info(String message) {
            this.message = message;
        }

        String getMessage() {
            return message;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE_FIELD.getPreferredName(), message);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            if (getClass() != o.getClass()) {
                return false;
            }
            Info info = (Info) o;
            return Objects.equals(getMessage(), info.getMessage());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getMessage());
        }
    }
}
