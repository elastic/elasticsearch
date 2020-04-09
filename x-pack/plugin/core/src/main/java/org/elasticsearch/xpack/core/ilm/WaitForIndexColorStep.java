/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Objects;

/**
 * Wait Step for index based on color
 */

class WaitForIndexColorStep extends ClusterStateWaitStep {

    static final String NAME = "wait-for-index-color";

    private final ClusterHealthStatus color;

    WaitForIndexColorStep(StepKey key, StepKey nextStepKey, ClusterHealthStatus color) {
        super(key, nextStepKey);
        this.color = color;
    }

    public ClusterHealthStatus getColor() {
        return this.color;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.color);
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
        return super.equals(obj) && Objects.equals(this.color, other.color);
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        RoutingTable routingTable = clusterState.routingTable();
        IndexRoutingTable indexRoutingTable = routingTable.index(index);
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
