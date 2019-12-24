/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Objects;

class WaitForYellowStep extends ClusterStateWaitStep {

    static final String NAME = "wait-for-yellow-step";

    WaitForYellowStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        RoutingTable routingTable = clusterState.routingTable();
        IndexRoutingTable indexShardRoutingTable = routingTable.index(index);
        if (indexShardRoutingTable == null) {
            return new Result(false, new Info("index is red; no IndexRoutingTable"));
        }

        boolean indexIsAtLeastYellow = indexShardRoutingTable.allPrimaryShardsActive();
        if (indexIsAtLeastYellow) {
            return new Result(true, null);
        } else {
            return new Result(false, new Info("index is red; not all primary shards are active"));
        }
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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Info info = (Info) o;
            return Objects.equals(getMessage(), info.getMessage());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getMessage());
        }
    }
}
