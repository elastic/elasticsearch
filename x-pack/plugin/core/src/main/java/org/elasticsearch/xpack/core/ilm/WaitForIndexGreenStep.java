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
package org.elasticsearch.xpack.core.ilm;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.ClusterState;
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

class WaitForIndexGreenStep extends ClusterStateWaitStep {

    static final String NAME = "wait-for-index-green-step";

    WaitForIndexGreenStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        RoutingTable routingTable = clusterState.routingTable();
        IndexRoutingTable indexRoutingTable = routingTable.index(index);
        if (indexRoutingTable == null) {
            return new Result(false, new Info("index is red; no IndexRoutingTable"));
        }

        boolean indexIsGreen = false;
        if(indexRoutingTable.allPrimaryShardsActive()) {
            boolean replicaIndexIsGreen = false;
            for (ObjectCursor<IndexShardRoutingTable> shardRouting : indexRoutingTable.getShards().values()) {
                replicaIndexIsGreen = shardRouting.value.replicaShards().stream().allMatch(ShardRouting::active);
                if(!replicaIndexIsGreen) {
                    return new Result(false, new Info("index is yellow; not all replica shards are active"));
                }
            }
            indexIsGreen = replicaIndexIsGreen;
        }


        if (indexIsGreen) {
            return new Result(true, null);
        } else {
            return new Result(false, new Info("index is not green; not all shards are active"));
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
