/*
 * Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package org.elasticsearch.xpack.core.ilm;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
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
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

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
