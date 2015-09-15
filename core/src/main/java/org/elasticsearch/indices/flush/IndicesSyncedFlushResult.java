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
package org.elasticsearch.indices.flush;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The result of performing a sync flush operation on all shards of multiple indices
 */
public class IndicesSyncedFlushResult implements ToXContent {

    final Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex;
    final ShardCounts shardCounts;


    public IndicesSyncedFlushResult(Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex) {
        this.shardsResultPerIndex = ImmutableMap.copyOf(shardsResultPerIndex);
        this.shardCounts = calculateShardCounts(Iterables.flatten(shardsResultPerIndex.values()));
    }

    /** total number shards, including replicas, both assigned and unassigned */
    public int totalShards() {
        return shardCounts.total;
    }

    /** total number of shards for which the operation failed */
    public int failedShards() {
        return shardCounts.failed;
    }

    /** total number of shards which were successfully sync-flushed */
    public int successfulShards() {
        return shardCounts.successful;
    }

    public RestStatus restStatus() {
        return failedShards() == 0 ? RestStatus.OK : RestStatus.CONFLICT;
    }

    public Map<String, List<ShardsSyncedFlushResult>> getShardsResultPerIndex() {
        return shardsResultPerIndex;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields._SHARDS);
        shardCounts.toXContent(builder, params);
        builder.endObject();
        for (Map.Entry<String, List<ShardsSyncedFlushResult>> indexEntry : shardsResultPerIndex.entrySet()) {
            List<ShardsSyncedFlushResult> indexResult = indexEntry.getValue();
            builder.startObject(indexEntry.getKey());
            ShardCounts indexShardCounts = calculateShardCounts(indexResult);
            indexShardCounts.toXContent(builder, params);
            if (indexShardCounts.failed > 0) {
                builder.startArray(Fields.FAILURES);
                for (ShardsSyncedFlushResult shardResults : indexResult) {
                    if (shardResults.failed()) {
                        builder.startObject();
                        builder.field(Fields.SHARD, shardResults.shardId().id());
                        builder.field(Fields.REASON, shardResults.failureReason());
                        builder.endObject();
                        continue;
                    }
                    Map<ShardRouting, SyncedFlushService.SyncedFlushResponse> failedShards = shardResults.failedShards();
                    for (Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardEntry : failedShards.entrySet()) {
                        builder.startObject();
                        builder.field(Fields.SHARD, shardResults.shardId().id());
                        builder.field(Fields.REASON, shardEntry.getValue().failureReason());
                        builder.field(Fields.ROUTING, shardEntry.getKey());
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();
        }
        return builder;
    }

    static ShardCounts calculateShardCounts(Iterable<ShardsSyncedFlushResult> results) {
        int total = 0, successful = 0, failed = 0;
        for (ShardsSyncedFlushResult result : results) {
            total += result.totalShards();
            successful += result.successfulShards();
            if (result.failed()) {
                // treat all shard copies as failed
                failed += result.totalShards();
            } else {
                // some shards may have failed during the sync phase
                failed += result.failedShards().size();
            }
        }
        return new ShardCounts(total, successful, failed);
    }

    static final class ShardCounts implements ToXContent {

        public final int total;
        public final int successful;
        public final int failed;

        ShardCounts(int total, int successful, int failed) {
            this.total = total;
            this.successful = successful;
            this.failed = failed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.TOTAL, total);
            builder.field(Fields.SUCCESSFUL, successful);
            builder.field(Fields.FAILED, failed);
            return builder;
        }
    }

    static final class Fields {
        static final XContentBuilderString _SHARDS = new XContentBuilderString("_shards");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString SUCCESSFUL = new XContentBuilderString("successful");
        static final XContentBuilderString FAILED = new XContentBuilderString("failed");
        static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
        static final XContentBuilderString SHARD = new XContentBuilderString("shard");
        static final XContentBuilderString ROUTING = new XContentBuilderString("routing");
        static final XContentBuilderString REASON = new XContentBuilderString("reason");
    }
}
