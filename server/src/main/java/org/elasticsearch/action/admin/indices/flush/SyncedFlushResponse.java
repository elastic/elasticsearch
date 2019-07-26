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
package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.flush.ShardsSyncedFlushResult;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * The result of performing a sync flush operation on all shards of multiple indices
 */
public class SyncedFlushResponse extends ActionResponse implements ToXContentFragment {

    private final Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex;
    private final ShardCounts shardCounts;

    public SyncedFlushResponse(Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex) {
        // shardsResultPerIndex is never modified after it is passed to this
        // constructor so this is safe even though shardsResultPerIndex is a
        // ConcurrentHashMap
        this.shardsResultPerIndex = unmodifiableMap(shardsResultPerIndex);
        this.shardCounts = calculateShardCounts(Iterables.flatten(shardsResultPerIndex.values()));
    }

    public SyncedFlushResponse(StreamInput in) throws IOException {
        super(in);
        shardCounts = new ShardCounts(in);
        Map<String, List<ShardsSyncedFlushResult>> tmpShardsResultPerIndex = new HashMap<>();
        int numShardsResults = in.readInt();
        for (int i =0 ; i< numShardsResults; i++) {
            String index = in.readString();
            List<ShardsSyncedFlushResult> shardsSyncedFlushResults = new ArrayList<>();
            int numShards = in.readInt();
            for (int j =0; j< numShards; j++) {
                shardsSyncedFlushResults.add(new ShardsSyncedFlushResult(in));
            }
            tmpShardsResultPerIndex.put(index, shardsSyncedFlushResults);
        }
        shardsResultPerIndex = Collections.unmodifiableMap(tmpShardsResultPerIndex);
    }

    /**
     * total number shards, including replicas, both assigned and unassigned
     */
    public int totalShards() {
        return shardCounts.total;
    }

    /**
     * total number of shards for which the operation failed
     */
    public int failedShards() {
        return shardCounts.failed;
    }

    /**
     * total number of shards which were successfully sync-flushed
     */
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
                    Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> failedShards = shardResults.failedShards();
                    for (Map.Entry<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardEntry : failedShards.entrySet()) {
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

    static final class ShardCounts implements ToXContentFragment, Writeable {

        public final int total;
        public final int successful;
        public final int failed;

        ShardCounts(int total, int successful, int failed) {
            this.total = total;
            this.successful = successful;
            this.failed = failed;
        }

        ShardCounts(StreamInput in) throws IOException {
            total = in.readInt();
            successful = in.readInt();
            failed = in.readInt();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.TOTAL, total);
            builder.field(Fields.SUCCESSFUL, successful);
            builder.field(Fields.FAILED, failed);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(total);
            out.writeInt(successful);
            out.writeInt(failed);
        }
    }

    static final class Fields {
        static final String _SHARDS = "_shards";
        static final String TOTAL = "total";
        static final String SUCCESSFUL = "successful";
        static final String FAILED = "failed";
        static final String FAILURES = "failures";
        static final String SHARD = "shard";
        static final String ROUTING = "routing";
        static final String REASON = "reason";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardCounts.writeTo(out);
        out.writeInt(shardsResultPerIndex.size());
        for (Map.Entry<String, List<ShardsSyncedFlushResult>> entry : shardsResultPerIndex.entrySet()) {
            out.writeString(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (ShardsSyncedFlushResult shardsSyncedFlushResult : entry.getValue()) {
                shardsSyncedFlushResult.writeTo(out);
            }
        }
    }
}
