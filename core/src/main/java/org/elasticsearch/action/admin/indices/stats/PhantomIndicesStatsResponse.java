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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response message with {@link PhantomShardStats} for requesting node or rest client.
 */
public class PhantomIndicesStatsResponse extends BroadcastResponse implements ToXContent {

    private PhantomShardStats[] shards;

    public PhantomIndicesStatsResponse() {

    }

    PhantomIndicesStatsResponse(PhantomShardStats[] shards, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    private PhantomIndexStats[] getIndexStats() {
        Map<String, PhantomIndexStats> indexStats = new HashMap<>();

        for (PhantomShardStats shard : shards) {
            if (shard == null) {
                continue; // skip null response from non-phantom shards
            }

            String index = shard.shardId().getIndex();
            PhantomIndexStats phantomIndexStats = indexStats.get(index);
            if (phantomIndexStats == null) {
                phantomIndexStats = new PhantomIndexStats(index);
                indexStats.put(index, phantomIndexStats);
            }
            phantomIndexStats.add(shard);
        }

        return indexStats.values().toArray(new PhantomIndexStats[indexStats.size()]);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        String level = params.param("level", "indices");
        boolean isLevelValid = "indices".equalsIgnoreCase(level) || "shards".equalsIgnoreCase(level) || "cluster".equalsIgnoreCase(level);
        if (!isLevelValid) {
            return builder;
        }

        if (level.equalsIgnoreCase("indices") || level.equalsIgnoreCase("shards")) {
            builder.startArray("indices");
            for (PhantomIndexStats indexStats : getIndexStats()) {
                builder.startObject();

                builder.startObject(indexStats.index);
                builder.field("loaded_shards_count", indexStats.loadShardCount());
                builder.field("used_heap_size", ByteSizeValue.of(indexStats.usedHeapSize()));
                builder.field("used_heap_size_in_bytes", indexStats.usedHeapSize());
                builder.field("used_heap_size_percent", indexStats.usedHeapSize() * 100 / indexStats.actualHeapSize());
                builder.field("actual_heap_size", ByteSizeValue.of(indexStats.actualHeapSize()));
                builder.field("actual_heap_size_in_bytes", indexStats.actualHeapSize());

                if (level.equalsIgnoreCase("shards")) {
                    builder.startArray("shards");
                    for (Map.Entry<Integer, List<PhantomShardStats>> shardsStats : indexStats.getStatsGroupedByShardId().entrySet()) {
                        builder.startObject();
                        builder.startArray(String.valueOf(shardsStats.getKey()));
                        for (PhantomShardStats shardStats : shardsStats.getValue()) {
                            shardStats.toXContent(builder, params);
                        }
                        builder.endArray();
                        builder.endObject();
                    }
                    builder.endArray();
                }

                builder.endObject();

                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shards = new PhantomShardStats[in.readVInt()];
        for (int i = 0; i < shards.length; i++) {
            shards[i] = PhantomShardStats.read(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shards.length);
        for (PhantomShardStats shard : shards) {
            shard.writeTo(out);
        }
    }
}
