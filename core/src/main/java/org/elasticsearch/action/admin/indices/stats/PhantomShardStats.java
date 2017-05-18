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

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.engine.phantom.PhantomEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Statistics about usage of shards with phantom engines.
 */
public class PhantomShardStats implements Streamable, ToXContent {

    private long segmentCount;
    private long heapSizeInBytes;
    private boolean hasActiveSearches;
    private boolean loadState;
    private long loadTimestamp;
    private long loadCount;
    private long loadTime;
    private long unloadTime;
    private long searchCompletionAwaitTime;
    private long hits;
    private long totalHits;
    private ShardRouting shardRouting;

    PhantomShardStats() {
    }

    public PhantomShardStats(IndexShard indexShard) {
        PhantomEngine engine = (PhantomEngine) indexShard.engine();
        segmentCount = engine.segmentCount();
        heapSizeInBytes = engine.usedHeapSizeInBytes();
        hasActiveSearches = engine.hasActiveSearches();
        loadState = engine.loadState();
        loadTimestamp = engine.loadTimestamp();
        loadCount = engine.loadCount();
        loadTime = engine.loadTime();
        unloadTime = engine.unloadTime();
        searchCompletionAwaitTime = engine.searchCompletionAwaitTime();
        hits = engine.hits();
        totalHits = engine.totalHits();
        shardRouting = indexShard.routingEntry();
    }

    public long heapSizeInBytes() {
        return heapSizeInBytes;
    }

    public boolean isLoaded() {
        return loadState;
    }

    public ShardId shardId() {
        return shardRouting.shardId();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        segmentCount = in.readLong();
        heapSizeInBytes = in.readLong();
        hasActiveSearches = in.readBoolean();
        loadState = in.readBoolean();
        loadTimestamp = in.readLong();
        loadCount = in.readLong();
        loadTime = in.readLong();
        unloadTime = in.readLong();
        searchCompletionAwaitTime = in.readLong();
        hits = in.readLong();
        totalHits = in.readLong();
        shardRouting = ShardRouting.readShardRoutingEntry(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(segmentCount);
        out.writeLong(heapSizeInBytes);
        out.writeBoolean(hasActiveSearches);
        out.writeBoolean(loadState);
        out.writeLong(loadTimestamp);
        out.writeLong(loadCount);
        out.writeLong(loadTime);
        out.writeLong(unloadTime);
        out.writeLong(searchCompletionAwaitTime);
        out.writeLong(hits);
        out.writeLong(totalHits);
        shardRouting.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject(ShardStats.Fields.ROUTING)
            .field("state", shardRouting.state())
            .field("primary", shardRouting.primary())
            .field("node", shardRouting.currentNodeId())
            .field("relocating_node", shardRouting.relocatingNodeId())
            .endObject();

        builder.field("segment_count", segmentCount)
            .field("heap_size", ByteSizeValue.of(heapSizeInBytes))
            .field("heap_size_in_bytes", heapSizeInBytes)
            .field("has_active_searches", hasActiveSearches)
            .field("loaded", loadState)
            .field("last_load_timestamp", loadTimestamp)
            .field("load_count", loadCount)
            .field("load_time", TimeValue.timeValueMillis(loadTime))
            .field("load_time_in_millis", loadTime)
            .field("unload_time", TimeValue.timeValueMillis(unloadTime))
            .field("unload_time_in_millis", unloadTime)
            .field("search_completion_await_time", TimeValue.timeValueMillis(searchCompletionAwaitTime))
            .field("search_completion_await_time_in_millis", searchCompletionAwaitTime)
            .field("current_search_hits", hits)
            .field("total_search_hits", totalHits);

        builder.endObject();

        return builder;
    }

    public static PhantomShardStats read(StreamInput in) throws IOException {
        PhantomShardStats stats = new PhantomShardStats();
        stats.readFrom(in);
        return stats;
    }
}
