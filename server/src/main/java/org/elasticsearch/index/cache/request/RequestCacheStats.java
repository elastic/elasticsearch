/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.cache.request;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class RequestCacheStats implements Writeable, ToXContentFragment {

    private long memorySize;
    private long evictions;
    private long hitCount;
    private long missCount;

    public RequestCacheStats() {
    }

    public RequestCacheStats(StreamInput in) throws IOException {
        memorySize = in.readVLong();
        evictions = in.readVLong();
        hitCount = in.readVLong();
        missCount = in.readVLong();
    }

    public RequestCacheStats(long memorySize, long evictions, long hitCount, long missCount) {
        this.memorySize = memorySize;
        this.evictions = evictions;
        this.hitCount = hitCount;
        this.missCount = missCount;
    }

    public void add(RequestCacheStats stats) {
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;
        this.hitCount += stats.hitCount;
        this.missCount += stats.missCount;
    }

    public long getMemorySizeInBytes() {
        return this.memorySize;
    }

    public ByteSizeValue getMemorySize() {
        return new ByteSizeValue(memorySize);
    }

    public long getEvictions() {
        return this.evictions;
    }

    public long getHitCount() {
        return this.hitCount;
    }

    public long getMissCount() {
        return this.missCount;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memorySize);
        out.writeVLong(evictions);
        out.writeVLong(hitCount);
        out.writeVLong(missCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.REQUEST_CACHE_STATS);
        builder.humanReadableField(Fields.MEMORY_SIZE_IN_BYTES, Fields.MEMORY_SIZE, getMemorySize());
        builder.field(Fields.EVICTIONS, getEvictions());
        builder.field(Fields.HIT_COUNT, getHitCount());
        builder.field(Fields.MISS_COUNT, getMissCount());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String REQUEST_CACHE_STATS = "request_cache";
        static final String MEMORY_SIZE = "memory_size";
        static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
        static final String EVICTIONS = "evictions";
        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
    }
}
