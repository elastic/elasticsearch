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

package org.elasticsearch.index.cache.query;

import org.apache.lucene.search.DocIdSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class QueryCacheStats implements Streamable, ToXContentFragment {

    long ramBytesUsed;
    long hitCount;
    long missCount;
    long cacheCount;
    long cacheSize;

    public QueryCacheStats() {
    }

    public QueryCacheStats(long ramBytesUsed, long hitCount, long missCount, long cacheCount, long cacheSize) {
        this.ramBytesUsed = ramBytesUsed;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.cacheCount = cacheCount;
        this.cacheSize = cacheSize;
    }

    public void add(QueryCacheStats stats) {
        ramBytesUsed += stats.ramBytesUsed;
        hitCount += stats.hitCount;
        missCount += stats.missCount;
        cacheCount += stats.cacheCount;
        cacheSize += stats.cacheSize;
    }

    public long getMemorySizeInBytes() {
        return ramBytesUsed;
    }

    public ByteSizeValue getMemorySize() {
        return new ByteSizeValue(ramBytesUsed);
    }

    /**
     * The total number of lookups in the cache.
     */
    public long getTotalCount() {
        return hitCount + missCount;
    }

    /**
     * The number of successful lookups in the cache.
     */
    public long getHitCount() {
        return hitCount;
    }

    /**
     * The number of lookups in the cache that failed to retrieve a {@link DocIdSet}.
     */
    public long getMissCount() {
        return missCount;
    }

    /**
     * The number of {@link DocIdSet}s that have been cached.
     */
    public long getCacheCount() {
        return cacheCount;
    }

    /**
     * The number of {@link DocIdSet}s that are in the cache.
     */
    public long getCacheSize() {
        return cacheSize;
    }

    /**
     * The number of {@link DocIdSet}s that have been evicted from the cache.
     */
    public long getEvictions() {
        return cacheCount - cacheSize;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        ramBytesUsed = in.readLong();
        hitCount = in.readLong();
        missCount = in.readLong();
        cacheCount = in.readLong();
        cacheSize = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(ramBytesUsed);
        out.writeLong(hitCount);
        out.writeLong(missCount);
        out.writeLong(cacheCount);
        out.writeLong(cacheSize);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.QUERY_CACHE);
        builder.byteSizeField(Fields.MEMORY_SIZE_IN_BYTES, Fields.MEMORY_SIZE, ramBytesUsed);
        builder.field(Fields.TOTAL_COUNT, getTotalCount());
        builder.field(Fields.HIT_COUNT, getHitCount());
        builder.field(Fields.MISS_COUNT, getMissCount());
        builder.field(Fields.CACHE_SIZE, getCacheSize());
        builder.field(Fields.CACHE_COUNT, getCacheCount());
        builder.field(Fields.EVICTIONS, getEvictions());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String QUERY_CACHE = "query_cache";
        static final String MEMORY_SIZE = "memory_size";
        static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
        static final String TOTAL_COUNT = "total_count";
        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
        static final String CACHE_SIZE = "cache_size";
        static final String CACHE_COUNT = "cache_count";
        static final String EVICTIONS = "evictions";
    }

}
