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

package org.elasticsearch.indices;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.shard.IndexShard;

import java.io.IOException;

/**
 * Abstract base class for the an {@link IndexShard} level {@linkplain IndicesRequestCache.CacheEntity}.
 */
abstract class AbstractIndexShardCacheEntity implements IndicesRequestCache.CacheEntity {
    @FunctionalInterface
    public interface Loader {
        void load(StreamOutput out) throws IOException;
    }

    private final Loader loader;
    private boolean loadedFromCache = true;

    protected AbstractIndexShardCacheEntity(Loader loader) {
        this.loader = loader;
    }

    /**
     * When called after passing this through
     * {@link IndicesRequestCache#getOrCompute(IndicesRequestCache.CacheEntity, DirectoryReader, BytesReference)} this will return whether
     * or not the result was loaded from the cache.
     */
    public final boolean loadedFromCache() {
        return loadedFromCache;
    }

    /**
     * Get the {@linkplain ShardRequestCache} used to track cache statistics.
     */
    protected abstract ShardRequestCache stats();

    @Override
    public final IndicesRequestCache.Value loadValue() throws IOException {
        /* BytesStreamOutput allows to pass the expected size but by default uses
         * BigArrays.PAGE_SIZE_IN_BYTES which is 16k. A common cached result ie.
         * a date histogram with 3 buckets is ~100byte so 16k might be very wasteful
         * since we don't shrink to the actual size once we are done serializing.
         * By passing 512 as the expected size we will resize the byte array in the stream
         * slowly until we hit the page size and don't waste too much memory for small query
         * results.*/
        final int expectedSizeInBytes = 512;
        try (BytesStreamOutput out = new BytesStreamOutput(expectedSizeInBytes)) {
            loader.load(out);
            // for now, keep the paged data structure, which might have unused bytes to fill a page, but better to keep
            // the memory properly paged instead of having varied sized bytes
            final BytesReference reference = out.bytes();
            loadedFromCache = false;
            return new IndicesRequestCache.Value(reference, out.ramBytesUsed());
        }
    }

    @Override
    public final void onCached(IndicesRequestCache.Key key, IndicesRequestCache.Value value) {
        stats().onCached(key, value);
    }

    @Override
    public final void onHit() {
        stats().onHit();
    }

    @Override
    public final void onMiss() {
        stats().onMiss();
    }

    @Override
    public final void onRemoval(RemovalNotification<IndicesRequestCache.Key, IndicesRequestCache.Value> notification) {
        stats().onRemoval(notification.getKey(), notification.getValue(),
                notification.getRemovalReason() == RemovalNotification.RemovalReason.EVICTED);
    }
}
