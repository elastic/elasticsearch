/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cache.memory;

import org.apache.lucene.store.bytebuffer.ByteBufferAllocator;
import org.apache.lucene.store.bytebuffer.CachingByteBufferAllocator;
import org.apache.lucene.store.bytebuffer.PlainByteBufferAllocator;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public class ByteBufferCache extends AbstractComponent implements ByteBufferAllocator {

    private final boolean direct;

    private final ByteSizeValue smallBufferSize;
    private final ByteSizeValue largeBufferSize;

    private final ByteSizeValue smallCacheSize;
    private final ByteSizeValue largeCacheSize;

    private final ByteBufferAllocator allocator;

    public ByteBufferCache() {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    // really, for testing...
    public ByteBufferCache(int bufferSizeInBytes, int cacheSizeInBytes, boolean direct) {
        this(ImmutableSettings.settingsBuilder()
                .put("cache.memory.small_buffer_size", bufferSizeInBytes)
                .put("cache.memory.small_cache_size", cacheSizeInBytes)
                .put("cache.memory.large_buffer_size", bufferSizeInBytes)
                .put("cache.memory.large_cache_size", cacheSizeInBytes)
                .put("cache.memory.direct", direct).build());
    }

    @Inject
    public ByteBufferCache(Settings settings) {
        super(settings);

        this.direct = componentSettings.getAsBoolean("direct", true);
        this.smallBufferSize = componentSettings.getAsBytesSize("small_buffer_size", new ByteSizeValue(1, ByteSizeUnit.KB));
        this.largeBufferSize = componentSettings.getAsBytesSize("large_buffer_size", new ByteSizeValue(1, ByteSizeUnit.MB));
        this.smallCacheSize = componentSettings.getAsBytesSize("small_cache_size", new ByteSizeValue(10, ByteSizeUnit.MB));
        this.largeCacheSize = componentSettings.getAsBytesSize("large_cache_size", new ByteSizeValue(500, ByteSizeUnit.MB));

        if (smallCacheSize.bytes() == 0 || largeCacheSize.bytes() == 0) {
            this.allocator = new PlainByteBufferAllocator(direct, (int) smallBufferSize.bytes(), (int) largeBufferSize.bytes());
        } else {
            this.allocator = new CachingByteBufferAllocator(direct, (int) smallBufferSize.bytes(), (int) largeBufferSize.bytes(), (int) smallCacheSize.bytes(), (int) largeCacheSize.bytes());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("using bytebuffer cache with small_buffer_size [{}], large_buffer_size [{}], small_cache_size [{}], large_cache_size [{}], direct [{}]",
                    smallBufferSize, largeBufferSize, smallCacheSize, largeCacheSize, direct);
        }
    }

    public boolean direct() {
        return this.direct;
    }

    public void close() {
        allocator.close();
    }

    @Override
    public int sizeInBytes(Type type) {
        return allocator.sizeInBytes(type);
    }

    @Override
    public ByteBuffer allocate(Type type) throws IOException {
        return allocator.allocate(type);
    }

    @Override
    public void release(ByteBuffer buffer) {
        allocator.release(buffer);
    }
}
