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

package org.elasticsearch.transport;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.transport.netty4.Netty4Utils;

public class Netty4PluginConfig {

    private static final ByteBufAllocator NO_DIRECT_ALLOCATOR = noDirectAllocator();

    private final ByteBufAllocator allocator;
    private final Settings settings;
    private final Boolean directBufferPoolingDisabled;

    public Netty4PluginConfig(Settings settings) {
        Netty4Utils.setAvailableProcessors(EsExecutors.PROCESSORS_SETTING.get(settings));
        this.settings = settings;
        directBufferPoolingDisabled = Netty4Plugin.NETTY_DISABLE_DIRECT_POOL.get(settings);
        if (directBufferPoolingDisabled) {
            allocator = NO_DIRECT_ALLOCATOR;
        } else {
            allocator = ByteBufAllocator.DEFAULT;
        }
    }

    public Settings getSettings() {
        return settings;
    }

    public boolean isDirectBufferPoolingDisabled() {
        return directBufferPoolingDisabled;
    }

    public ByteBufAllocator getAllocator() {
        return allocator;
    }

    private static ByteBufAllocator noDirectAllocator() {
        return new PooledByteBufAllocator(false, PooledByteBufAllocator.defaultNumHeapArena(), 0,
            PooledByteBufAllocator.defaultPageSize(), PooledByteBufAllocator.defaultMaxOrder(),
            PooledByteBufAllocator.defaultTinyCacheSize(), PooledByteBufAllocator.defaultSmallCacheSize(),
            PooledByteBufAllocator.defaultNormalCacheSize(), PooledByteBufAllocator.defaultUseCacheForAllThreads());
    }
}
