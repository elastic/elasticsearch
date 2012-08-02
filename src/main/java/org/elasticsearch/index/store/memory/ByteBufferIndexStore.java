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

package org.elasticsearch.index.store.memory;

import org.elasticsearch.cache.memory.ByteBufferCache;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.support.AbstractIndexStore;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.jvm.JvmStats;

/**
 *
 */
public class ByteBufferIndexStore extends AbstractIndexStore {

    private final boolean direct;

    @Inject
    public ByteBufferIndexStore(Index index, @IndexSettings Settings indexSettings, IndexService indexService,
                                ByteBufferCache byteBufferCache, IndicesStore indicesStore) {
        super(index, indexSettings, indexService, indicesStore);
        this.direct = byteBufferCache.direct();
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public Class<? extends DirectoryService> shardDirectory() {
        return ByteBufferDirectoryService.class;
    }

    @Override
    public ByteSizeValue backingStoreTotalSpace() {
        if (direct) {
            // TODO, we can use sigar...
            return new ByteSizeValue(-1, ByteSizeUnit.BYTES);
        }
        return JvmInfo.jvmInfo().mem().heapMax();
    }

    @Override
    public ByteSizeValue backingStoreFreeSpace() {
        if (direct) {
            return new ByteSizeValue(-1, ByteSizeUnit.BYTES);
        }
        return JvmStats.jvmStats().mem().heapUsed();
    }
}