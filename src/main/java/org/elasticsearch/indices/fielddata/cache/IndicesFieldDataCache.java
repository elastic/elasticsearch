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

package org.elasticsearch.indices.fielddata.cache;

import com.google.common.cache.*;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 */
public class IndicesFieldDataCache extends AbstractComponent implements RemovalListener<IndicesFieldDataCache.Key, AtomicFieldData> {

    Cache<Key, AtomicFieldData> cache;

    private volatile String size;
    private volatile long sizeInBytes;
    private volatile TimeValue expire;


    @Inject
    public IndicesFieldDataCache(Settings settings) {
        super(settings);
        this.size = componentSettings.get("size", "-1");
        this.expire = componentSettings.getAsTime("expire", null);
        computeSizeInBytes();
        buildCache();
    }

    private void buildCache() {
        CacheBuilder<Key, AtomicFieldData> cacheBuilder = CacheBuilder.newBuilder()
                .removalListener(this);
        if (sizeInBytes > 0) {
            cacheBuilder.maximumWeight(sizeInBytes).weigher(new FieldDataWeigher());
        }
        // defaults to 4, but this is a busy map for all indices, increase it a bit
        cacheBuilder.concurrencyLevel(16);
        if (expire != null && expire.millis() > 0) {
            cacheBuilder.expireAfterAccess(expire.millis(), TimeUnit.MILLISECONDS);
        }
        logger.debug("using size [{}] [{}], expire [{}]", size, new ByteSizeValue(sizeInBytes), expire);
        cache = cacheBuilder.build();
    }

    private void computeSizeInBytes() {
        if (size.equals("-1")) {
            sizeInBytes = -1;
        } else if (size.endsWith("%")) {
            double percent = Double.parseDouble(size.substring(0, size.length() - 1));
            sizeInBytes = (long) ((percent / 100) * JvmInfo.jvmInfo().getMem().getHeapMax().bytes());
        } else {
            sizeInBytes = ByteSizeValue.parseBytesSizeValue(size).bytes();
        }
    }

    public void close() {
        cache.invalidateAll();
    }

    public IndexFieldDataCache buildIndexFieldDataCache(@Nullable IndexService indexService, Index index, FieldMapper.Names fieldNames, FieldDataType fieldDataType) {
        return new IndexFieldCache(indexService, index, fieldNames, fieldDataType);
    }

    @Override
    public void onRemoval(RemovalNotification<Key, AtomicFieldData> notification) {
        Key key = notification.getKey();
        if (key == null || key.listener == null) {
            return; // nothing to do here really...
        }
        IndexFieldCache indexCache = key.indexCache;
        long sizeInBytes = key.sizeInBytes;
        AtomicFieldData value = notification.getValue();
        if (sizeInBytes == -1 && value != null) {
            sizeInBytes = value.getMemorySizeInBytes();
        }
        key.listener.onUnload(indexCache.fieldNames, indexCache.fieldDataType, notification.wasEvicted(), sizeInBytes, value);
    }

    public static class FieldDataWeigher implements Weigher<Key, AtomicFieldData> {

        @Override
        public int weigh(Key key, AtomicFieldData fieldData) {
            int weight = (int) Math.min(fieldData.getMemorySizeInBytes(), Integer.MAX_VALUE);
            return weight == 0 ? 1 : weight;
        }
    }

    /**
     * A specific cache instance for the relevant parameters of it (index, fieldNames, fieldType).
     */
    class IndexFieldCache implements IndexFieldDataCache, SegmentReader.CoreClosedListener {

        @Nullable
        private final IndexService indexService;
        final Index index;
        final FieldMapper.Names fieldNames;
        final FieldDataType fieldDataType;

        IndexFieldCache(@Nullable IndexService indexService, Index index, FieldMapper.Names fieldNames, FieldDataType fieldDataType) {
            this.indexService = indexService;
            this.index = index;
            this.fieldNames = fieldNames;
            this.fieldDataType = fieldDataType;
        }

        @Override
        public <FD extends AtomicFieldData, IFD extends IndexFieldData<FD>> FD load(final AtomicReaderContext context, final IFD indexFieldData) throws Exception {
            final Key key = new Key(this, context.reader().getCoreCacheKey());
            //noinspection unchecked
            return (FD) cache.get(key, new Callable<AtomicFieldData>() {
                @Override
                public AtomicFieldData call() throws Exception {
                    if (context.reader() instanceof SegmentReader) {
                        ((SegmentReader) context.reader()).addCoreClosedListener(IndexFieldCache.this);
                    }
                    AtomicFieldData fieldData = indexFieldData.loadDirect(context);

                    if (indexService != null) {
                        ShardId shardId = ShardUtils.extractShardId(context.reader());
                        if (shardId != null) {
                            IndexShard shard = indexService.shard(shardId.id());
                            if (shard != null) {
                                key.listener = shard.fieldData();
                            }
                        }
                    }

                    if (key.listener != null) {
                        key.listener.onLoad(fieldNames, fieldDataType, fieldData);
                    }

                    return fieldData;
                }
            });
        }

        @Override
        public void onClose(Object coreKey) {
            cache.invalidate(new Key(this, coreKey));
        }

        @Override
        public void clear() {
            for (Key key : cache.asMap().keySet()) {
                if (key.indexCache.index.equals(index)) {
                    cache.invalidate(key);
                }
            }
        }

        @Override
        public void clear(String fieldName) {
            for (Key key : cache.asMap().keySet()) {
                if (key.indexCache.index.equals(index)) {
                    if (key.indexCache.fieldNames.fullName().equals(fieldName)) {
                        cache.invalidate(key);
                    }
                }
            }
        }

        @Override
        public void clear(Object coreCacheKey) {
            cache.invalidate(new Key(this, coreCacheKey));
        }
    }

    public static class Key {
        public final IndexFieldCache indexCache;
        public final Object readerKey;

        @Nullable
        public IndexFieldDataCache.Listener listener; // optional stats listener
        long sizeInBytes = -1; // optional size in bytes (we keep it here in case the values are soft references)


        Key(IndexFieldCache indexCache, Object readerKey) {
            this.indexCache = indexCache;
            this.readerKey = readerKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            Key key = (Key) o;
            if (!indexCache.equals(key.indexCache)) return false;
            if (!readerKey.equals(key.readerKey)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = indexCache.hashCode();
            result = 31 * result + readerKey.hashCode();
            return result;
        }
    }
}
