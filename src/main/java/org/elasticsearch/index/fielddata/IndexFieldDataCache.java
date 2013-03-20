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

package org.elasticsearch.index.fielddata;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldMapper;

import java.util.concurrent.Callable;

/**
 * A simple field data cache abstraction.
 */
public interface IndexFieldDataCache {

    <FD extends AtomicFieldData, IFD extends IndexFieldData<FD>> FD load(AtomicReaderContext context, IFD indexFieldData) throws Exception;

    void clear(Index index);

    void clear(Index index, String fieldName);

    void clear(Index index, IndexReader reader);

    interface Listener {

        void onLoad(Index index, FieldMapper.Names fieldNames, FieldDataType fieldDataType, AtomicFieldData fieldData);

        void onUnload(Index index, FieldMapper.Names fieldNames, FieldDataType fieldDataType, boolean wasEvicted, @Nullable AtomicFieldData fieldData);
    }

    /**
     * The resident field data cache is a *per field* cache that keeps all the values in memory.
     */
    static abstract class FieldBased implements IndexFieldDataCache, SegmentReader.CoreClosedListener, RemovalListener<Object, AtomicFieldData> {
        private final Index index;
        private final FieldMapper.Names fieldNames;
        private final FieldDataType fieldDataType;
        private final Listener listener;
        private final Cache<Object, AtomicFieldData> cache;

        protected FieldBased(Index index, FieldMapper.Names fieldNames, FieldDataType fieldDataType, Listener listener, CacheBuilder cache) {
            this.index = index;
            this.fieldNames = fieldNames;
            this.fieldDataType = fieldDataType;
            this.listener = listener;
            cache.removalListener(this);
            this.cache = cache.build();
        }

        @Override
        public void onRemoval(RemovalNotification<Object, AtomicFieldData> notification) {
            listener.onUnload(index, fieldNames, fieldDataType, notification.wasEvicted(), notification.getValue());
        }

        @Override
        public void onClose(SegmentReader owner) {
            cache.invalidate(owner.getCoreCacheKey());
        }

        @Override
        public <FD extends AtomicFieldData, IFD extends IndexFieldData<FD>> FD load(final AtomicReaderContext context, final IFD indexFieldData) throws Exception {
            //noinspection unchecked
            return (FD) cache.get(context.reader().getCoreCacheKey(), new Callable<AtomicFieldData>() {
                @Override
                public AtomicFieldData call() throws Exception {
                    if (context.reader() instanceof SegmentReader) {
                        ((SegmentReader) context.reader()).addCoreClosedListener(FieldBased.this);
                    }
                    AtomicFieldData fieldData = indexFieldData.loadDirect(context);
                    listener.onLoad(index, fieldNames, fieldDataType, fieldData);
                    return fieldData;
                }
            });
        }

        @Override
        public void clear(Index index) {
            cache.invalidateAll();
        }

        @Override
        public void clear(Index index, String fieldName) {
            cache.invalidateAll();
        }

        @Override
        public void clear(Index index, IndexReader reader) {
            cache.invalidate(reader.getCoreCacheKey());
        }
    }

    static class Resident extends FieldBased {

        public Resident(Index index, FieldMapper.Names fieldNames, FieldDataType fieldDataType, Listener listener) {
            super(index, fieldNames, fieldDataType, listener, CacheBuilder.newBuilder());
        }
    }

    static class Soft extends FieldBased {

        public Soft(Index index, FieldMapper.Names fieldNames, FieldDataType fieldDataType, Listener listener) {
            super(index, fieldNames, fieldDataType, listener, CacheBuilder.newBuilder().softValues());
        }
    }
}
