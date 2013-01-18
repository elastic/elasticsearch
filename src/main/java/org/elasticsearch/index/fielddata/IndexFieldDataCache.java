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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.index.Index;

import java.util.concurrent.Callable;

/**
 * A simple field data cache abstraction.
 */
public interface IndexFieldDataCache {

    <FD extends AtomicFieldData, IFD extends IndexFieldData<FD>> FD load(AtomicReaderContext context, IFD indexFieldData) throws Exception;

    void clear(Index index);

    void clear(Index index, String fieldName);

    /**
     * The resident field data cache is a *per field* cache that keeps all the values in memory.
     */
    static abstract class FieldBased implements IndexFieldDataCache, SegmentReader.CoreClosedListener {
        private final Cache<Object, AtomicFieldData> cache;

        protected FieldBased(Cache<Object, AtomicFieldData> cache) {
            this.cache = cache;
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
                    return indexFieldData.loadDirect(context);
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
    }

    static class Resident extends FieldBased {

        public Resident() {
            super(CacheBuilder.newBuilder().<Object, AtomicFieldData>build());
        }
    }

    static class Soft extends FieldBased {

        public Soft() {
            super(CacheBuilder.newBuilder().softValues().<Object, AtomicFieldData>build());
        }
    }
}
