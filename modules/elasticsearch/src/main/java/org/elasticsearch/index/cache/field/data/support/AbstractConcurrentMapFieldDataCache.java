/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.cache.field.data.support;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.MapMaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * @author kimchy (shay.banon)
 */
public abstract class AbstractConcurrentMapFieldDataCache extends AbstractIndexComponent implements FieldDataCache, IndexReader.ReaderFinishedListener {

    private final ConcurrentMap<Object, ConcurrentMap<String, FieldData>> cache;

    private final Object creationMutex = new Object();

    protected AbstractConcurrentMapFieldDataCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        // weak keys is fine, it will only be cleared once IndexReader references will be removed
        // (assuming clear(...) will not be called)
        this.cache = new MapMaker().weakKeys().makeMap();
    }

    @Override public void close() throws ElasticSearchException {
        clear();
    }

    @Override public void clear() {
        cache.clear();
    }

    @Override public void finished(IndexReader reader) {
        clear(reader);
    }

    @Override public void clear(IndexReader reader) {
        ConcurrentMap<String, FieldData> map = cache.remove(reader.getCoreCacheKey());
        // help soft/weak handling GC
        if (map != null) {
            map.clear();
        }
    }

    @Override public long sizeInBytes() {
        // the overhead of the map is not really relevant...
        long sizeInBytes = 0;
        for (ConcurrentMap<String, FieldData> map : cache.values()) {
            for (FieldData fieldData : map.values()) {
                sizeInBytes += fieldData.sizeInBytes();
            }
        }
        return sizeInBytes;
    }

    @Override public long sizeInBytes(String fieldName) {
        long sizeInBytes = 0;
        for (ConcurrentMap<String, FieldData> map : cache.values()) {
            FieldData fieldData = map.get(fieldName);
            if (fieldData != null) {
                sizeInBytes += fieldData.sizeInBytes();
            }
        }
        return sizeInBytes;
    }

    @Override public FieldData cache(FieldDataType type, IndexReader reader, String fieldName) throws IOException {
        ConcurrentMap<String, FieldData> fieldDataCache = cache.get(reader.getCoreCacheKey());
        if (fieldDataCache == null) {
            synchronized (creationMutex) {
                fieldDataCache = cache.get(reader.getCoreCacheKey());
                if (fieldDataCache == null) {
                    fieldDataCache = buildFieldDataMap();
                    reader.addReaderFinishedListener(this);
                    cache.put(reader.getCoreCacheKey(), fieldDataCache);
                }
            }
        }
        FieldData fieldData = fieldDataCache.get(fieldName);
        if (fieldData == null) {
            synchronized (fieldDataCache) {
                fieldData = fieldDataCache.get(fieldName);
                if (fieldData == null) {
                    fieldData = FieldData.load(type, reader, fieldName);
                    fieldDataCache.put(fieldName, fieldData);
                }
            }
        }
        return fieldData;
    }

    protected ConcurrentMap<String, FieldData> buildFieldDataMap() {
        return ConcurrentCollections.newConcurrentMap();
    }
}
