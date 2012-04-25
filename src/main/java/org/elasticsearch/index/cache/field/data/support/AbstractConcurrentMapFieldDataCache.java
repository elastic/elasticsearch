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

package org.elasticsearch.index.cache.field.data.support;

import com.google.common.cache.Cache;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public abstract class AbstractConcurrentMapFieldDataCache extends AbstractIndexComponent implements FieldDataCache, SegmentReader.CoreClosedListener {

    private final ConcurrentMap<Object, Cache<String, FieldData>> cache;

    private final Object creationMutex = new Object();

    protected AbstractConcurrentMapFieldDataCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        this.cache = ConcurrentCollections.newConcurrentMap();
    }

    @Override
    public void close() throws ElasticSearchException {
        clear();
    }

    @Override
    public void clear(String fieldName) {
        for (Map.Entry<Object, Cache<String, FieldData>> entry : cache.entrySet()) {
            entry.getValue().invalidate(fieldName);
        }
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void onClose(SegmentReader owner) {
        clear(owner);
    }

    @Override
    public void clear(IndexReader reader) {
        cache.remove(reader.getCoreCacheKey());
    }

    @Override
    public long sizeInBytes() {
        // the overhead of the map is not really relevant...
        long sizeInBytes = 0;
        for (Cache<String, FieldData> map : cache.values()) {
            for (FieldData fieldData : map.asMap().values()) {
                sizeInBytes += fieldData.sizeInBytes();
            }
        }
        return sizeInBytes;
    }

    @Override
    public long sizeInBytes(String fieldName) {
        long sizeInBytes = 0;
        for (Cache<String, FieldData> map : cache.values()) {
            FieldData fieldData = map.getIfPresent(fieldName);
            if (fieldData != null) {
                sizeInBytes += fieldData.sizeInBytes();
            }
        }
        return sizeInBytes;
    }

    @Override
    public FieldData cache(FieldDataType type, IndexReader reader, String fieldName) throws IOException {
        Cache<String, FieldData> fieldDataCache = cache.get(reader.getCoreCacheKey());
        if (fieldDataCache == null) {
            synchronized (creationMutex) {
                fieldDataCache = cache.get(reader.getCoreCacheKey());
                if (fieldDataCache == null) {
                    fieldDataCache = buildFieldDataMap();
                    ((SegmentReader) reader).addCoreClosedListener(this);
                    cache.put(reader.getCoreCacheKey(), fieldDataCache);
                }
            }
        }
        FieldData fieldData = fieldDataCache.getIfPresent(fieldName);
        if (fieldData == null) {
            synchronized (fieldDataCache) {
                fieldData = fieldDataCache.getIfPresent(fieldName);
                if (fieldData == null) {
                    try {
                        fieldData = FieldData.load(type, reader, fieldName);
                        fieldDataCache.put(fieldName, fieldData);
                    } catch (OutOfMemoryError e) {
                        logger.warn("loading field [" + fieldName + "] caused out of memory failure", e);
                        final OutOfMemoryError outOfMemoryError = new OutOfMemoryError("loading field [" + fieldName + "] caused out of memory failure");
                        outOfMemoryError.initCause(e);
                        throw outOfMemoryError;
                    }
                }
            }
        }
        return fieldData;
    }

    protected abstract Cache<String, FieldData> buildFieldDataMap();
}
