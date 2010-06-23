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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class AbstractConcurrentMapFieldDataCache extends AbstractIndexComponent implements FieldDataCache {

    private final ConcurrentMap<Object, ConcurrentMap<String, FieldData>> cache;

    private final Object creationMutex = new Object();

    protected AbstractConcurrentMapFieldDataCache(Index index, @IndexSettings Settings indexSettings,
                                                  ConcurrentMap<Object, ConcurrentMap<String, FieldData>> cache) {
        super(index, indexSettings);
        this.cache = cache;
    }

    @Override public void close() throws ElasticSearchException {
        cache.clear();
    }

    @Override public void clear() {
        cache.clear();
    }

    @Override public void clear(IndexReader reader) {
        cache.remove(reader.getFieldCacheKey());
    }

    @Override public void clearUnreferenced() {
        // nothing to do here...
    }

    @Override public FieldData cache(FieldData.Type type, IndexReader reader, String fieldName) throws IOException {
        return cache(type.fieldDataClass, reader, fieldName);
    }

    @Override public <T extends FieldData> T cache(Class<T> type, IndexReader reader, String fieldName) throws IOException {
        ConcurrentMap<String, FieldData> fieldDataCache = cache.get(reader.getFieldCacheKey());
        if (fieldDataCache == null) {
            synchronized (creationMutex) {
                fieldDataCache = cache.get(reader.getFieldCacheKey());
                if (fieldDataCache == null) {
                    fieldDataCache = ConcurrentCollections.newConcurrentMap();
                    cache.put(reader.getFieldCacheKey(), fieldDataCache);
                }
                T fieldData = (T) fieldDataCache.get(fieldName);
                if (fieldData == null) {
                    fieldData = FieldData.load(type, reader, fieldName);
                    fieldDataCache.put(fieldName, fieldData);
                }
                return fieldData;
            }
        }
        T fieldData = (T) fieldDataCache.get(fieldName);
        if (fieldData == null) {
            synchronized (creationMutex) {
                fieldData = (T) fieldDataCache.get(fieldName);
                if (fieldData == null) {
                    fieldData = FieldData.load(type, reader, fieldName);
                    fieldDataCache.put(fieldName, fieldData);
                }
                return fieldData;
            }
        }
        return fieldData;
    }
}
