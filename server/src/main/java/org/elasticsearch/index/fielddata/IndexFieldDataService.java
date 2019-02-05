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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexFieldDataService extends AbstractIndexComponent implements Closeable {
    public static final String FIELDDATA_CACHE_VALUE_NODE = "node";
    public static final String FIELDDATA_CACHE_KEY = "index.fielddata.cache";
    public static final Setting<String> INDEX_FIELDDATA_CACHE_KEY =
        new Setting<>(FIELDDATA_CACHE_KEY, (s) -> FIELDDATA_CACHE_VALUE_NODE, (s) -> {
            switch (s) {
                case "node":
                case "none":
                    return s;
                default:
                    throw new IllegalArgumentException("failed to parse [" + s + "] must be one of [node,none]");
            }
        }, Property.IndexScope);

    private final CircuitBreakerService circuitBreakerService;

    private final IndicesFieldDataCache indicesFieldDataCache;
    // the below map needs to be modified under a lock
    private final Map<String, IndexFieldDataCache> fieldDataCaches = new HashMap<>();
    private final MapperService mapperService;
    private static final IndexFieldDataCache.Listener DEFAULT_NOOP_LISTENER = new IndexFieldDataCache.Listener() {
        @Override
        public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
        }

        @Override
        public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
        }
    };
    private volatile IndexFieldDataCache.Listener listener = DEFAULT_NOOP_LISTENER;


    public IndexFieldDataService(IndexSettings indexSettings, IndicesFieldDataCache indicesFieldDataCache,
                                 CircuitBreakerService circuitBreakerService, MapperService mapperService) {
        super(indexSettings);
        this.indicesFieldDataCache = indicesFieldDataCache;
        this.circuitBreakerService = circuitBreakerService;
        this.mapperService = mapperService;
    }

    public synchronized void clear() {
        List<Exception> exceptions = new ArrayList<>(0);
        final Collection<IndexFieldDataCache> fieldDataCacheValues = fieldDataCaches.values();
        for (IndexFieldDataCache cache : fieldDataCacheValues) {
            try {
                cache.clear();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        fieldDataCacheValues.clear();
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }

    public synchronized void clearField(final String fieldName) {
        List<Exception> exceptions = new ArrayList<>(0);
        final IndexFieldDataCache cache = fieldDataCaches.remove(fieldName);
        if (cache != null) {
            try {
                cache.clear(fieldName);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType) {
        return getForField(fieldType, index().getName());
    }

    @SuppressWarnings("unchecked")
    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType, String fullyQualifiedIndexName) {
        final String fieldName = fieldType.name();
        IndexFieldData.Builder builder = fieldType.fielddataBuilder(fullyQualifiedIndexName);

        IndexFieldDataCache cache;
        synchronized (this) {
            cache = fieldDataCaches.get(fieldName);
            if (cache == null) {
                String cacheType = indexSettings.getValue(INDEX_FIELDDATA_CACHE_KEY);
                if (FIELDDATA_CACHE_VALUE_NODE.equals(cacheType)) {
                    cache = indicesFieldDataCache.buildIndexFieldDataCache(listener, index(), fieldName);
                } else if ("none".equals(cacheType)){
                    cache = new IndexFieldDataCache.None();
                } else {
                    throw new IllegalArgumentException("cache type not supported [" + cacheType + "] for field [" + fieldName + "]");
                }
                fieldDataCaches.put(fieldName, cache);
            }
        }

        return (IFD) builder.build(indexSettings, fieldType, cache, circuitBreakerService, mapperService);
    }

    /**
     * Sets a {@link org.elasticsearch.index.fielddata.IndexFieldDataCache.Listener} passed to each {@link IndexFieldData}
     * creation to capture onCache and onRemoval events. Setting a listener on this method will override any previously
     * set listeners.
     * @throws IllegalStateException if the listener is set more than once
     */
    public void setListener(IndexFieldDataCache.Listener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (this.listener != DEFAULT_NOOP_LISTENER) {
            throw new IllegalStateException("can't set listener more than once");
        }
        this.listener = listener;
    }

    @Override
    public void close() throws IOException {
        clear();
    }
}
