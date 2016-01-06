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
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.plain.AbstractGeoPointDVIndexFieldData;
import org.elasticsearch.index.fielddata.plain.BytesBinaryDVIndexFieldData;
import org.elasticsearch.index.fielddata.plain.DisabledIndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.GeoPointArrayIndexFieldData;
import org.elasticsearch.index.fielddata.plain.IndexIndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
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

import static java.util.Collections.unmodifiableMap;

/**
 */
public class IndexFieldDataService extends AbstractIndexComponent implements Closeable {

    public static final String FIELDDATA_CACHE_KEY = "index.fielddata.cache";
    public static final String FIELDDATA_CACHE_VALUE_NODE = "node";

    private static final IndexFieldData.Builder MISSING_DOC_VALUES_BUILDER = (indexProperties, fieldType, cache, breakerService, mapperService1) -> {
        throw new IllegalStateException("Can't load fielddata on [" + fieldType.name()
                + "] of index [" + indexProperties.getIndex().getName() + "] because fielddata is unsupported on fields of type ["
                + fieldType.fieldDataType().getType() + "]. Use doc values instead.");
    };

    private static final String ARRAY_FORMAT = "array";
    private static final String DISABLED_FORMAT = "disabled";
    private static final String DOC_VALUES_FORMAT = "doc_values";
    private static final String PAGED_BYTES_FORMAT = "paged_bytes";

    private final static Map<String, IndexFieldData.Builder> buildersByType;
    private final static Map<String, IndexFieldData.Builder> docValuesBuildersByType;
    private final static Map<Tuple<String, String>, IndexFieldData.Builder> buildersByTypeAndFormat;
    private final CircuitBreakerService circuitBreakerService;

    static {
        Map<String, IndexFieldData.Builder> buildersByTypeBuilder = new HashMap<>();
        buildersByTypeBuilder.put("string", new PagedBytesIndexFieldData.Builder());
        buildersByTypeBuilder.put("float", MISSING_DOC_VALUES_BUILDER);
        buildersByTypeBuilder.put("double", MISSING_DOC_VALUES_BUILDER);
        buildersByTypeBuilder.put("byte", MISSING_DOC_VALUES_BUILDER);
        buildersByTypeBuilder.put("short", MISSING_DOC_VALUES_BUILDER);
        buildersByTypeBuilder.put("int", MISSING_DOC_VALUES_BUILDER);
        buildersByTypeBuilder.put("long", MISSING_DOC_VALUES_BUILDER);
        buildersByTypeBuilder.put("geo_point",  new GeoPointArrayIndexFieldData.Builder());
        buildersByTypeBuilder.put(ParentFieldMapper.NAME, new ParentChildIndexFieldData.Builder());
        buildersByTypeBuilder.put(IndexFieldMapper.NAME, new IndexIndexFieldData.Builder());
        buildersByTypeBuilder.put("binary", new DisabledIndexFieldData.Builder());
        buildersByTypeBuilder.put(BooleanFieldMapper.CONTENT_TYPE, MISSING_DOC_VALUES_BUILDER);
        buildersByType = unmodifiableMap(buildersByTypeBuilder);


        docValuesBuildersByType = MapBuilder.<String, IndexFieldData.Builder>newMapBuilder()
                .put("string", new DocValuesIndexFieldData.Builder())
                .put("float", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.FLOAT))
                .put("double", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.DOUBLE))
                .put("byte", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.BYTE))
                .put("short", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.SHORT))
                .put("int", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.INT))
                .put("long", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.LONG))
                .put("geo_point", new AbstractGeoPointDVIndexFieldData.Builder())
                .put("binary", new BytesBinaryDVIndexFieldData.Builder())
                .put(BooleanFieldMapper.CONTENT_TYPE, new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.BOOLEAN))
                .immutableMap();

        buildersByTypeAndFormat = MapBuilder.<Tuple<String, String>, IndexFieldData.Builder>newMapBuilder()
                .put(Tuple.tuple("string", PAGED_BYTES_FORMAT), new PagedBytesIndexFieldData.Builder())
                .put(Tuple.tuple("string", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder())
                .put(Tuple.tuple("string", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("float", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.FLOAT))
                .put(Tuple.tuple("float", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("double", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.DOUBLE))
                .put(Tuple.tuple("double", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("byte", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.BYTE))
                .put(Tuple.tuple("byte", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("short", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.SHORT))
                .put(Tuple.tuple("short", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("int", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.INT))
                .put(Tuple.tuple("int", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("long", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.LONG))
                .put(Tuple.tuple("long", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("geo_point", ARRAY_FORMAT), new GeoPointArrayIndexFieldData.Builder())
                .put(Tuple.tuple("geo_point", DOC_VALUES_FORMAT), new AbstractGeoPointDVIndexFieldData.Builder())
                .put(Tuple.tuple("geo_point", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("binary", DOC_VALUES_FORMAT), new BytesBinaryDVIndexFieldData.Builder())
                .put(Tuple.tuple("binary", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple(BooleanFieldMapper.CONTENT_TYPE, DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.BOOLEAN))
                .put(Tuple.tuple(BooleanFieldMapper.CONTENT_TYPE, DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .immutableMap();
    }

    private final IndicesFieldDataCache indicesFieldDataCache;
    // the below map needs to be modified under a lock
    private final Map<String, IndexFieldDataCache> fieldDataCaches = new HashMap<>();
    private final MapperService mapperService;
    private static final IndexFieldDataCache.Listener DEFAULT_NOOP_LISTENER = new IndexFieldDataCache.Listener() {
        @Override
        public void onCache(ShardId shardId, String fieldName, FieldDataType fieldDataType, Accountable ramUsage) {
        }

        @Override
        public void onRemoval(ShardId shardId, String fieldName, FieldDataType fieldDataType, boolean wasEvicted, long sizeInBytes) {
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
        List<Throwable> exceptions = new ArrayList<>(0);
        final Collection<IndexFieldDataCache> fieldDataCacheValues = fieldDataCaches.values();
        for (IndexFieldDataCache cache : fieldDataCacheValues) {
            try {
                cache.clear();
            } catch (Throwable t) {
                exceptions.add(t);
            }
        }
        fieldDataCacheValues.clear();
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }

    public synchronized void clearField(final String fieldName) {
        List<Throwable> exceptions = new ArrayList<>(0);
        final IndexFieldDataCache cache = fieldDataCaches.remove(fieldName);
        if (cache != null) {
            try {
                cache.clear();
            } catch (Throwable t) {
                exceptions.add(t);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }

    @SuppressWarnings("unchecked")
    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType) {
        final String fieldName = fieldType.name();
        final FieldDataType type = fieldType.fieldDataType();
        if (type == null) {
            throw new IllegalArgumentException("found no fielddata type for field [" + fieldName + "]");
        }
        final boolean docValues = fieldType.hasDocValues();
        IndexFieldData.Builder builder = null;
        String format = type.getFormat(indexSettings.getSettings());
        if (format != null && FieldDataType.DOC_VALUES_FORMAT_VALUE.equals(format) && !docValues) {
            logger.warn("field [" + fieldName + "] has no doc values, will use default field data format");
            format = null;
        }
        if (format != null) {
            builder = buildersByTypeAndFormat.get(Tuple.tuple(type.getType(), format));
            if (builder == null) {
                logger.warn("failed to find format [" + format + "] for field [" + fieldName + "], will use default");
            }
        }
        if (builder == null && docValues) {
            builder = docValuesBuildersByType.get(type.getType());
        }
        if (builder == null) {
            builder = buildersByType.get(type.getType());
        }
        if (builder == null) {
            throw new IllegalArgumentException("failed to find field data builder for field " + fieldName + ", and type " + type.getType());
        }

        IndexFieldDataCache cache;
        synchronized (this) {
            cache = fieldDataCaches.get(fieldName);
            if (cache == null) {
                //  we default to node level cache, which in turn defaults to be unbounded
                // this means changing the node level settings is simple, just set the bounds there
                String cacheType = type.getSettings().get("cache", indexSettings.getSettings().get(FIELDDATA_CACHE_KEY, FIELDDATA_CACHE_VALUE_NODE));
                if (FIELDDATA_CACHE_VALUE_NODE.equals(cacheType)) {
                    cache = indicesFieldDataCache.buildIndexFieldDataCache(listener, index(), fieldName, type);
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
