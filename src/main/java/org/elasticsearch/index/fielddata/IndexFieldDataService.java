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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.plain.*;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class IndexFieldDataService extends AbstractIndexComponent {

    public static final String FIELDDATA_CACHE_KEY = "index.fielddata.cache";
    public static final String FIELDDATA_CACHE_VALUE_NODE = "node";

    private static final String DISABLED_FORMAT = "disabled";
    private static final String DOC_VALUES_FORMAT = "doc_values";
    private static final String ARRAY_FORMAT = "array";
    private static final String PAGED_BYTES_FORMAT = "paged_bytes";
    private static final String FST_FORMAT = "fst";
    private static final String COMPRESSED_FORMAT = "compressed";

    private final static ImmutableMap<String, IndexFieldData.Builder> buildersByType;
    private final static ImmutableMap<String, IndexFieldData.Builder> docValuesBuildersByType;
    private final static ImmutableMap<Tuple<String, String>, IndexFieldData.Builder> buildersByTypeAndFormat;
    private final CircuitBreakerService circuitBreakerService;

    static {
        buildersByType = MapBuilder.<String, IndexFieldData.Builder>newMapBuilder()
                .put("string", new PagedBytesIndexFieldData.Builder())
                .put("float", new FloatArrayIndexFieldData.Builder())
                .put("double", new DoubleArrayIndexFieldData.Builder())
                .put("byte", new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.BYTE))
                .put("short", new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.SHORT))
                .put("int", new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.INT))
                .put("long", new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.LONG))
                .put("geo_point", new GeoPointDoubleArrayIndexFieldData.Builder())
                .put(ParentFieldMapper.NAME, new ParentChildIndexFieldData.Builder())
                .put(IndexFieldMapper.NAME, new IndexIndexFieldData.Builder())
                .put("binary", new DisabledIndexFieldData.Builder())
                .put(BooleanFieldMapper.CONTENT_TYPE, new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.BOOLEAN))
                .immutableMap();

        docValuesBuildersByType = MapBuilder.<String, IndexFieldData.Builder>newMapBuilder()
                .put("string", new DocValuesIndexFieldData.Builder())
                .put("float", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.FLOAT))
                .put("double", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.DOUBLE))
                .put("byte", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.BYTE))
                .put("short", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.SHORT))
                .put("int", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.INT))
                .put("long", new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.LONG))
                .put("geo_point", new GeoPointBinaryDVIndexFieldData.Builder())
                .put("binary", new BytesBinaryDVIndexFieldData.Builder())
                .put(BooleanFieldMapper.CONTENT_TYPE, new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.BOOLEAN))
                .immutableMap();

        buildersByTypeAndFormat = MapBuilder.<Tuple<String, String>, IndexFieldData.Builder>newMapBuilder()
                .put(Tuple.tuple("string", PAGED_BYTES_FORMAT), new PagedBytesIndexFieldData.Builder())
                .put(Tuple.tuple("string", FST_FORMAT), new FSTBytesIndexFieldData.Builder())
                .put(Tuple.tuple("string", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder())
                .put(Tuple.tuple("string", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("float", ARRAY_FORMAT), new FloatArrayIndexFieldData.Builder())
                .put(Tuple.tuple("float", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.FLOAT))
                .put(Tuple.tuple("float", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("double", ARRAY_FORMAT), new DoubleArrayIndexFieldData.Builder())
                .put(Tuple.tuple("double", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.DOUBLE))
                .put(Tuple.tuple("double", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("byte", ARRAY_FORMAT), new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.BYTE))
                .put(Tuple.tuple("byte", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.BYTE))
                .put(Tuple.tuple("byte", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("short", ARRAY_FORMAT), new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.SHORT))
                .put(Tuple.tuple("short", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.SHORT))
                .put(Tuple.tuple("short", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("int", ARRAY_FORMAT), new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.INT))
                .put(Tuple.tuple("int", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.INT))
                .put(Tuple.tuple("int", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("long", ARRAY_FORMAT), new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.LONG))
                .put(Tuple.tuple("long", DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.LONG))
                .put(Tuple.tuple("long", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple("geo_point", ARRAY_FORMAT), new GeoPointDoubleArrayIndexFieldData.Builder())
                .put(Tuple.tuple("geo_point", DOC_VALUES_FORMAT), new GeoPointBinaryDVIndexFieldData.Builder())
                .put(Tuple.tuple("geo_point", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())
                .put(Tuple.tuple("geo_point", COMPRESSED_FORMAT), new GeoPointCompressedIndexFieldData.Builder())

                .put(Tuple.tuple("binary", DOC_VALUES_FORMAT), new BytesBinaryDVIndexFieldData.Builder())
                .put(Tuple.tuple("binary", DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .put(Tuple.tuple(BooleanFieldMapper.CONTENT_TYPE, ARRAY_FORMAT), new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.BOOLEAN))
                .put(Tuple.tuple(BooleanFieldMapper.CONTENT_TYPE, DOC_VALUES_FORMAT), new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.BOOLEAN))
                .put(Tuple.tuple(BooleanFieldMapper.CONTENT_TYPE, DISABLED_FORMAT), new DisabledIndexFieldData.Builder())

                .immutableMap();
    }

    private final IndicesFieldDataCache indicesFieldDataCache;
    private final ConcurrentMap<String, IndexFieldData<?>> loadedFieldData = ConcurrentCollections.newConcurrentMap();
    private final KeyedLock.GlobalLockable<String> fieldLoadingLock = new KeyedLock.GlobalLockable<>();
    private final Map<String, IndexFieldDataCache> fieldDataCaches = Maps.newHashMap(); // no need for concurrency support, always used under lock

    IndexService indexService;

    @Inject
    public IndexFieldDataService(Index index, @IndexSettings Settings indexSettings, IndicesFieldDataCache indicesFieldDataCache,
                                 CircuitBreakerService circuitBreakerService) {
        super(index, indexSettings);
        this.indicesFieldDataCache = indicesFieldDataCache;
        this.circuitBreakerService = circuitBreakerService;
    }

    // we need to "inject" the index service to not create cyclic dep
    public void setIndexService(IndexService indexService) {
        this.indexService = indexService;
    }

    public void clear() {
        fieldLoadingLock.globalLock().lock();
        try {
            List<Throwable> exceptions = new ArrayList<>(0);
            final Collection<IndexFieldData<?>> fieldDataValues = loadedFieldData.values();
            for (IndexFieldData<?> fieldData : fieldDataValues) {
                try {
                    fieldData.clear();
                } catch (Throwable t) {
                    exceptions.add(t);
                }
            }
            fieldDataValues.clear();
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
        } finally {
            fieldLoadingLock.globalLock().unlock();
        }
    }

    public void clearField(final String fieldName) {
        fieldLoadingLock.acquire(fieldName);
        try {
            List<Throwable> exceptions = new ArrayList<>(0);
            final IndexFieldData<?> fieldData = loadedFieldData.remove(fieldName);
            if (fieldData != null) {
                try {
                    fieldData.clear();
                } catch (Throwable t) {
                    exceptions.add(t);
                }
            }
            final IndexFieldDataCache cache = fieldDataCaches.remove(fieldName);
            if (cache != null) {
                try {
                    cache.clear();
                } catch (Throwable t) {
                    exceptions.add(t);
                }
            }
            ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
        } finally {
            fieldLoadingLock.release(fieldName);
        }
    }

    public void onMappingUpdate() {
        // synchronize to make sure to not miss field data instances that are being loaded
        fieldLoadingLock.globalLock().lock();
        try {
            // important: do not clear fieldDataCaches: the cache may be reused
            loadedFieldData.clear();
        } finally {
            fieldLoadingLock.globalLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    public <IFD extends IndexFieldData<?>> IFD getForField(FieldMapper<?> mapper) {
        final FieldMapper.Names fieldNames = mapper.names();
        final FieldDataType type = mapper.fieldDataType();
        if (type == null) {
            throw new ElasticsearchIllegalArgumentException("found no fielddata type for field [" + fieldNames.fullName() + "]");
        }
        final boolean docValues = mapper.hasDocValues();
        final String key = fieldNames.indexName();
        IndexFieldData<?> fieldData = loadedFieldData.get(key);
        if (fieldData == null) {
            fieldLoadingLock.acquire(key);
            try {
                fieldData = loadedFieldData.get(key);
                if (fieldData == null) {
                    IndexFieldData.Builder builder = null;
                    String format = type.getFormat(indexSettings);
                    if (format != null && FieldDataType.DOC_VALUES_FORMAT_VALUE.equals(format) && !docValues) {
                        logger.warn("field [" + fieldNames.fullName() + "] has no doc values, will use default field data format");
                        format = null;
                    }
                    if (format != null) {
                        builder = buildersByTypeAndFormat.get(Tuple.tuple(type.getType(), format));
                        if (builder == null) {
                            logger.warn("failed to find format [" + format + "] for field [" + fieldNames.fullName() + "], will use default");
                        }
                    }
                    if (builder == null && docValues) {
                        builder = docValuesBuildersByType.get(type.getType());
                    }
                    if (builder == null) {
                        builder = buildersByType.get(type.getType());
                    }
                    if (builder == null) {
                        throw new ElasticsearchIllegalArgumentException("failed to find field data builder for field " + fieldNames.fullName() + ", and type " + type.getType());
                    }

                    IndexFieldDataCache cache = fieldDataCaches.get(fieldNames.indexName());
                    if (cache == null) {
                        //  we default to node level cache, which in turn defaults to be unbounded
                        // this means changing the node level settings is simple, just set the bounds there
                        String cacheType = type.getSettings().get("cache", indexSettings.get(FIELDDATA_CACHE_KEY, FIELDDATA_CACHE_VALUE_NODE));
                        if (FIELDDATA_CACHE_VALUE_NODE.equals(cacheType)) {
                            cache = indicesFieldDataCache.buildIndexFieldDataCache(indexService, index, fieldNames, type);
                        } else if ("none".equals(cacheType)){
                            cache = new IndexFieldDataCache.None();
                        } else {
                            throw new ElasticsearchIllegalArgumentException("cache type not supported [" + cacheType + "] for field [" + fieldNames.fullName() + "]");
                        }
                        fieldDataCaches.put(fieldNames.indexName(), cache);
                    }

                    fieldData = builder.build(index, indexSettings, mapper, cache, circuitBreakerService, indexService.mapperService());
                    loadedFieldData.put(fieldNames.indexName(), fieldData);
                }
            } finally {
                fieldLoadingLock.release(key);
            }
        }
        return (IFD) fieldData;
    }

}
