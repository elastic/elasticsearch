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
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.fielddata.ordinals.InternalGlobalOrdinalsBuilder;
import org.elasticsearch.index.fielddata.plain.*;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCacheListener;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class IndexFieldDataService extends AbstractIndexComponent {

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
    private final IndicesFieldDataCacheListener indicesFieldDataCacheListener;

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

                .immutableMap();
    }

    private final IndicesFieldDataCache indicesFieldDataCache;
    private final ConcurrentMap<String, IndexFieldData<?>> loadedFieldData = ConcurrentCollections.newConcurrentMap();
    private final Map<String, IndexFieldDataCache> fieldDataCaches = Maps.newHashMap(); // no need for concurrency support, always used under lock

    IndexService indexService;

    // public for testing
    public IndexFieldDataService(Index index, CircuitBreakerService circuitBreakerService) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS, new IndicesFieldDataCache(ImmutableSettings.Builder.EMPTY_SETTINGS, new IndicesFieldDataCacheListener(circuitBreakerService)), circuitBreakerService, new IndicesFieldDataCacheListener(circuitBreakerService));
    }

    // public for testing
    public IndexFieldDataService(Index index, CircuitBreakerService circuitBreakerService, IndicesFieldDataCache indicesFieldDataCache) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS, indicesFieldDataCache, circuitBreakerService, new IndicesFieldDataCacheListener(circuitBreakerService));
    }

    @Inject
    public IndexFieldDataService(Index index, @IndexSettings Settings indexSettings, IndicesFieldDataCache indicesFieldDataCache,
                                 CircuitBreakerService circuitBreakerService, IndicesFieldDataCacheListener indicesFieldDataCacheListener) {
        super(index, indexSettings);
        this.indicesFieldDataCache = indicesFieldDataCache;
        this.circuitBreakerService = circuitBreakerService;
        this.indicesFieldDataCacheListener = indicesFieldDataCacheListener;
    }

    // we need to "inject" the index service to not create cyclic dep
    public void setIndexService(IndexService indexService) {
        this.indexService = indexService;
    }

    public void clear() {
        synchronized (loadedFieldData) {
            for (IndexFieldData<?> fieldData : loadedFieldData.values()) {
                fieldData.clear();
            }
            loadedFieldData.clear();
            for (IndexFieldDataCache cache : fieldDataCaches.values()) {
                cache.clear();
            }
            fieldDataCaches.clear();
        }
    }

    public void clearField(String fieldName) {
        synchronized (loadedFieldData) {
            IndexFieldData<?> fieldData = loadedFieldData.remove(fieldName);
            if (fieldData != null) {
                fieldData.clear();
            }
            IndexFieldDataCache cache = fieldDataCaches.remove(fieldName);
            if (cache != null) {
                cache.clear();
            }
        }
    }

    public void clear(IndexReader reader) {
        synchronized (loadedFieldData) {
            for (IndexFieldData<?> indexFieldData : loadedFieldData.values()) {
                indexFieldData.clear(reader);
            }
            for (IndexFieldDataCache cache : fieldDataCaches.values()) {
                cache.clear(reader);
            }
        }
    }

    public void onMappingUpdate() {
        // synchronize to make sure to not miss field data instances that are being loaded
        synchronized (loadedFieldData) {
            // important: do not clear fieldDataCaches: the cache may be reused
            loadedFieldData.clear();
        }
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(FieldMapper<?> mapper) {
        final FieldMapper.Names fieldNames = mapper.names();
        final FieldDataType type = mapper.fieldDataType();
        if (type == null) {
            throw new ElasticsearchIllegalArgumentException("found no fielddata type for field [" + fieldNames.fullName() + "]");
        }
        final boolean docValues = mapper.hasDocValues();
        IndexFieldData<?> fieldData = loadedFieldData.get(fieldNames.indexName());
        if (fieldData == null) {
            synchronized (loadedFieldData) {
                fieldData = loadedFieldData.get(fieldNames.indexName());
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
                        String cacheType = type.getSettings().get("cache", indexSettings.get("index.fielddata.cache", "node"));
                        if ("resident".equals(cacheType)) {
                            cache = new IndexFieldDataCache.Resident(logger, indexService, fieldNames, type, indicesFieldDataCacheListener);
                        } else if ("soft".equals(cacheType)) {
                            cache = new IndexFieldDataCache.Soft(logger, indexService, fieldNames, type, indicesFieldDataCacheListener);
                        } else if ("node".equals(cacheType)) {
                            cache = indicesFieldDataCache.buildIndexFieldDataCache(indexService, index, fieldNames, type);
                        } else {
                            throw new ElasticsearchIllegalArgumentException("cache type not supported [" + cacheType + "] for field [" + fieldNames.fullName() + "]");
                        }
                        fieldDataCaches.put(fieldNames.indexName(), cache);
                    }

                    GlobalOrdinalsBuilder globalOrdinalBuilder = new InternalGlobalOrdinalsBuilder(index(), indexSettings);
                    fieldData = builder.build(index, indexSettings, mapper, cache, circuitBreakerService, indexService.mapperService(), globalOrdinalBuilder);
                    loadedFieldData.put(fieldNames.indexName(), fieldData);
                }
            }
        }
        return (IFD) fieldData;
    }

}
