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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.plain.*;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;

import java.util.concurrent.ConcurrentMap;

/**
 */
public class IndexFieldDataService extends AbstractIndexComponent {

    private final static ImmutableMap<String, IndexFieldData.Builder> buildersByType;
    private final static ImmutableMap<Tuple<String, String>, IndexFieldData.Builder> buildersByTypeAndFormat;

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
                .immutableMap();

        buildersByTypeAndFormat = MapBuilder.<Tuple<String, String>, IndexFieldData.Builder>newMapBuilder()
                .put(Tuple.tuple("string", "paged_bytes"), new PagedBytesIndexFieldData.Builder())
                .put(Tuple.tuple("string", "fst"), new FSTBytesIndexFieldData.Builder())
                .put(Tuple.tuple("float", "array"), new FloatArrayIndexFieldData.Builder())
                .put(Tuple.tuple("double", "array"), new DoubleArrayIndexFieldData.Builder())
                .put(Tuple.tuple("byte", "array"), new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.BYTE))
                .put(Tuple.tuple("short", "array"), new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.SHORT))
                .put(Tuple.tuple("int", "array"), new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.INT))
                .put(Tuple.tuple("long", "array"), new PackedArrayIndexFieldData.Builder().setNumericType(IndexNumericFieldData.NumericType.LONG))
                .put(Tuple.tuple("geo_point", "array"), new GeoPointDoubleArrayIndexFieldData.Builder())
                .immutableMap();
    }

    private final IndicesFieldDataCache indicesFieldDataCache;
    private final ConcurrentMap<String, IndexFieldData> loadedFieldData = ConcurrentCollections.newConcurrentMap();

    IndexService indexService;

    public IndexFieldDataService(Index index) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS, new IndicesFieldDataCache(ImmutableSettings.Builder.EMPTY_SETTINGS));
    }

    @Inject
    public IndexFieldDataService(Index index, @IndexSettings Settings indexSettings, IndicesFieldDataCache indicesFieldDataCache) {
        super(index, indexSettings);
        this.indicesFieldDataCache = indicesFieldDataCache;
    }

    // we need to "inject" the index service to not create cyclic dep
    public void setIndexService(IndexService indexService) {
        this.indexService = indexService;
    }

    public void clear() {
        synchronized (loadedFieldData) {
            for (IndexFieldData fieldData : loadedFieldData.values()) {
                fieldData.clear();
            }
            loadedFieldData.clear();
        }
    }

    public void clearField(String fieldName) {
        synchronized (loadedFieldData) {
            IndexFieldData fieldData = loadedFieldData.remove(fieldName);
            if (fieldData != null) {
                fieldData.clear();
            }
        }
    }

    public void clear(IndexReader reader) {
        for (IndexFieldData indexFieldData : loadedFieldData.values()) {
            indexFieldData.clear(reader);
        }
    }

    public <IFD extends IndexFieldData> IFD getForField(FieldMapper mapper) {
        return getForField(mapper.names(), mapper.fieldDataType());
    }

    public <IFD extends IndexFieldData> IFD getForField(FieldMapper.Names fieldNames, FieldDataType type) {
        IndexFieldData fieldData = loadedFieldData.get(fieldNames.indexName());
        if (fieldData == null) {
            synchronized (loadedFieldData) {
                fieldData = loadedFieldData.get(fieldNames.indexName());
                if (fieldData == null) {
                    IndexFieldData.Builder builder = null;
                    String format = type.getSettings().get("format", indexSettings.get("index.fielddata.type." + type.getType() + ".format", null));
                    if (format != null) {
                        builder = buildersByTypeAndFormat.get(Tuple.tuple(type.getType(), format));
                        if (builder == null) {
                            logger.warn("failed to find format [" + format + "] for field [" + fieldNames.fullName() + "], will use default");
                        }
                    }
                    if (builder == null) {
                        builder = buildersByType.get(type.getType());
                    }
                    if (builder == null) {
                        throw new ElasticSearchIllegalArgumentException("failed to find field data builder for field " + fieldNames.fullName() + ", and type " + type.getType());
                    }

                    IndexFieldDataCache cache;
                    //  we default to node level cache, which in turn defaults to be unbounded
                    // this means changing the node level settings is simple, just set the bounds there
                    String cacheType = type.getSettings().get("cache", indexSettings.get("index.fielddata.cache", "node"));
                    if ("resident".equals(cacheType)) {
                        cache = new IndexFieldDataCache.Resident(indexService, fieldNames, type);
                    } else if ("soft".equals(cacheType)) {
                        cache = new IndexFieldDataCache.Soft(indexService, fieldNames, type);
                    } else if ("node".equals(cacheType)) {
                        cache = indicesFieldDataCache.buildIndexFieldDataCache(indexService, index, fieldNames, type);
                    } else {
                        throw new ElasticSearchIllegalArgumentException("cache type not supported [" + cacheType + "] for field [" + fieldNames.fullName() + "]");
                    }

                    fieldData = builder.build(index, indexSettings, fieldNames, type, cache);
                    loadedFieldData.put(fieldNames.indexName(), fieldData);
                }
            }
        }
        return (IFD) fieldData;
    }
}
