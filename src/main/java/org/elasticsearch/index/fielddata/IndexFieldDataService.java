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
import org.elasticsearch.index.settings.IndexSettings;

import java.util.concurrent.ConcurrentMap;

/**
 */
public class IndexFieldDataService extends AbstractIndexComponent {

    private final static ImmutableMap<String, IndexFieldData.Builder> buildersByType;
    private final static ImmutableMap<Tuple<String, String>, IndexFieldData.Builder> buildersByTypeAndFormat;

    static {
        buildersByType = MapBuilder.<String, IndexFieldData.Builder>newMapBuilder()
                .put("string", new ConcreteBytesRefIndexFieldData.Builder())
                .put("float", new FloatArrayIndexFieldData.Builder())
                .put("double", new DoubleArrayIndexFieldData.Builder())
                .put("byte", new ByteArrayIndexFieldData.Builder())
                .put("short", new ShortArrayIndexFieldData.Builder())
                .put("int", new IntArrayIndexFieldData.Builder())
                .put("long", new LongArrayIndexFieldData.Builder())
                .put("geo_point", new GeoPointDoubleArrayIndexFieldData.Builder())
                .immutableMap();

        buildersByTypeAndFormat = MapBuilder.<Tuple<String, String>, IndexFieldData.Builder>newMapBuilder()
                .put(Tuple.tuple("string", "concrete_bytes"), new ConcreteBytesRefIndexFieldData.Builder())
                .put(Tuple.tuple("string", "paged_bytes"), new PagesBytesIndexFieldData.Builder())
                .put(Tuple.tuple("float", "array"), new FloatArrayIndexFieldData.Builder())
                .put(Tuple.tuple("double", "array"), new DoubleArrayIndexFieldData.Builder())
                .put(Tuple.tuple("byte", "array"), new ByteArrayIndexFieldData.Builder())
                .put(Tuple.tuple("short", "array"), new ShortArrayIndexFieldData.Builder())
                .put(Tuple.tuple("int", "array"), new IntArrayIndexFieldData.Builder())
                .put(Tuple.tuple("long", "array"), new LongArrayIndexFieldData.Builder())
                .put(Tuple.tuple("geo_point", "array"), new GeoPointDoubleArrayIndexFieldData.Builder())
                .immutableMap();
    }

    private final ConcurrentMap<String, IndexFieldData> loadedFieldData = ConcurrentCollections.newConcurrentMap();

    public IndexFieldDataService(Index index) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    @Inject
    public IndexFieldDataService(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
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

    public <IFD extends IndexFieldData> IFD getForField(FieldMapper mapper) {
        return getForField(mapper.names(), mapper.fieldDataType2());
    }

    public <IFD extends IndexFieldData> IFD getForField(FieldMapper.Names fieldNames, FieldDataType type) {
        IndexFieldData fieldData = loadedFieldData.get(type.getType());
        if (fieldData == null) {
            synchronized (loadedFieldData) {
                fieldData = loadedFieldData.get(type.getType());
                if (fieldData == null) {
                    IndexFieldData.Builder builder = null;
                    if (type.getFormat() != null) {
                        builder = buildersByTypeAndFormat.get(Tuple.tuple(type.getType(), type.getFormat()));
                    }
                    if (builder == null) {
                        builder = buildersByType.get(type.getType());
                    }
                    if (builder == null) {
                        throw new ElasticSearchIllegalArgumentException("failed to find field data builder for field " + fieldNames.fullName() + ", and type " + type);
                    }

                    IndexFieldDataCache cache;
                    if (type.getOptions().containsKey("cache")) {
                        String cacheType = type.getOptions().get("cache");
                        if ("resident".equals(cacheType)) {
                            cache = new IndexFieldDataCache.Resident();
                        } else if ("soft".equals(cacheType)) {
                            cache = new IndexFieldDataCache.Soft();
                        } else {
                            throw new ElasticSearchIllegalArgumentException("cache type not supported [" + cacheType + "] for field [" + fieldNames.fullName() + "]");
                        }
                    } else {
                        cache = new IndexFieldDataCache.Resident();
                    }

                    fieldData = builder.build(index, indexSettings, fieldNames, type, cache);
                    loadedFieldData.put(fieldNames.indexName(), fieldData);
                }
            }
        }
        return (IFD) fieldData;
    }
}
