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
package org.elasticsearch.search.aggregations.support;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Arrays;

/**
 *
 */
@SuppressWarnings({"unchecked", "ForLoopReplaceableByForEach"})
public class AggregationContext {

    private final SearchContext searchContext;

    private ObjectObjectOpenHashMap<ConfigCacheKey, ValuesSource>[] perDepthFieldDataSources = new ObjectObjectOpenHashMap[4];

    private boolean scoreDocsInOrder = false;

    public AggregationContext(SearchContext searchContext) {
        this.searchContext = searchContext;
    }

    public SearchContext searchContext() {
        return searchContext;
    }

    public PageCacheRecycler pageCacheRecycler() {
        return searchContext.pageCacheRecycler();
    }

    public BigArrays bigArrays() {
        return searchContext.bigArrays();
    }

    public boolean scoreDocsInOrder() {
        return scoreDocsInOrder;
    }

    public void ensureScoreDocsInOrder() {
        this.scoreDocsInOrder = true;
    }

    /** Get a value source given its configuration and the depth of the aggregator in the aggregation tree. */
    public <VS extends ValuesSource> VS valuesSource(ValuesSourceConfig<VS> config, int depth) {
        assert config.valid() : "value source config is invalid - must have either a field context or a script or marked as unmapped";
        assert !config.unmapped : "value source should not be created for unmapped fields";

        if (perDepthFieldDataSources.length <= depth) {
            perDepthFieldDataSources = Arrays.copyOf(perDepthFieldDataSources, ArrayUtil.oversize(1 + depth, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
        }
        if (perDepthFieldDataSources[depth] == null) {
            perDepthFieldDataSources[depth] = new ObjectObjectOpenHashMap<>();
        }
        final ObjectObjectOpenHashMap<ConfigCacheKey, ValuesSource> fieldDataSources = perDepthFieldDataSources[depth];

        if (config.fieldContext == null) {
            if (ValuesSource.Numeric.class.isAssignableFrom(config.valueSourceType)) {
                return (VS) numericScript(config);
            }
            if (ValuesSource.Bytes.class.isAssignableFrom(config.valueSourceType)) {
                return (VS) bytesScript(config);
            }
            throw new AggregationExecutionException("value source of type [" + config.valueSourceType.getSimpleName() + "] is not supported by scripts");
        }

        if (ValuesSource.Numeric.class.isAssignableFrom(config.valueSourceType)) {
            return (VS) numericField(fieldDataSources, config);
        }
        if (ValuesSource.GeoPoint.class.isAssignableFrom(config.valueSourceType)) {
            return (VS) geoPointField(fieldDataSources, config);
        }
        // falling back to bytes values
        return (VS) bytesField(fieldDataSources, config);
    }

    private ValuesSource.Numeric numericScript(ValuesSourceConfig<?> config) {
        ValuesSource.Numeric source = new ValuesSource.Numeric.Script(config.script, config.scriptValueType);
        if (config.ensureUnique || config.ensureSorted) {
            source = new ValuesSource.Numeric.SortedAndUnique(source);
        }
        return source;
    }

    private ValuesSource.Numeric numericField(ObjectObjectOpenHashMap<ConfigCacheKey, ValuesSource> fieldDataSources, ValuesSourceConfig<?> config) {
        final ConfigCacheKey cacheKey = new ConfigCacheKey(config);
        ValuesSource.Numeric dataSource = (ValuesSource.Numeric) fieldDataSources.get(cacheKey);
        if (dataSource == null) {
            ValuesSource.MetaData metaData = ValuesSource.MetaData.load(config.fieldContext.indexFieldData(), searchContext);
            dataSource = new ValuesSource.Numeric.FieldData((IndexNumericFieldData<?>) config.fieldContext.indexFieldData(), metaData);
            fieldDataSources.put(cacheKey, dataSource);
        }
        if (config.script != null) {
            dataSource = new ValuesSource.Numeric.WithScript(dataSource, config.script);

            if (config.ensureUnique || config.ensureSorted) {
                dataSource = new ValuesSource.Numeric.SortedAndUnique(dataSource);
            }
        }
        if (config.needsHashes) {
            dataSource.setNeedsHashes(true);
        }
        return dataSource;
    }

    private ValuesSource bytesField(ObjectObjectOpenHashMap<ConfigCacheKey, ValuesSource> fieldDataSources, ValuesSourceConfig<?> config) {
        final ConfigCacheKey cacheKey = new ConfigCacheKey(config);
        ValuesSource dataSource = fieldDataSources.get(cacheKey);
        if (dataSource == null) {
            final IndexFieldData<?> indexFieldData = config.fieldContext.indexFieldData();
            ValuesSource.MetaData metaData = ValuesSource.MetaData.load(config.fieldContext.indexFieldData(), searchContext);
            if (indexFieldData instanceof IndexFieldData.WithOrdinals) {
                dataSource = new ValuesSource.Bytes.WithOrdinals.FieldData((IndexFieldData.WithOrdinals) indexFieldData, metaData);
            } else {
                dataSource = new ValuesSource.Bytes.FieldData(indexFieldData, metaData);
            }
            fieldDataSources.put(cacheKey, dataSource);
        }
        if (config.script != null) {
            dataSource = new ValuesSource.WithScript(dataSource, config.script);
        }
        // Even in case we wrap field data, we might still need to wrap for sorting, because the wrapped field data might be
        // eg. a numeric field data that doesn't sort according to the byte order. However field data values are unique so no
        // need to wrap for uniqueness
        if ((config.ensureUnique && !dataSource.metaData().uniqueness().unique()) || config.ensureSorted) {
            dataSource = new ValuesSource.Bytes.SortedAndUnique(dataSource);
        }

        if (config.needsHashes) { // the data source needs hash if at least one consumer needs hashes
            dataSource.setNeedsHashes(true);
        }
        return dataSource;
    }

    private ValuesSource.Bytes bytesScript(ValuesSourceConfig<?> config) {
        ValuesSource.Bytes source = new ValuesSource.Bytes.Script(config.script);
        if (config.ensureUnique || config.ensureSorted) {
            source = new ValuesSource.Bytes.SortedAndUnique(source);
        }
        return source;
    }

    private ValuesSource.GeoPoint geoPointField(ObjectObjectOpenHashMap<ConfigCacheKey, ValuesSource> fieldDataSources, ValuesSourceConfig<?> config) {
        final ConfigCacheKey cacheKey = new ConfigCacheKey(config);
        ValuesSource.GeoPoint dataSource = (ValuesSource.GeoPoint) fieldDataSources.get(cacheKey);
        if (dataSource == null) {
            ValuesSource.MetaData metaData = ValuesSource.MetaData.load(config.fieldContext.indexFieldData(), searchContext);
            dataSource = new ValuesSource.GeoPoint((IndexGeoPointFieldData<?>) config.fieldContext.indexFieldData(), metaData);
            fieldDataSources.put(cacheKey, dataSource);
        }
        if (config.needsHashes) {
            dataSource.setNeedsHashes(true);
        }
        return dataSource;
    }

    private static class ConfigCacheKey {

        private final String field;
        private final Class<? extends ValuesSource> valueSourceType;

        private ConfigCacheKey(ValuesSourceConfig config) {
            this.field = config.fieldContext.field();
            this.valueSourceType = config.valueSourceType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConfigCacheKey that = (ConfigCacheKey) o;

            if (!field.equals(that.field)) return false;
            if (!valueSourceType.equals(that.valueSourceType)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = field.hashCode();
            result = 31 * result + valueSourceType.hashCode();
            return result;
        }
    }
}
