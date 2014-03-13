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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.support.geopoints.GeoPointValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
@SuppressWarnings({"unchecked", "ForLoopReplaceableByForEach"})
public class AggregationContext implements ReaderContextAware, ScorerAware {

    private final SearchContext searchContext;

    private ObjectObjectOpenHashMap<ConfigCacheKey, FieldDataSource>[] perDepthFieldDataSources = new ObjectObjectOpenHashMap[4];
    private List<ReaderContextAware> readerAwares = new ArrayList<ReaderContextAware>();
    private List<ScorerAware> scorerAwares = new ArrayList<ScorerAware>();

    private AtomicReaderContext reader;
    private Scorer scorer;

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

    public AtomicReaderContext currentReader() {
        return reader;
    }

    public Scorer currentScorer() {
        return scorer;
    }

    public void setNextReader(AtomicReaderContext reader) {
        this.reader = reader;
        for (ReaderContextAware aware : readerAwares) {
            aware.setNextReader(reader);
        }
    }

    public void setScorer(Scorer scorer) {
        this.scorer = scorer;
        for (ScorerAware scorerAware : scorerAwares) {
            scorerAware.setScorer(scorer);
        }
    }

    /** Get a value source given its configuration and the depth of the aggregator in the aggregation tree. */
    public <VS extends ValuesSource> VS valuesSource(ValuesSourceConfig<VS> config, int depth) {
        assert config.valid() : "value source config is invalid - must have either a field context or a script or marked as unmapped";
        assert !config.unmapped : "value source should not be created for unmapped fields";

        if (perDepthFieldDataSources.length <= depth) {
            perDepthFieldDataSources = Arrays.copyOf(perDepthFieldDataSources, ArrayUtil.oversize(1 + depth, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
        }
        if (perDepthFieldDataSources[depth] == null) {
            perDepthFieldDataSources[depth] = new ObjectObjectOpenHashMap<ConfigCacheKey, FieldDataSource>();
        }
        final ObjectObjectOpenHashMap<ConfigCacheKey, FieldDataSource> fieldDataSources = perDepthFieldDataSources[depth];

        if (config.fieldContext == null) {
            if (NumericValuesSource.class.isAssignableFrom(config.valueSourceType)) {
                return (VS) numericScript(config);
            }
            if (BytesValuesSource.class.isAssignableFrom(config.valueSourceType)) {
                return (VS) bytesScript(config);
            }
            throw new AggregationExecutionException("value source of type [" + config.valueSourceType.getSimpleName() + "] is not supported by scripts");
        }

        if (NumericValuesSource.class.isAssignableFrom(config.valueSourceType)) {
            return (VS) numericField(fieldDataSources, config);
        }
        if (GeoPointValuesSource.class.isAssignableFrom(config.valueSourceType)) {
            return (VS) geoPointField(fieldDataSources, config);
        }
        // falling back to bytes values
        return (VS) bytesField(fieldDataSources, config);
    }

    private NumericValuesSource numericScript(ValuesSourceConfig<?> config) {
        setScorerIfNeeded(config.script);
        setReaderIfNeeded(config.script);
        scorerAwares.add(config.script);
        readerAwares.add(config.script);
        FieldDataSource.Numeric source = new FieldDataSource.Numeric.Script(config.script, config.scriptValueType);
        if (config.ensureUnique || config.ensureSorted) {
            source = new FieldDataSource.Numeric.SortedAndUnique(source);
            readerAwares.add((ReaderContextAware) source);
        }
        return new NumericValuesSource(source, config.formatter(), config.parser());
    }

    private NumericValuesSource numericField(ObjectObjectOpenHashMap<ConfigCacheKey, FieldDataSource> fieldDataSources, ValuesSourceConfig<?> config) {
        final ConfigCacheKey cacheKey = new ConfigCacheKey(config);
        FieldDataSource.Numeric dataSource = (FieldDataSource.Numeric) fieldDataSources.get(cacheKey);
        if (dataSource == null) {
            FieldDataSource.MetaData metaData = FieldDataSource.MetaData.load(config.fieldContext.indexFieldData(), searchContext);
            dataSource = new FieldDataSource.Numeric.FieldData((IndexNumericFieldData<?>) config.fieldContext.indexFieldData(), metaData);
            setReaderIfNeeded((ReaderContextAware) dataSource);
            readerAwares.add((ReaderContextAware) dataSource);
            fieldDataSources.put(cacheKey, dataSource);
        }
        if (config.script != null) {
            setScorerIfNeeded(config.script);
            setReaderIfNeeded(config.script);
            scorerAwares.add(config.script);
            readerAwares.add(config.script);
            dataSource = new FieldDataSource.Numeric.WithScript(dataSource, config.script);

            if (config.ensureUnique || config.ensureSorted) {
                dataSource = new FieldDataSource.Numeric.SortedAndUnique(dataSource);
                readerAwares.add((ReaderContextAware) dataSource);
            }
        }
        if (config.needsHashes) {
            dataSource.setNeedsHashes(true);
        }
        return new NumericValuesSource(dataSource, config.formatter(), config.parser());
    }

    private ValuesSource bytesField(ObjectObjectOpenHashMap<ConfigCacheKey, FieldDataSource> fieldDataSources, ValuesSourceConfig<?> config) {
        final ConfigCacheKey cacheKey = new ConfigCacheKey(config);
        FieldDataSource dataSource = fieldDataSources.get(cacheKey);
        if (dataSource == null) {
            final IndexFieldData<?> indexFieldData = config.fieldContext.indexFieldData();
            FieldDataSource.MetaData metaData = FieldDataSource.MetaData.load(config.fieldContext.indexFieldData(), searchContext);
            if (indexFieldData instanceof IndexFieldData.WithOrdinals) {
                dataSource = new FieldDataSource.Bytes.WithOrdinals.FieldData((IndexFieldData.WithOrdinals) indexFieldData, metaData);
            } else {
                dataSource = new FieldDataSource.Bytes.FieldData(indexFieldData, metaData);
            }
            setReaderIfNeeded((ReaderContextAware) dataSource);
            readerAwares.add((ReaderContextAware) dataSource);
            fieldDataSources.put(cacheKey, dataSource);
        }
        if (config.script != null) {
            setScorerIfNeeded(config.script);
            setReaderIfNeeded(config.script);
            scorerAwares.add(config.script);
            readerAwares.add(config.script);
            dataSource = new FieldDataSource.WithScript(dataSource, config.script);
        }
        // Even in case we wrap field data, we might still need to wrap for sorting, because the wrapped field data might be
        // eg. a numeric field data that doesn't sort according to the byte order. However field data values are unique so no
        // need to wrap for uniqueness
        if ((config.ensureUnique && !dataSource.metaData().uniqueness().unique()) || config.ensureSorted) {
            dataSource = new FieldDataSource.Bytes.SortedAndUnique(dataSource);
            readerAwares.add((ReaderContextAware) dataSource);
        }

        if (config.needsHashes) { // the data source needs hash if at least one consumer needs hashes
            dataSource.setNeedsHashes(true);
        }
        if (dataSource instanceof FieldDataSource.Bytes.WithOrdinals) {
            return new BytesValuesSource.WithOrdinals((FieldDataSource.Bytes.WithOrdinals) dataSource);
        } else {
            return new BytesValuesSource(dataSource);
        }
    }

    private BytesValuesSource bytesScript(ValuesSourceConfig<?> config) {
        setScorerIfNeeded(config.script);
        setReaderIfNeeded(config.script);
        scorerAwares.add(config.script);
        readerAwares.add(config.script);
        FieldDataSource.Bytes source = new FieldDataSource.Bytes.Script(config.script);
        if (config.ensureUnique || config.ensureSorted) {
            source = new FieldDataSource.Bytes.SortedAndUnique(source);
            readerAwares.add((ReaderContextAware) source);
        }
        return new BytesValuesSource(source);
    }

    private GeoPointValuesSource geoPointField(ObjectObjectOpenHashMap<ConfigCacheKey, FieldDataSource> fieldDataSources, ValuesSourceConfig<?> config) {
        final ConfigCacheKey cacheKey = new ConfigCacheKey(config);
        FieldDataSource.GeoPoint dataSource = (FieldDataSource.GeoPoint) fieldDataSources.get(cacheKey);
        if (dataSource == null) {
            FieldDataSource.MetaData metaData = FieldDataSource.MetaData.load(config.fieldContext.indexFieldData(), searchContext);
            dataSource = new FieldDataSource.GeoPoint((IndexGeoPointFieldData<?>) config.fieldContext.indexFieldData(), metaData);
            setReaderIfNeeded(dataSource);
            readerAwares.add(dataSource);
            fieldDataSources.put(cacheKey, dataSource);
        }
        if (config.needsHashes) {
            dataSource.setNeedsHashes(true);
        }
        return new GeoPointValuesSource(dataSource);
    }

    public void registerReaderContextAware(ReaderContextAware readerContextAware) {
        setReaderIfNeeded(readerContextAware);
        readerAwares.add(readerContextAware);
    }

    public void registerScorerAware(ScorerAware scorerAware) {
        setScorerIfNeeded(scorerAware);
        scorerAwares.add(scorerAware);
    }

    private void setReaderIfNeeded(ReaderContextAware readerAware) {
        if (reader != null) {
            readerAware.setNextReader(reader);
        }
    }

    private void setScorerIfNeeded(ScorerAware scorerAware) {
        if (scorer != null) {
            scorerAware.setScorer(scorer);
        }
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
