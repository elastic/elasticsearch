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
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

/**
 */
public class GeoPointDoubleArrayIndexFieldData extends AbstractGeoPointIndexFieldData {

    private final CircuitBreakerService breakerService;

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService, GlobalOrdinalsBuilder globalOrdinalBuilder) {
            return new GeoPointDoubleArrayIndexFieldData(index, indexSettings, mapper.names(), mapper.fieldDataType(), cache, breakerService);
        }
    }

    public GeoPointDoubleArrayIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames,
                                             FieldDataType fieldDataType, IndexFieldDataCache cache, CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        this.breakerService = breakerService;
    }

    @Override
    public AtomicGeoPointFieldData<ScriptDocValues> loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();

        Terms terms = reader.terms(getFieldNames().indexName());
        AtomicGeoPointFieldData data = null;
        // TODO: Use an actual estimator to estimate before loading.
        NonEstimatingEstimator estimator = new NonEstimatingEstimator(breakerService.getBreaker());
        if (terms == null) {
            data = new Empty();
            estimator.afterLoad(null, data.getMemorySizeInBytes());
            return data;
        }
        DoubleArray lat = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(128);
        DoubleArray lon = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(128);
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        boolean success = false;
        try (OrdinalsBuilder builder = new OrdinalsBuilder(terms.size(), reader.maxDoc(), acceptableTransientOverheadRatio)) {
            final GeoPointEnum iter = new GeoPointEnum(builder.buildFromTerms(terms.iterator(null)));
            GeoPoint point;
            long numTerms = 0;
            while ((point = iter.next()) != null) {
                lat = BigArrays.NON_RECYCLING_INSTANCE.resize(lat, numTerms + 1);
                lon = BigArrays.NON_RECYCLING_INSTANCE.resize(lon, numTerms + 1);
                lat.set(numTerms, point.getLat());
                lon.set(numTerms, point.getLon());
                ++numTerms;
            }
            lat = BigArrays.NON_RECYCLING_INSTANCE.resize(lat, numTerms);
            lon = BigArrays.NON_RECYCLING_INSTANCE.resize(lon, numTerms);

            Ordinals build = builder.build(fieldDataType.getSettings());
            if (!(build.isMultiValued() || CommonSettings.getMemoryStorageHint(fieldDataType) == CommonSettings.MemoryStorageFormat.ORDINALS)) {
                Docs ordinals = build.ordinals();
                int maxDoc = reader.maxDoc();
                DoubleArray sLat = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(reader.maxDoc());
                DoubleArray sLon = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(reader.maxDoc());
                for (int i = 0; i < maxDoc; i++) {
                    long nativeOrdinal = ordinals.getOrd(i);
                    if (nativeOrdinal != Ordinals.MISSING_ORDINAL) {
                        sLat.set(i, lat.get(nativeOrdinal));
                        sLon.set(i, lon.get(nativeOrdinal));
                    }
                }
                FixedBitSet set = builder.buildDocsWithValuesSet();
                if (set == null) {
                    data = new GeoPointDoubleArrayAtomicFieldData.Single(sLon, sLat, ordinals.getMaxOrd() - Ordinals.MIN_ORDINAL);
                } else {
                    data = new GeoPointDoubleArrayAtomicFieldData.SingleFixedSet(sLon, sLat, set, ordinals.getMaxOrd() - Ordinals.MIN_ORDINAL);
                }
            } else {
                data = new GeoPointDoubleArrayAtomicFieldData.WithOrdinals(lon, lat, build);
            }
            success = true;
            return data;
        } finally {
            if (success) {
                estimator.afterLoad(null, data.getMemorySizeInBytes());
            }

        }

    }
}