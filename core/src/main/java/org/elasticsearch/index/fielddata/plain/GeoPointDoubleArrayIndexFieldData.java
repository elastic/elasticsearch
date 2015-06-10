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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

/**
 */
public class GeoPointDoubleArrayIndexFieldData extends AbstractIndexGeoPointFieldData {

    private final CircuitBreakerService breakerService;

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, @IndexSettings Settings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            return new GeoPointDoubleArrayIndexFieldData(index, indexSettings, fieldType.names(), fieldType.fieldDataType(), cache, breakerService);
        }
    }

    public GeoPointDoubleArrayIndexFieldData(Index index, @IndexSettings Settings indexSettings, MappedFieldType.Names fieldNames,
                                             FieldDataType fieldDataType, IndexFieldDataCache cache, CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        this.breakerService = breakerService;
    }

    @Override
    public AtomicGeoPointFieldData loadDirect(LeafReaderContext context) throws Exception {
        LeafReader reader = context.reader();

        Terms terms = reader.terms(getFieldNames().indexName());
        AtomicGeoPointFieldData data = null;
        // TODO: Use an actual estimator to estimate before loading.
        NonEstimatingEstimator estimator = new NonEstimatingEstimator(breakerService.getBreaker(CircuitBreaker.FIELDDATA));
        if (terms == null) {
            data = AbstractAtomicGeoPointFieldData.empty(reader.maxDoc());
            estimator.afterLoad(null, data.ramBytesUsed());
            return data;
        }
        DoubleArray lat = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(128);
        DoubleArray lon = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(128);
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        boolean success = false;
        try (OrdinalsBuilder builder = new OrdinalsBuilder(terms.size(), reader.maxDoc(), acceptableTransientOverheadRatio)) {
            final GeoPointEnum iter = new GeoPointEnum(builder.buildFromTerms(terms.iterator()));
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
            RandomAccessOrds ordinals = build.ordinals();
            if (!(FieldData.isMultiValued(ordinals) || CommonSettings.getMemoryStorageHint(fieldDataType) == CommonSettings.MemoryStorageFormat.ORDINALS)) {
                int maxDoc = reader.maxDoc();
                DoubleArray sLat = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(reader.maxDoc());
                DoubleArray sLon = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(reader.maxDoc());
                for (int i = 0; i < maxDoc; i++) {
                    ordinals.setDocument(i);
                    long nativeOrdinal = ordinals.nextOrd();
                    if (nativeOrdinal != RandomAccessOrds.NO_MORE_ORDS) {
                        sLat.set(i, lat.get(nativeOrdinal));
                        sLon.set(i, lon.get(nativeOrdinal));
                    }
                }
                BitSet set = builder.buildDocsWithValuesSet();
                data = new GeoPointDoubleArrayAtomicFieldData.Single(sLon, sLat, set);
            } else {
                data = new GeoPointDoubleArrayAtomicFieldData.WithOrdinals(lon, lat, build, reader.maxDoc());
            }
            success = true;
            return data;
        } finally {
            if (success) {
                estimator.afterLoad(null, data.ramBytesUsed());
            }

        }

    }
}