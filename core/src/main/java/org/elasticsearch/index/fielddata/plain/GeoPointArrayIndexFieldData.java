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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicGeoPointFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

/**
 * Loads FieldData for an array of GeoPoints supporting both long encoded points and backward compatible double arrays
 */
public class GeoPointArrayIndexFieldData extends AbstractIndexGeoPointFieldData {
    private final CircuitBreakerService breakerService;

    public static class Builder implements IndexFieldData.Builder {
        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            return new GeoPointArrayIndexFieldData(indexSettings, fieldType.names(), fieldType.fieldDataType(), cache,
                    breakerService);
        }
    }

    public GeoPointArrayIndexFieldData(IndexSettings indexSettings, MappedFieldType.Names fieldNames,
                                       FieldDataType fieldDataType, IndexFieldDataCache cache, CircuitBreakerService breakerService) {
        super(indexSettings, fieldNames, fieldDataType, cache);
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
        return loadFieldData(reader, estimator, terms, data);
    }

    /**
     * long encoded geopoint field data
     */
    private AtomicGeoPointFieldData loadFieldData(LeafReader reader, NonEstimatingEstimator estimator, Terms terms,
                                                    AtomicGeoPointFieldData data) throws Exception {
        LongArray indexedPoints = BigArrays.NON_RECYCLING_INSTANCE.newLongArray(128);
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio",
                OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        boolean success = false;
        try (OrdinalsBuilder builder = new OrdinalsBuilder(reader.maxDoc(), acceptableTransientOverheadRatio)) {
            final GeoPointTermsEnum iter = new GeoPointTermsEnum(builder.buildFromTerms(OrdinalsBuilder.wrapNumeric64Bit(terms.iterator())));
            Long hashedPoint;
            long numTerms = 0;
            while ((hashedPoint = iter.next()) != null) {
                indexedPoints = BigArrays.NON_RECYCLING_INSTANCE.resize(indexedPoints, numTerms + 1);
                indexedPoints.set(numTerms++, hashedPoint);
            }
            indexedPoints = BigArrays.NON_RECYCLING_INSTANCE.resize(indexedPoints, numTerms);

            Ordinals build = builder.build(fieldDataType.getSettings());
            RandomAccessOrds ordinals = build.ordinals();
            if (!(FieldData.isMultiValued(ordinals) || CommonSettings.getMemoryStorageHint(fieldDataType) == CommonSettings
                    .MemoryStorageFormat.ORDINALS)) {
                int maxDoc = reader.maxDoc();
                LongArray sIndexedPoint = BigArrays.NON_RECYCLING_INSTANCE.newLongArray(reader.maxDoc());
                for (int i=0; i<maxDoc; ++i) {
                    ordinals.setDocument(i);
                    long nativeOrdinal = ordinals.nextOrd();
                    if (nativeOrdinal != RandomAccessOrds.NO_MORE_ORDS) {
                        sIndexedPoint.set(i, indexedPoints.get(nativeOrdinal));
                    }
                }
                BitSet set = builder.buildDocsWithValuesSet();
                data = new GeoPointArrayAtomicFieldData.Single(sIndexedPoint, set);
            } else {
                data = new GeoPointArrayAtomicFieldData.WithOrdinals(indexedPoints, build, reader.maxDoc());
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