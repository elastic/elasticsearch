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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
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
    private final boolean indexCreatedBefore22;

    public static class Builder implements IndexFieldData.Builder {
        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            return new GeoPointArrayIndexFieldData(indexSettings, fieldType.name(), fieldType.fieldDataType(), cache,
                    breakerService, fieldType.fieldDataType().getSettings()
                    .getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).before(Version.V_2_2_0) ||
                    indexSettings.getIndexVersionCreated().before(Version.V_2_2_0));
        }
    }

    public GeoPointArrayIndexFieldData(IndexSettings indexSettings, String fieldName,
                                       FieldDataType fieldDataType, IndexFieldDataCache cache, CircuitBreakerService breakerService,
                                       final boolean indexCreatedBefore22) {
        super(indexSettings, fieldName, fieldDataType, cache);
        this.breakerService = breakerService;
        this.indexCreatedBefore22 = indexCreatedBefore22;
    }

    @Override
    public AtomicGeoPointFieldData loadDirect(LeafReaderContext context) throws Exception {
        LeafReader reader = context.reader();

        Terms terms = reader.terms(getFieldName());
        AtomicGeoPointFieldData data = null;
        // TODO: Use an actual estimator to estimate before loading.
        NonEstimatingEstimator estimator = new NonEstimatingEstimator(breakerService.getBreaker(CircuitBreaker.FIELDDATA));
        if (terms == null) {
            data = AbstractAtomicGeoPointFieldData.empty(reader.maxDoc());
            estimator.afterLoad(null, data.ramBytesUsed());
            return data;
        }
        return (indexCreatedBefore22 == true) ? loadLegacyFieldData(reader, estimator, terms, data) : loadFieldData22(reader, estimator, terms, data);
    }

    /**
     * long encoded geopoint field data
     */
    private AtomicGeoPointFieldData loadFieldData22(LeafReader reader, NonEstimatingEstimator estimator, Terms terms,
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

    /**
     * Backward compatibility support for legacy lat/lon double arrays
     */
    private AtomicGeoPointFieldData loadLegacyFieldData(LeafReader reader, NonEstimatingEstimator estimator, Terms terms,
                                                        AtomicGeoPointFieldData data) throws Exception {
        DoubleArray lat = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(128);
        DoubleArray lon = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(128);
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        boolean success = false;
        try (OrdinalsBuilder builder = new OrdinalsBuilder(terms.size(), reader.maxDoc(), acceptableTransientOverheadRatio)) {
            final GeoPointTermsEnumLegacy iter = new GeoPointTermsEnumLegacy(builder.buildFromTerms(terms.iterator()));
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
                data = new GeoPointArrayLegacyAtomicFieldData.Single(sLon, sLat, set);
            } else {
                data = new GeoPointArrayLegacyAtomicFieldData.WithOrdinals(lon, lat, build, reader.maxDoc());
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