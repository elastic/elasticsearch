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
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PagedMutable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.DistanceUnit.Distance;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

/**
 */
public class GeoPointCompressedIndexFieldData extends AbstractIndexGeoPointFieldData {

    private static final String PRECISION_KEY = "precision";
    private static final Distance DEFAULT_PRECISION_VALUE = new Distance(1, DistanceUnit.CENTIMETERS);
    private final CircuitBreakerService breakerService;

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            FieldDataType type = mapper.fieldDataType();
            final String precisionAsString = type.getSettings().get(PRECISION_KEY);
            final Distance precision;
            if (precisionAsString != null) {
                precision = Distance.parseDistance(precisionAsString);
            } else {
                precision = DEFAULT_PRECISION_VALUE;
            }
            return new GeoPointCompressedIndexFieldData(index, indexSettings, mapper.names(), mapper.fieldDataType(), cache, precision, breakerService);
        }
    }

    private final GeoPointFieldMapper.Encoding encoding;

    public GeoPointCompressedIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames,
                                            FieldDataType fieldDataType, IndexFieldDataCache cache, Distance precision,
                                            CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        this.encoding = GeoPointFieldMapper.Encoding.of(precision);
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
        final long initialSize;
        if (terms.size() >= 0) {
            initialSize = 1 + terms.size();
        } else { // codec doesn't expose size
            initialSize = 1 + Math.min(1 << 12, reader.maxDoc());
        }
        final int pageSize = Integer.highestOneBit(BigArrays.PAGE_SIZE_IN_BYTES * 8 / encoding.numBitsPerCoordinate() - 1) << 1;
        PagedMutable lat = new PagedMutable(initialSize, pageSize, encoding.numBitsPerCoordinate(), PackedInts.COMPACT);
        PagedMutable lon = new PagedMutable(initialSize, pageSize, encoding.numBitsPerCoordinate(), PackedInts.COMPACT);
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        boolean success = false;
        try (OrdinalsBuilder builder = new OrdinalsBuilder(terms.size(), reader.maxDoc(), acceptableTransientOverheadRatio)) {
            final GeoPointEnum iter = new GeoPointEnum(builder.buildFromTerms(terms.iterator()));
            GeoPoint point;
            while ((point = iter.next()) != null) {
                final long ord = builder.currentOrdinal();
                if (lat.size() <= ord) {
                    final long newSize = BigArrays.overSize(ord + 1);
                    lat = lat.resize(newSize);
                    lon = lon.resize(newSize);
                }
                lat.set(ord, encoding.encodeCoordinate(point.getLat()));
                lon.set(ord, encoding.encodeCoordinate(point.getLon()));
            }

            Ordinals build = builder.build(fieldDataType.getSettings());
            RandomAccessOrds ordinals = build.ordinals();
            if (FieldData.isMultiValued(ordinals) || CommonSettings.getMemoryStorageHint(fieldDataType) == CommonSettings.MemoryStorageFormat.ORDINALS) {
                if (lat.size() != ordinals.getValueCount()) {
                    lat = lat.resize(ordinals.getValueCount());
                    lon = lon.resize(ordinals.getValueCount());
                }
                data = new GeoPointCompressedAtomicFieldData.WithOrdinals(encoding, lon, lat, build, reader.maxDoc());
            } else {
                int maxDoc = reader.maxDoc();
                PagedMutable sLat = new PagedMutable(reader.maxDoc(), pageSize, encoding.numBitsPerCoordinate(), PackedInts.COMPACT);
                PagedMutable sLon = new PagedMutable(reader.maxDoc(), pageSize, encoding.numBitsPerCoordinate(), PackedInts.COMPACT);
                final long missing = encoding.encodeCoordinate(0);
                for (int i = 0; i < maxDoc; i++) {
                    ordinals.setDocument(i);
                    final long nativeOrdinal = ordinals.nextOrd();
                    if (nativeOrdinal >= 0) {
                        sLat.set(i, lat.get(nativeOrdinal));
                        sLon.set(i, lon.get(nativeOrdinal));
                    } else {
                        sLat.set(i, missing);
                        sLon.set(i, missing);
                    }
                }
                BitSet set = builder.buildDocsWithValuesSet();
                data = new GeoPointCompressedAtomicFieldData.Single(encoding, sLon, sLat, set);
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