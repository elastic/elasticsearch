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
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PagedMutable;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.DistanceUnit.Distance;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

/**
 */
public class GeoPointCompressedIndexFieldData extends AbstractGeoPointIndexFieldData {

    private static final String PRECISION_KEY = "precision";
    private static final Distance DEFAULT_PRECISION_VALUE = new Distance(1, DistanceUnit.CENTIMETERS);
    private final CircuitBreakerService breakerService;

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService, GlobalOrdinalsBuilder globalOrdinalBuilder) {
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
            final GeoPointEnum iter = new GeoPointEnum(builder.buildFromTerms(terms.iterator(null)));
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
            if (build.isMultiValued() || CommonSettings.getMemoryStorageHint(fieldDataType) == CommonSettings.MemoryStorageFormat.ORDINALS) {
                if (lat.size() != build.getMaxOrd()) {
                    lat = lat.resize(build.getMaxOrd());
                    lon = lon.resize(build.getMaxOrd());
                }
                data = new GeoPointCompressedAtomicFieldData.WithOrdinals(encoding, lon, lat, build);
            } else {
                Docs ordinals = build.ordinals();
                int maxDoc = reader.maxDoc();
                PagedMutable sLat = new PagedMutable(reader.maxDoc(), pageSize, encoding.numBitsPerCoordinate(), PackedInts.COMPACT);
                PagedMutable sLon = new PagedMutable(reader.maxDoc(), pageSize, encoding.numBitsPerCoordinate(), PackedInts.COMPACT);
                final long missing = encoding.encodeCoordinate(0);
                for (int i = 0; i < maxDoc; i++) {
                    final long nativeOrdinal = ordinals.getOrd(i);
                    if (nativeOrdinal != Ordinals.MISSING_ORDINAL) {
                        sLat.set(i, lat.get(nativeOrdinal));
                        sLon.set(i, lon.get(nativeOrdinal));
                    } else {
                        sLat.set(i, missing);
                        sLon.set(i, missing);
                    }
                }
                FixedBitSet set = builder.buildDocsWithValuesSet();
                if (set == null) {
                    data = new GeoPointCompressedAtomicFieldData.Single(encoding, sLon, sLat, ordinals.getMaxOrd() - Ordinals.MIN_ORDINAL);
                } else {
                    data = new GeoPointCompressedAtomicFieldData.SingleFixedSet(encoding, sLon, sLat, set, ordinals.getMaxOrd() - Ordinals.MIN_ORDINAL);
                }
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