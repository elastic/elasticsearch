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
import org.elasticsearch.common.util.BigDoubleArrayList;
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
        final BigDoubleArrayList lat = new BigDoubleArrayList();
        final BigDoubleArrayList lon = new BigDoubleArrayList();
        lat.add(0); // first "t" indicates null value
        lon.add(0); // first "t" indicates null value
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        boolean success = false;
        try (OrdinalsBuilder builder = new OrdinalsBuilder(terms.size(), reader.maxDoc(), acceptableTransientOverheadRatio)) {
            final GeoPointEnum iter = new GeoPointEnum(builder.buildFromTerms(terms.iterator(null)));
            GeoPoint point;
            while ((point = iter.next()) != null) {
                lat.add(point.getLat());
                lon.add(point.getLon());
            }

            Ordinals build = builder.build(fieldDataType.getSettings());
            if (!(build.isMultiValued() || CommonSettings.getMemoryStorageHint(fieldDataType) == CommonSettings.MemoryStorageFormat.ORDINALS)) {
                Docs ordinals = build.ordinals();
                int maxDoc = reader.maxDoc();
                BigDoubleArrayList sLat = new BigDoubleArrayList(reader.maxDoc());
                BigDoubleArrayList sLon = new BigDoubleArrayList(reader.maxDoc());
                for (int i = 0; i < maxDoc; i++) {
                    long nativeOrdinal = ordinals.getOrd(i);
                    sLat.add(lat.get(nativeOrdinal));
                    sLon.add(lon.get(nativeOrdinal));
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