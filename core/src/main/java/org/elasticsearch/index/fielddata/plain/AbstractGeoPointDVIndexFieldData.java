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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

public abstract class AbstractGeoPointDVIndexFieldData extends DocValuesIndexFieldData implements IndexGeoPointFieldData {

    AbstractGeoPointDVIndexFieldData(Index index, String fieldName) {
        super(index, fieldName);
    }

    @Override
    public final XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        throw new IllegalArgumentException("can't sort on geo_point field without using specific sorting feature, like geo_distance");
    }

    /**
     * Lucene 5.4 GeoPointFieldType
     */
    public static class GeoPointDVIndexFieldData extends AbstractGeoPointDVIndexFieldData {
        final boolean indexCreatedBefore2x;

        public GeoPointDVIndexFieldData(Index index, String fieldName, final boolean indexCreatedBefore2x) {
            super(index, fieldName);
            this.indexCreatedBefore2x = indexCreatedBefore2x;
        }

        @Override
        public AtomicGeoPointFieldData load(LeafReaderContext context) {
            try {
                if (indexCreatedBefore2x) {
                    return new GeoPointLegacyDVAtomicFieldData(DocValues.getBinary(context.reader(), fieldName));
                }
                return new GeoPointDVAtomicFieldData(DocValues.getSortedNumeric(context.reader(), fieldName));
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public AtomicGeoPointFieldData loadDirect(LeafReaderContext context) throws Exception {
            return load(context);
        }
    }

    public static class Builder implements IndexFieldData.Builder {
        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            if (indexSettings.getIndexVersionCreated().before(Version.V_2_2_0)
                    && fieldType.hasDocValues() == false) {
                return new GeoPointArrayIndexFieldData(indexSettings, fieldType.name(), cache, breakerService);
            }
            // Ignore breaker
            return new GeoPointDVIndexFieldData(indexSettings.getIndex(), fieldType.name(),
                    indexSettings.getIndexVersionCreated().before(Version.V_2_2_0));
        }
    }
}
