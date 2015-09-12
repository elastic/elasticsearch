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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Names;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

/**
 * A field data implementation that forbids loading and will throw an {@link IllegalStateException} if you try to load
 * {@link AtomicFieldData} instances.
 */
public final class DisabledIndexFieldData extends AbstractIndexFieldData<AtomicFieldData> {

    public static class Builder implements IndexFieldData.Builder {
        @Override
        public IndexFieldData<AtomicFieldData> build(Index index, @IndexSettings Settings indexSettings, MappedFieldType fieldType,
                                                        IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
            // Ignore Circuit Breaker
            return new DisabledIndexFieldData(index, indexSettings, fieldType.names(), fieldType.fieldDataType(), cache);
        }
    }

    public DisabledIndexFieldData(Index index, Settings indexSettings, Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
    }

    @Override
    public AtomicFieldData loadDirect(LeafReaderContext context) throws Exception {
        throw fail();
    }

    @Override
    protected AtomicFieldData empty(int maxDoc) {
        throw fail();
    }

    @Override
    public IndexFieldData.XFieldComparatorSource comparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
        throw fail();
    }

    private IllegalStateException fail() {
        return new IllegalStateException("Field data loading is forbidden on " + getFieldNames().fullName());
    }

}
