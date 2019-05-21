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

package org.elasticsearch.index.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;


public class VectorDVIndexFieldData extends DocValuesIndexFieldData implements IndexFieldData<VectorDVAtomicFieldData> {
    private final boolean isDense;

    public VectorDVIndexFieldData(Index index, String fieldName, boolean isDense) {
        super(index, fieldName);
        this.isDense = isDense;
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        throw new IllegalArgumentException("can't sort on the vector field");
    }

    @Override
    public VectorDVAtomicFieldData load(LeafReaderContext context) {
        return new VectorDVAtomicFieldData(context.reader(), fieldName, isDense);
    }

    @Override
    public VectorDVAtomicFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    public static class Builder implements IndexFieldData.Builder {
        private final boolean isDense;
        public Builder(boolean isDense) {
            this.isDense = isDense;
        }

        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            final String fieldName = fieldType.name();
            return new VectorDVIndexFieldData(indexSettings.getIndex(), fieldName, isDense);
        }

    }
}
