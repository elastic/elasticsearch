/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.query;

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

    public VectorDVIndexFieldData(Index index, String fieldName) {
        super(index, fieldName);
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        throw new IllegalArgumentException("can't sort on the vector field");
    }

    @Override
    public VectorDVAtomicFieldData load(LeafReaderContext context) {
        return new VectorDVAtomicFieldData(context.reader(), fieldName);
    }

    @Override
    public VectorDVAtomicFieldData loadDirect(LeafReaderContext context) {
        return load(context);
    }

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            final String fieldName = fieldType.name();
            return new VectorDVIndexFieldData(indexSettings.getIndex(), fieldName);
        }

    }
}
