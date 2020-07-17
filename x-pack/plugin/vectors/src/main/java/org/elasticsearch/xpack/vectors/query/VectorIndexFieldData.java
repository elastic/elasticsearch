/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;


public class VectorIndexFieldData implements IndexFieldData<VectorDVLeafFieldData> {

    protected final String fieldName;
    private final boolean isDense;
    protected final ValuesSourceType valuesSourceType;

    public VectorIndexFieldData(String fieldName, boolean isDense, ValuesSourceType valuesSourceType) {
        this.fieldName = fieldName;
        this.isDense = isDense;
        this.valuesSourceType = valuesSourceType;
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    public final void clear() {
        // can't do
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        throw new IllegalArgumentException("can't sort on the vector field");
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, Nested nested,
            SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }

    @Override
    public VectorDVLeafFieldData load(LeafReaderContext context) {
        return new VectorDVLeafFieldData(context.reader(), fieldName, isDense);
    }

    @Override
    public VectorDVLeafFieldData loadDirect(LeafReaderContext context) {
        return load(context);
    }

    public static class Builder implements IndexFieldData.Builder {
        private final boolean isDense;
        private final ValuesSourceType valuesSourceType;
        public Builder(boolean isDense, ValuesSourceType valuesSourceType) {
            this.isDense = isDense;
            this.valuesSourceType = valuesSourceType;
        }

        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            final String fieldName = fieldType.name();
            return new VectorIndexFieldData(fieldName, isDense, valuesSourceType);
        }

    }
}
