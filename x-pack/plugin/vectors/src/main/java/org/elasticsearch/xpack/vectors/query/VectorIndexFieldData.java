/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper;

public class VectorIndexFieldData implements IndexFieldData<VectorDVLeafFieldData> {

    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;
    private final Version indexVersion;
    private final int dims;
    private final boolean indexed;

    public VectorIndexFieldData(String fieldName, ValuesSourceType valuesSourceType, Version indexVersion, int dims, boolean indexed) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
        this.indexVersion = indexVersion;
        this.dims = dims;
        this.indexed = indexed;
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
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        throw new IllegalArgumentException(
            "Field [" + fieldName + "] of type [" + DenseVectorFieldMapper.CONTENT_TYPE + "] doesn't support sort"
        );
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }

    @Override
    public VectorDVLeafFieldData load(LeafReaderContext context) {
        return new VectorDVLeafFieldData(context.reader(), fieldName, indexVersion, dims, indexed);
    }

    @Override
    public VectorDVLeafFieldData loadDirect(LeafReaderContext context) {
        return load(context);
    }

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final ValuesSourceType valuesSourceType;
        private final Version indexVersion;
        private final int dims;
        private final boolean indexed;

        public Builder(String name, ValuesSourceType valuesSourceType, Version indexVersion, int dims, boolean indexed) {
            this.name = name;
            this.valuesSourceType = valuesSourceType;
            this.indexVersion = indexVersion;
            this.dims = dims;
            this.indexed = indexed;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new VectorIndexFieldData(name, valuesSourceType, indexVersion, dims, indexed);
        }
    }
}
