/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

public class RankVectorsIndexFieldData implements IndexFieldData<RankVectorsDVLeafFieldData> {
    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;
    private final int dims;
    private final DenseVectorFieldMapper.ElementType elementType;

    public RankVectorsIndexFieldData(
        String fieldName,
        int dims,
        ValuesSourceType valuesSourceType,
        DenseVectorFieldMapper.ElementType elementType
    ) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
        this.elementType = elementType;
        this.dims = dims;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    public RankVectorsDVLeafFieldData load(LeafReaderContext context) {
        return new RankVectorsDVLeafFieldData(context.reader(), fieldName, elementType, dims);
    }

    @Override
    public RankVectorsDVLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        throw new IllegalArgumentException(
            "Field [" + fieldName + "] of type [" + RankVectorsFieldMapper.CONTENT_TYPE + "] doesn't support sort"
        );
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }

    public static class Builder implements IndexFieldData.Builder {

        private final String name;
        private final ValuesSourceType valuesSourceType;
        private final int dims;
        private final DenseVectorFieldMapper.ElementType elementType;

        public Builder(String name, ValuesSourceType valuesSourceType, int dims, DenseVectorFieldMapper.ElementType elementType) {
            this.name = name;
            this.valuesSourceType = valuesSourceType;
            this.dims = dims;
            this.elementType = elementType;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new RankVectorsIndexFieldData(name, dims, valuesSourceType, elementType);
        }
    }
}
