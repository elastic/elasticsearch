/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.util.function.Function;

public class VectorIndexFieldData implements IndexFieldData<VectorDVLeafFieldData> {

    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;
    private final IndexVersion indexVersion;
    private final ElementType elementType;
    private final int dims;
    private final boolean indexed;
    private final Function<LeafReader, LeafReader> readerWrapper;

    public VectorIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        IndexVersion indexVersion,
        ElementType elementType,
        int dims,
        boolean indexed,
        Function<LeafReader, LeafReader> readerWrapper
    ) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
        this.indexVersion = indexVersion;
        this.elementType = elementType;
        this.dims = dims;
        this.indexed = indexed;
        this.readerWrapper = readerWrapper;
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
        return new VectorDVLeafFieldData(readerWrapper.apply(context.reader()), fieldName, indexVersion, elementType, dims, indexed);
    }

    @Override
    public VectorDVLeafFieldData loadDirect(LeafReaderContext context) {
        return load(context);
    }

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final ValuesSourceType valuesSourceType;
        private final IndexVersion indexVersion;
        private final ElementType elementType;
        private final int dims;
        private final boolean indexed;
        private final Function<LeafReader, LeafReader> readerWrapper;

        public Builder(
            String name,
            ValuesSourceType valuesSourceType,
            IndexVersion indexVersion,
            ElementType elementType,
            int dims,
            boolean indexed,
            Function<LeafReader, LeafReader> readerWrapper
        ) {
            this.name = name;
            this.valuesSourceType = valuesSourceType;
            this.indexVersion = indexVersion;
            this.elementType = elementType;
            this.dims = dims;
            this.indexed = indexed;
            this.readerWrapper = readerWrapper;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new VectorIndexFieldData(name, valuesSourceType, indexVersion, elementType, dims, indexed, readerWrapper);
        }
    }
}
