/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValuesSeparateCounts;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

public class BytesBinaryIndexFieldData implements IndexFieldData<MultiValuedBinaryDVLeafFieldData> {

    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;
    protected final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;

    public BytesBinaryIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory
    ) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
        this.toScriptFieldFactory = toScriptFieldFactory;
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
        // TODO: support sorts on binary fields (keyword fields can be stored in binary fields as of #138548,
        // and we should support sorts on keyword fields)
        throw new IllegalArgumentException("can't sort on binary field");
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
        throw new IllegalArgumentException("can't sort on binary field");
    }

    @Override
    public MultiValuedBinaryDVLeafFieldData load(LeafReaderContext context) {
        try {
            NumericDocValues counts = DocValues.getNumeric(context.reader(), fieldName + ".counts");
            BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);

            final SortedBinaryDocValues sortedBinaryDocValues;
            if (counts == null) {
                sortedBinaryDocValues = new MultiValuedSortedBinaryDocValues(values);
            } else {
                sortedBinaryDocValues = new MultiValuedSortedBinaryDocValuesSeparateCounts(values, counts);
            }
            return new MultiValuedBinaryDVLeafFieldData(sortedBinaryDocValues, toScriptFieldFactory);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public MultiValuedBinaryDVLeafFieldData loadDirect(LeafReaderContext context) {
        return load(context);
    }

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;
        private final ValuesSourceType valuesSourceType;

        public Builder(String name, ValuesSourceType valuesSourceType, ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory) {
            this.name = name;
            this.valuesSourceType = valuesSourceType;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            // Ignore breaker
            return new BytesBinaryIndexFieldData(name, valuesSourceType, toScriptFieldFactory);
        }
    }
}
