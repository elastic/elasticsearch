/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.UncheckedIOException;

public class SingletonBinaryDocValuesFieldData implements IndexFieldData<LeafFieldData> {

    private final String fieldName;

    static class Builder implements IndexFieldData.Builder {

        final String fieldName;

        Builder(String fieldName) {
            this.fieldName = fieldName;
        }

        public SingletonBinaryDocValuesFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new SingletonBinaryDocValuesFieldData(fieldName);
        }
    }

    SingletonBinaryDocValuesFieldData(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return null;
    }

    @Override
    public LeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public LeafFieldData loadDirect(LeafReaderContext context) throws IOException {
        LeafReader leafReader = context.reader();
        var values = DocValues.getBinary(leafReader, fieldName);
        return new LeafFieldData() {
            final ToScriptFieldFactory<SortedBinaryDocValues> factory = KeywordDocValuesField::new;

            @Override
            public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                return factory.getScriptFieldFactory(getBytesValues(), name);
            }

            @Override
            public SortedBinaryDocValues getBytesValues() {
                return FieldData.singleton(values);
            }

            @Override
            public long ramBytesUsed() {
                return 1L;
            }
        };
    }


    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
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
}
