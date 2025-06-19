/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
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

public class PatternedTextIndexFieldData implements IndexFieldData<LeafFieldData> {

    private final PatternedTextFieldType fieldType;

    static class Builder implements IndexFieldData.Builder {

        final PatternedTextFieldType fieldType;

        Builder(PatternedTextFieldType fieldType) {
            this.fieldType = fieldType;
        }

        public PatternedTextIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new PatternedTextIndexFieldData(fieldType);
        }
    }

    PatternedTextIndexFieldData(PatternedTextFieldType fieldType) {
        this.fieldType = fieldType;
    }

    @Override
    public String getFieldName() {
        return fieldType.name();
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
        PatternedTextDocValues docValues = PatternedTextDocValues.from(
            leafReader,
            fieldType.templateFieldName(),
            fieldType.argsFieldName()
        );
        return new LeafFieldData() {

            final ToScriptFieldFactory<SortedBinaryDocValues> factory = KeywordDocValuesField::new;

            @Override
            public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                return factory.getScriptFieldFactory(getBytesValues(), name);
            }

            @Override
            public SortedBinaryDocValues getBytesValues() {
                return new SortedBinaryDocValues() {
                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return docValues.advanceExact(doc);
                    }

                    @Override
                    public int docValueCount() {
                        return 1;
                    }

                    @Override
                    public BytesRef nextValue() throws IOException {
                        return docValues.binaryValue();
                    }
                };
            }

            @Override
            public long ramBytesUsed() {
                return 1L;
            }
        };
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        throw new IllegalArgumentException("not supported for source patterned text field type");
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
