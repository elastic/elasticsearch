/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
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


public class PatternedTextIndexFieldData implements IndexFieldData<LeafFieldData> {

    private final SortedSetOrdinalsIndexFieldData templateFieldData;
    private final SortedSetOrdinalsIndexFieldData argsFieldData;
    private final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;
    private final String name;

    static class Builder implements IndexFieldData.Builder {

        final String name;
        final SortedSetOrdinalsIndexFieldData.Builder templateFieldDataBuilder;
        final SortedSetOrdinalsIndexFieldData.Builder argsFieldDataBuilder;

        Builder(
            String name,
            SortedSetOrdinalsIndexFieldData.Builder templateFieldData,
            SortedSetOrdinalsIndexFieldData.Builder argsFieldData
        ) {
            this.name = name;
            this.templateFieldDataBuilder = templateFieldData;
            this.argsFieldDataBuilder = argsFieldData;
        }

        public PatternedTextIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            SortedSetOrdinalsIndexFieldData templateFieldData = templateFieldDataBuilder.build(cache, breakerService);
            SortedSetOrdinalsIndexFieldData argsFieldData = argsFieldDataBuilder.build(cache, breakerService);
            ToScriptFieldFactory<SortedBinaryDocValues> factory = KeywordDocValuesField::new;
            return new PatternedTextIndexFieldData(name, factory, templateFieldData, argsFieldData);
        }
    }

    PatternedTextIndexFieldData(
        String name,
        ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory,
        SortedSetOrdinalsIndexFieldData templateFieldData,
        SortedSetOrdinalsIndexFieldData argsFieldData
    ) {
        this.name = name;
        this.templateFieldData = templateFieldData;
        this.argsFieldData = argsFieldData;
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public String getFieldName() {
        return name;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return null;
    }

    @Override
    public LeafFieldData load(LeafReaderContext context) {
        return loadDirect(context);
    }

    @Override
    public LeafFieldData loadDirect(LeafReaderContext context) {
        LeafOrdinalsFieldData leafTemplateFieldData = templateFieldData.loadDirect(context);
        LeafOrdinalsFieldData leafArgsFieldData = argsFieldData.loadDirect(context);
        return new LeafFieldData() {
            @Override
            public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                return toScriptFieldFactory.getScriptFieldFactory(getBytesValues(), name);
            }

            @Override
            public SortedBinaryDocValues getBytesValues() {
                SortedSetDocValues templateDocValues = leafTemplateFieldData.getOrdinalsValues();
                SortedSetDocValues argsDocValues = leafArgsFieldData.getOrdinalsValues();
                var docValues = new PatternedTextDocValues(templateDocValues, argsDocValues);
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
                return leafTemplateFieldData.ramBytesUsed() + leafArgsFieldData.ramBytesUsed();
            }
        };
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        return templateFieldData.sortField(missingValue, sortMode, nested, reverse);
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
