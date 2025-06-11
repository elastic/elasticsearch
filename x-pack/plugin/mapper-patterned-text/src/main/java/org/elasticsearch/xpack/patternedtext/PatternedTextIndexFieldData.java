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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractIndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import static org.elasticsearch.search.aggregations.support.CoreValuesSourceType.KEYWORD;

public class PatternedTextIndexFieldData extends AbstractIndexOrdinalsFieldData {

    final SortedSetOrdinalsIndexFieldData templateFieldData;
    final SortedSetOrdinalsIndexFieldData argsFieldData;

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
            ToScriptFieldFactory<SortedSetDocValues> factory = (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n);
            return new PatternedTextIndexFieldData(name, cache, breakerService, factory, templateFieldData, argsFieldData);
        }
    }

    PatternedTextIndexFieldData(
        String name,
        IndexFieldDataCache cache,
        CircuitBreakerService breakerService,
        ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory,
        SortedSetOrdinalsIndexFieldData templateFieldData,
        SortedSetOrdinalsIndexFieldData argsFieldData
    ) {
        super(name, KEYWORD, cache, breakerService, toScriptFieldFactory);
        this.templateFieldData = templateFieldData;
        this.argsFieldData = argsFieldData;
    }

    @Override
    public LeafOrdinalsFieldData load(LeafReaderContext context) {
        return loadDirect(context);
    }

    @Override
    public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) {
        LeafOrdinalsFieldData leafTemplateFieldData = templateFieldData.loadDirect(context);
        LeafOrdinalsFieldData leafArgsFieldData = argsFieldData.loadDirect(context);

        return new AbstractLeafOrdinalsFieldData(toScriptFieldFactory) {
            @Override
            public SortedSetDocValues getOrdinalsValues() {
                SortedSetDocValues templateDocValues = leafTemplateFieldData.getOrdinalsValues();
                SortedSetDocValues argsDocValues = leafArgsFieldData.getOrdinalsValues();
                return new PatternedTextDocValues(templateDocValues, argsDocValues);
            }

            @Override
            public long ramBytesUsed() {
                return 0; // unknown
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
