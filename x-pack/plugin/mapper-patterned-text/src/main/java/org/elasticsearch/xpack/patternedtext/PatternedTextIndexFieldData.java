/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractIndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
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
    final SortedSetOrdinalsIndexFieldData[] optimizedArgsFieldData;
    final SortedNumericIndexFieldData timestampFieldData;

    static class Builder implements IndexFieldData.Builder {

        final String name;
        final SortedSetOrdinalsIndexFieldData.Builder templateFieldDataBuilder;
        final SortedSetOrdinalsIndexFieldData.Builder argsFieldDataBuilder;
        final SortedSetOrdinalsIndexFieldData.Builder[] optimizedArgsFieldDataBuilder;
        final SortedNumericIndexFieldData.Builder timestampFieldDataBuilder;

        Builder(
            String name,
            SortedSetOrdinalsIndexFieldData.Builder templateFieldData,
            SortedSetOrdinalsIndexFieldData.Builder argsFieldData,
            SortedSetOrdinalsIndexFieldData.Builder[] optimizedArgsFieldData,
            SortedNumericIndexFieldData.Builder timestampFieldData
        ) {
            this.name = name;
            this.templateFieldDataBuilder = templateFieldData;
            this.argsFieldDataBuilder = argsFieldData;
            this.optimizedArgsFieldDataBuilder = optimizedArgsFieldData;
            this.timestampFieldDataBuilder = timestampFieldData;
        }

        public PatternedTextIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            SortedSetOrdinalsIndexFieldData templateFieldData = templateFieldDataBuilder.build(cache, breakerService);
            SortedSetOrdinalsIndexFieldData argsFieldData = argsFieldDataBuilder.build(cache, breakerService);
            SortedSetOrdinalsIndexFieldData[] optimizedArgsFieldData =
                new SortedSetOrdinalsIndexFieldData[optimizedArgsFieldDataBuilder.length];
            for (int i = 0; i < optimizedArgsFieldData.length; i++) {
                optimizedArgsFieldData[i] = optimizedArgsFieldDataBuilder[i].build(cache, breakerService);
            }
            SortedNumericIndexFieldData timestampFieldData = timestampFieldDataBuilder.build(cache, breakerService);
            ToScriptFieldFactory<SortedSetDocValues> factory = (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n);
            return new PatternedTextIndexFieldData(
                name,
                cache,
                breakerService,
                factory,
                templateFieldData,
                argsFieldData,
                optimizedArgsFieldData,
                timestampFieldData
            );
        }
    }

    PatternedTextIndexFieldData(
        String name,
        IndexFieldDataCache cache,
        CircuitBreakerService breakerService,
        ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory,
        SortedSetOrdinalsIndexFieldData templateFieldData,
        SortedSetOrdinalsIndexFieldData argsFieldData,
        SortedSetOrdinalsIndexFieldData[] optimizedArgsFieldData,
        SortedNumericIndexFieldData timestampFieldData
    ) {
        super(name, KEYWORD, cache, breakerService, toScriptFieldFactory);
        this.templateFieldData = templateFieldData;
        this.argsFieldData = argsFieldData;
        this.optimizedArgsFieldData = optimizedArgsFieldData;
        this.timestampFieldData = timestampFieldData;
    }

    @Override
    public LeafOrdinalsFieldData load(LeafReaderContext context) {
        return loadDirect(context);
    }

    @Override
    public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) {
        LeafOrdinalsFieldData leafTemplateFieldData = templateFieldData.loadDirect(context);
        LeafOrdinalsFieldData leafArgsFieldData = argsFieldData.loadDirect(context);
        LeafOrdinalsFieldData[] leafOptimizedArgsFieldData = new LeafOrdinalsFieldData[optimizedArgsFieldData.length];
        LeafNumericFieldData leafTimestampFieldData = timestampFieldData.loadDirect(context);
        for (int i = 0; i < leafOptimizedArgsFieldData.length; i++) {
            leafOptimizedArgsFieldData[i] = optimizedArgsFieldData[i].loadDirect(context);
        }

        return new AbstractLeafOrdinalsFieldData(toScriptFieldFactory) {
            @Override
            public SortedSetDocValues getOrdinalsValues() {
                SortedSetDocValues templateDocValues = leafTemplateFieldData.getOrdinalsValues();
                SortedSetDocValues argsDocValues = leafArgsFieldData.getOrdinalsValues();
                SortedSetDocValues[] optimizedArgsDocValues = new SortedSetDocValues[PatternedTextFieldMapper.OPTIMIZED_ARG_COUNT];
                SortedNumericDocValues timestampDocValues = leafTimestampFieldData.getLongValues();
                for (int i = 0; i < optimizedArgsDocValues.length; i++) {
                    optimizedArgsDocValues[i] = leafOptimizedArgsFieldData[i].getOrdinalsValues();
                }
                return new PatternedTextDocValues(templateDocValues, argsDocValues, optimizedArgsDocValues, timestampDocValues);
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
