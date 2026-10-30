/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.field.DoubleDocValuesField;
import org.elasticsearch.script.field.LongDocValuesField;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.DoubleScriptFieldRangeQuery;
import org.elasticsearch.search.runtime.DoubleScriptFieldTermQuery;
import org.elasticsearch.search.runtime.DoubleScriptFieldTermsQuery;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * This script is using the aggregate metric double metrics sum and value count to calculate the average.
 *
 * All helper methods assume that the `aggregate_metric_double` has the metrics `sum` and `value_count` configured.
 */
class AggregateMetricAverageScript extends DoubleFieldScript {
    private static final Script EMPTY_SCRIPT = new Script("");

    private final DoubleDocValuesField sumDocValuesField;
    private final LongDocValuesField countDocValuesField;

    AggregateMetricAverageScript(String fieldName, SearchLookup lookup, LeafReaderContext ctx) {
        super(fieldName, Map.of(), lookup, OnScriptError.FAIL, ctx);
        try {
            String sumFieldName = AggregateMetricDoubleFieldMapper.subfieldName(fieldName, AggregateMetricDoubleFieldMapper.Metric.sum);
            sumDocValuesField = new DoubleDocValuesField(
                SortedNumericDoubleValues.wrap(DocValues.getSortedNumeric(ctx.reader(), sumFieldName)),
                sumFieldName
            );
            String countFieldName = AggregateMetricDoubleFieldMapper.subfieldName(
                fieldName,
                AggregateMetricDoubleFieldMapper.Metric.value_count
            );
            countDocValuesField = new LongDocValuesField(
                SortedNumericLongValues.wrap(DocValues.getSortedNumeric(ctx.reader(), countFieldName)),
                countFieldName
            );
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }

    static LeafFactory newLeafFactory(String fieldName, SearchLookup lookup) {
        return ctx -> new AggregateMetricAverageScript(fieldName, lookup, ctx);
    }

    static Query doubleRangeQuery(
        String fieldName,
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        SearchLookup lookup
    ) {
        return NumberFieldMapper.NumberType.doubleRangeQuery(
            lowerTerm,
            upperTerm,
            includeLower,
            includeUpper,
            (l, u) -> new DoubleScriptFieldRangeQuery(
                EMPTY_SCRIPT,
                AggregateMetricAverageScript.newLeafFactory(fieldName, lookup),
                fieldName,
                l,
                u
            )
        );
    }

    static Query doubleTermsQuery(String fieldName, Collection<?> values, SearchLookup lookup) {
        Set<Long> terms = Sets.newHashSetWithExpectedSize(values.size());
        for (Object value : values) {
            terms.add(Double.doubleToLongBits(NumberFieldMapper.NumberType.objectToDouble(value)));
        }
        return new DoubleScriptFieldTermsQuery(
            EMPTY_SCRIPT,
            AggregateMetricAverageScript.newLeafFactory(fieldName, lookup),
            fieldName,
            terms
        );
    }

    static Query doubleTermQuery(String fieldName, Object value, SearchLookup lookup) {
        return new DoubleScriptFieldTermQuery(
            EMPTY_SCRIPT,
            AggregateMetricAverageScript.newLeafFactory(fieldName, lookup),
            fieldName,
            NumberFieldMapper.NumberType.objectToDouble(value)
        );
    }

    static SortField sortField(
        String fieldName,
        Object missingValue,
        MultiValueMode sortMode,
        IndexFieldData.XFieldComparatorSource.Nested nested,
        boolean reverse,
        Function<LeafReaderContext, SortedNumericDoubleValues> docValueLoader
    ) {
        return new SortField(fieldName, new DoubleValuesComparatorSource(null, missingValue, sortMode, nested) {
            @Override
            protected SortedNumericDoubleValues getValues(LeafReaderContext context) {
                return docValueLoader.apply(context);
            }
        }, reverse);
    }

    @Override
    protected void emitFromObject(Object v) {
        // we only use doc-values, not _source, so no need to implement this method
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDocument(int docID) {
        try {
            sumDocValuesField.setNextDocId(docID);
            countDocValuesField.setNextDocId(docID);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public void execute() {
        Iterator<Double> sumIterator = sumDocValuesField.iterator();
        Iterator<Long> countIterator = countDocValuesField.iterator();
        while (sumIterator.hasNext() && countIterator.hasNext()) {
            Double sum = sumIterator.next();
            Long count = countIterator.next();
            emit(sum / count);
        }
    }
}
