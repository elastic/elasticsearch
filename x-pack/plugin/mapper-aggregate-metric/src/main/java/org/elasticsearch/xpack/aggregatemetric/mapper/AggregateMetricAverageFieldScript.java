/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.field.DoubleDocValuesField;
import org.elasticsearch.script.field.LongDocValuesField;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * This script is using the aggregate metric double metrics sum and value count to calculate the average.
 */
class AggregateMetricAverageFieldScript extends DoubleFieldScript {

    final DoubleDocValuesField sumDocValuesField;
    final LongDocValuesField countDocValuesField;

    AggregateMetricAverageFieldScript(String fieldName, SearchLookup lookup, LeafReaderContext ctx) {
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
        return ctx -> new AggregateMetricAverageFieldScript(fieldName, lookup, ctx);
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

    /**
     * @return a SortField that uses the average script of the aggregate metric double to sort.
     */
    static SortField sortField(
        String fieldName,
        SearchLookup lookup,
        Object missingValue,
        MultiValueMode sortMode,
        IndexFieldData.XFieldComparatorSource.Nested nested,
        boolean reverse
    ) {
        LeafFactory leafFactory = newLeafFactory(fieldName, lookup);
        return new SortField(fieldName, new DoubleValuesComparatorSource(null, missingValue, sortMode, nested) {
            @Override
            protected SortedNumericDoubleValues getValues(LeafReaderContext context) {
                DoubleFieldScript script = leafFactory.newInstance(context);
                return new SortedNumericDoubleValues() {
                    private double[] values = new double[0];
                    private int count;
                    private int index;

                    @Override
                    public boolean advanceExact(int doc) {
                        script.runForDoc(doc);
                        count = script.count();
                        if (count == 0) {
                            index = 0;
                            return false;
                        }
                        if (values.length < count) {
                            values = new double[count];
                        }
                        System.arraycopy(script.values(), 0, values, 0, count);
                        if (count > 1) {
                            Arrays.sort(values, 0, count);
                        }
                        index = 0;
                        return true;
                    }

                    @Override
                    public double nextValue() {
                        return values[index++];
                    }

                    @Override
                    public int docValueCount() {
                        return count;
                    }
                };
            }
        }, reverse);
    }
}
