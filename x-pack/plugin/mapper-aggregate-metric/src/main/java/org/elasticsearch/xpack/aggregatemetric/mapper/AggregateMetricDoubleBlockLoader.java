/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;
import java.util.EnumMap;

public class AggregateMetricDoubleBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    final NumberFieldMapper.NumberFieldType minFieldType;
    final NumberFieldMapper.NumberFieldType maxFieldType;
    final NumberFieldMapper.NumberFieldType sumFieldType;
    final NumberFieldMapper.NumberFieldType countFieldType;

    AggregateMetricDoubleBlockLoader(EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType> metricsRequested) {
        minFieldType = metricsRequested.getOrDefault(AggregateMetricDoubleFieldMapper.Metric.min, null);
        maxFieldType = metricsRequested.getOrDefault(AggregateMetricDoubleFieldMapper.Metric.max, null);
        sumFieldType = metricsRequested.getOrDefault(AggregateMetricDoubleFieldMapper.Metric.sum, null);
        countFieldType = metricsRequested.getOrDefault(AggregateMetricDoubleFieldMapper.Metric.value_count, null);
    }

    private static NumericDocValues getNumericDocValues(NumberFieldMapper.NumberFieldType field, LeafReader leafReader) throws IOException {
        if (field == null) {
            return null;
        }
        String fieldName = field.name();
        var values = leafReader.getNumericDocValues(fieldName);
        if (values != null) {
            return values;
        }

        var sortedValues = leafReader.getSortedNumericDocValues(fieldName);
        return DocValues.unwrapSingleton(sortedValues);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        NumericDocValues minValues = getNumericDocValues(minFieldType, context.reader());
        NumericDocValues maxValues = getNumericDocValues(maxFieldType, context.reader());
        NumericDocValues sumValues = getNumericDocValues(sumFieldType, context.reader());
        NumericDocValues valueCountValues = getNumericDocValues(countFieldType, context.reader());

        return new BlockDocValuesReader() {

            private int docID = -1;

            @Override
            protected int docId() {
                return docID;
            }

            @Override
            public String toString() {
                return "BlockDocValuesReader.AggregateMetricDouble";
            }

            @Override
            public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
                boolean success = false;
                Block minBlock = null;
                Block maxBlock = null;
                Block sumBlock = null;
                Block countBlock = null;
                try {
                    minBlock = readDoubleSubblock(factory, docs, offset, minValues);
                    maxBlock = readDoubleSubblock(factory, docs, offset, maxValues);
                    sumBlock = readDoubleSubblock(factory, docs, offset, sumValues);
                    countBlock = readIntSubblock(factory, docs, offset, valueCountValues);
                    Block block = factory.buildAggregateMetricDoubleDirect(minBlock, maxBlock, sumBlock, countBlock);
                    success = true;
                    return block;
                } finally {
                    if (success == false) {
                        Releasables.closeExpectNoException(minBlock, maxBlock, sumBlock, countBlock);
                    }
                }
            }

            private Block readDoubleSubblock(BlockFactory factory, Docs docs, int offset, NumericDocValues values) throws IOException {
                int count = docs.count() - offset;
                if (values == null) {
                    return factory.constantNulls(count);
                }
                try (DoubleBuilder builder = factory.doubles(count)) {
                    copyDoubleValuesToBuilder(docs, offset, builder, values);
                    return builder.build();
                }
            }

            private Block readIntSubblock(BlockFactory factory, Docs docs, int offset, NumericDocValues values) throws IOException {
                int count = docs.count() - offset;
                if (values == null) {
                    return factory.constantNulls(count);
                }
                try (IntBuilder builder = factory.ints(count)) {
                    copyIntValuesToBuilder(docs, offset, builder, values);
                    return builder.build();
                }
            }

            private void copyDoubleValuesToBuilder(Docs docs, int offset, DoubleBuilder builder, NumericDocValues values)
                throws IOException {
                int lastDoc = -1;
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < lastDoc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (values == null || values.advanceExact(doc) == false) {
                        builder.appendNull();
                    } else {
                        double value = NumericUtils.sortableLongToDouble(values.longValue());
                        lastDoc = doc;
                        this.docID = doc;
                        builder.appendDouble(value);
                    }
                }
            }

            private void copyIntValuesToBuilder(Docs docs, int offset, IntBuilder builder, NumericDocValues values) throws IOException {
                int lastDoc = -1;
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < lastDoc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (values == null || values.advanceExact(doc) == false) {
                        builder.appendNull();
                    } else {
                        int value = Math.toIntExact(values.longValue());
                        lastDoc = doc;
                        this.docID = doc;
                        builder.appendInt(value);
                    }
                }
            }

            @Override
            public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                var blockBuilder = (AggregateMetricDoubleBuilder) builder;
                this.docID = docId;
                readSingleRow(docId, blockBuilder);
            }

            private void readSingleRow(int docId, AggregateMetricDoubleBuilder builder) throws IOException {
                if (minValues != null && minValues.advanceExact(docId)) {
                    builder.min().appendDouble(NumericUtils.sortableLongToDouble(minValues.longValue()));
                } else {
                    builder.min().appendNull();
                }
                if (maxValues != null && maxValues.advanceExact(docId)) {
                    builder.max().appendDouble(NumericUtils.sortableLongToDouble(maxValues.longValue()));
                } else {
                    builder.max().appendNull();
                }
                if (sumValues != null && sumValues.advanceExact(docId)) {
                    builder.sum().appendDouble(NumericUtils.sortableLongToDouble(sumValues.longValue()));
                } else {
                    builder.sum().appendNull();
                }
                if (valueCountValues != null && valueCountValues.advanceExact(docId)) {
                    builder.count().appendInt(Math.toIntExact(valueCountValues.longValue()));
                } else {
                    builder.count().appendNull();
                }
            }
        };
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.aggregateMetricDoubleBuilder(expectedCount);
    }
}
