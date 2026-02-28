/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.DoublesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.IntsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.NumericDvSingletonOrSorted;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;

import java.io.IOException;
import java.util.EnumMap;

public class AggregateMetricDoubleBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final DoublesBlockLoader minLoader;
    private final DoublesBlockLoader maxLoader;
    private final DoublesBlockLoader sumLoader;
    private final IntsBlockLoader countLoader;

    AggregateMetricDoubleBlockLoader(EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType> metricsRequested) {
        minLoader = getDoublesBlockLoader(AggregateMetricDoubleFieldMapper.Metric.min, metricsRequested);
        maxLoader = getDoublesBlockLoader(AggregateMetricDoubleFieldMapper.Metric.max, metricsRequested);
        sumLoader = getDoublesBlockLoader(AggregateMetricDoubleFieldMapper.Metric.sum, metricsRequested);
        countLoader = getIntsBlockLoader(AggregateMetricDoubleFieldMapper.Metric.value_count, metricsRequested);
    }

    private static DoublesBlockLoader getDoublesBlockLoader(
        AggregateMetricDoubleFieldMapper.Metric metric,
        EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType> metricsRequested
    ) {
        if (metricsRequested.containsKey(metric) == false) {
            return null;
        }
        var toLoad = metricsRequested.get(metric);
        return new DoublesBlockLoader(toLoad.name(), NumericUtils::sortableLongToDouble);
    }

    private static IntsBlockLoader getIntsBlockLoader(
        AggregateMetricDoubleFieldMapper.Metric metric,
        EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType> metricsRequested
    ) {
        if (metricsRequested.containsKey(metric) == false) {
            return null;
        }
        var toLoad = metricsRequested.get(metric);
        return new IntsBlockLoader(toLoad.name());
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        ColumnAtATimeReader minReader = null;
        ColumnAtATimeReader maxReader = null;
        ColumnAtATimeReader sumReader = null;
        ColumnAtATimeReader countReader = null;
        boolean success = false;
        try {
            minReader = minLoader != null ? minLoader.reader(breaker, context) : null;
            maxReader = maxLoader != null ? maxLoader.reader(breaker, context) : null;
            sumReader = sumLoader != null ? sumLoader.reader(breaker, context) : null;
            countReader = countLoader != null ? countLoader.reader(breaker, context) : null;
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(minReader, maxReader, sumReader, countReader);
            }
        }

        return new Reader(minReader, maxReader, sumReader, countReader);
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.aggregateMetricDoubleBuilder(expectedCount);
    }

    private static class Reader implements ColumnAtATimeReader {
        private final ColumnAtATimeReader minReader;
        private final ColumnAtATimeReader maxReader;
        private final ColumnAtATimeReader sumReader;
        private final ColumnAtATimeReader countReader;

        private Reader(
            ColumnAtATimeReader minReader,
            ColumnAtATimeReader maxReader,
            ColumnAtATimeReader sumReader,
            ColumnAtATimeReader countReader
        ) {
            this.minReader = minReader;
            this.maxReader = maxReader;
            this.sumReader = sumReader;
            this.countReader = countReader;
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
                int count = docs.count() - offset;
                minBlock = minReader != null ? minReader.read(factory, docs, offset, nullsFiltered) : factory.constantNulls(count);
                maxBlock = maxReader != null ? maxReader.read(factory, docs, offset, nullsFiltered) : factory.constantNulls(count);
                sumBlock = sumReader != null ? sumReader.read(factory, docs, offset, nullsFiltered) : factory.constantNulls(count);
                countBlock = countReader != null ? countReader.read(factory, docs, offset, nullsFiltered) : factory.constantNulls(count);
                Block block = factory.buildAggregateMetricDoubleDirect(minBlock, maxBlock, sumBlock, countBlock);
                success = true;
                return block;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(minBlock, maxBlock, sumBlock, countBlock);
                }
            }
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return (minReader == null || minReader.canReuse(startingDocID))
                && (maxReader == null || maxReader.canReuse(startingDocID))
                && (sumReader == null || sumReader.canReuse(startingDocID))
                && (countReader == null || countReader.canReuse(startingDocID));
        }

        @Override
        public void close() {
            Releasables.close(minReader, maxReader, sumReader, countReader);
        }
    }

    public static class AvgBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
        NumberFieldMapper.NumberFieldType sumFieldType;
        NumberFieldMapper.NumberFieldType countFieldType;

        AvgBlockLoader(EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType> availableMetrics) {
            if (availableMetrics.containsKey(AggregateMetricDoubleFieldMapper.Metric.sum) == false
                || availableMetrics.containsKey(AggregateMetricDoubleFieldMapper.Metric.value_count) == false) {
                sumFieldType = null;
                countFieldType = null;
            } else {
                sumFieldType = availableMetrics.get(AggregateMetricDoubleFieldMapper.Metric.sum);
                countFieldType = availableMetrics.get(AggregateMetricDoubleFieldMapper.Metric.value_count);
            }
        }

        @Override
        public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
            if (sumFieldType == null || countFieldType == null) {
                return ConstantNull.READER;
            }
            NumericDvSingletonOrSorted dvSumValues = NumericDvSingletonOrSorted.get(breaker, context, sumFieldType.name());
            NumericDvSingletonOrSorted dvValueCountValues = NumericDvSingletonOrSorted.get(breaker, context, countFieldType.name());
            if (dvSumValues == null || dvValueCountValues == null) {
                return ConstantNull.READER;
            }
            assert dvSumValues.sorted() == null && dvValueCountValues.sorted() == null
                : "aggregate metric doubles shouldn't have multi-values";

            TrackingNumericDocValues trackingSumValues = dvSumValues.singleton();
            TrackingNumericDocValues trackingValueCountValues = dvValueCountValues.singleton();
            var sumValues = trackingSumValues.docValues();
            var valueCountValues = trackingValueCountValues.docValues();

            return new BlockDocValuesReader(breaker) {
                @Override
                public void close() {
                    Releasables.close(trackingSumValues, trackingValueCountValues);
                }

                private int docID = -1;

                @Override
                protected int docId() {
                    return docID;
                }

                @Override
                public String toString() {
                    return "BlockDocValuesReader.AggregateMetricDoubleAvg";
                }

                @Override
                public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
                    int expectedCount = docs.count() - offset;
                    if (sumValues == null || valueCountValues == null) {
                        return factory.constantNulls(expectedCount);
                    }
                    try (DoubleBuilder builder = factory.doublesFromDocValues(expectedCount)) {
                        int lastDoc = -1;

                        for (int i = offset; i < docs.count(); i++) {
                            int doc = docs.get(i);
                            if (doc < lastDoc) {
                                throw new IllegalStateException("docs within same block must be in order");
                            }
                            if (sumValues.advanceExact(doc) && valueCountValues.advanceExact(doc)) {
                                this.docID = doc;
                                lastDoc = doc;
                                double sum = NumericUtils.sortableLongToDouble(sumValues.longValue());
                                long count = valueCountValues.longValue();
                                builder.appendDouble(sum / count);
                            } else {
                                builder.appendNull();
                            }
                        }
                        return builder.build();
                    }
                }

                @Override
                public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                    DoubleBuilder blockBuilder = (DoubleBuilder) builder;
                    if (sumValues.advanceExact(docId) && valueCountValues.advanceExact(docId)) {
                        this.docID = docId;
                        var sum = NumericUtils.sortableLongToDouble(sumValues.longValue());
                        var count = valueCountValues.longValue();
                        blockBuilder.appendDouble(sum / count);
                    } else {
                        blockBuilder.appendNull();
                    }
                }
            };
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            throw new UnsupportedOperationException("AvgBlockLoader does not have a corresponding builder");
        }
    }
}
