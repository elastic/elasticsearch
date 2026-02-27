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
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.DoublesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.IntsBlockLoader;

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
    public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        AllReader minReader = null;
        AllReader maxReader = null;
        AllReader sumReader = null;
        AllReader countReader = null;
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

    private static class Reader implements AllReader {
        private final AllReader minReader;
        private final AllReader maxReader;
        private final AllReader sumReader;
        private final AllReader countReader;

        private Reader(AllReader minReader, AllReader maxReader, AllReader sumReader, AllReader countReader) {
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
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            var blockBuilder = (AggregateMetricDoubleBuilder) builder;
            readSingleRowFromSubblock(docId, storedFields, blockBuilder.min(), minReader);
            readSingleRowFromSubblock(docId, storedFields, blockBuilder.max(), maxReader);
            readSingleRowFromSubblock(docId, storedFields, blockBuilder.sum(), sumReader);
            readSingleRowFromSubblock(docId, storedFields, blockBuilder.count(), countReader);
        }

        private void readSingleRowFromSubblock(int docID, StoredFields storedFields, Builder builder, AllReader reader) throws IOException {
            if (reader == null) {
                builder.appendNull();
            } else {
                reader.read(docID, storedFields, builder);
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
}
