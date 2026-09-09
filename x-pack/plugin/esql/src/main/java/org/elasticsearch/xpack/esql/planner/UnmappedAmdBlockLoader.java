/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder.Metric;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockStoredFieldsReader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceFormat;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Reads an unmapped field from {@code _source} as {@code aggregate_metric_double}. Missing values
 * are null; a present value that is not an AMD object fails the query.
 */
final class UnmappedAmdBlockLoader implements BlockLoader {

    private final String fieldName;
    private final Set<String> sourcePaths;
    private final IgnoredSourceFormat ignoredSourceFormat;

    UnmappedAmdBlockLoader(String fieldName, Set<String> sourcePaths, IgnoredSourceFormat ignoredSourceFormat) {
        this.fieldName = fieldName;
        this.sourcePaths = sourcePaths;
        this.ignoredSourceFormat = ignoredSourceFormat;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.aggregateMetricDoubleBuilder(expectedCount);
    }

    @Override
    public IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
        return null;
    }

    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) {
        return new Reader(breaker, fieldName);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return StoredFieldsSpec.withSourcePaths(ignoredSourceFormat, sourcePaths);
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "UnmappedAmdBlockLoader[" + fieldName + "]";
    }

    private static final class Reader extends BlockStoredFieldsReader {
        private final String fieldName;

        Reader(CircuitBreaker breaker, String fieldName) {
            super(breaker);
            this.fieldName = fieldName;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            appendAmd((AggregateMetricDoubleBuilder) builder, fieldName, storedFields.source().extractValue(fieldName, null));
        }

        @Override
        public String toString() {
            return "UnmappedAmdBlockLoader.Reader[" + fieldName + "]";
        }
    }

    private static void appendAmd(AggregateMetricDoubleBuilder builder, String fieldName, Object value) {
        if (value == null) {
            builder.appendNull();
            return;
        }
        if (value instanceof Map<?, ?> map) {
            appendDouble(builder.min(), map.get(Metric.MIN.getLabel()));
            appendDouble(builder.max(), map.get(Metric.MAX.getLabel()));
            appendDouble(builder.sum(), map.get(Metric.SUM.getLabel()));
            appendCount(builder.count(), map.get(Metric.COUNT.getLabel()));
            return;
        }
        throw new IllegalArgumentException(
            Strings.format("Cannot load field [%s] as aggregate_metric_double from _source; got [%s]", fieldName, value)
        );
    }

    private static void appendDouble(BlockLoader.DoubleBuilder builder, Object value) {
        if (value instanceof Number n) {
            builder.appendDouble(n.doubleValue());
        } else {
            builder.appendNull();
        }
    }

    private static void appendCount(BlockLoader.IntBuilder builder, Object value) {
        if (value instanceof Number n) {
            builder.appendInt(Math.toIntExact(n.longValue()));
        } else {
            builder.appendNull();
        }
    }
}
