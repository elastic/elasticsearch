/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.xpack.analytics.topmetrics.InternalTopMetrics.MetricValue;
import org.elasticsearch.xpack.core.common.search.aggregations.MissingHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregationBuilder.REGISTRY_KEY;

/**
 * Collects the {@code top_metrics} aggregation, which functions like a memory
 * efficient but limited version of the {@code top_hits} aggregation. Amortized,
 * each bucket should take something like 16 bytes. Because of this, unlike
 * {@code top_hits}, you can sort by the buckets of this metric.
 *
 * This extends {@linkplain NumericMetricsAggregator.MultiValue} as a compromise
 * to allow sorting on the metric. Right now it only collects a single metric
 * but we expect it to collect a list of them in the future. Also in the future
 * we expect it to allow collecting non-numeric metrics which'll change how we
 * do the inheritance. Finally, we also expect it to allow collecting more than
 * one document worth of metrics. Once that happens we'll need to come up with
 * some way to pick which document's metrics to use for the sort.
 */
class TopMetricsAggregator extends NumericMetricsAggregator.MultiValue {
    private final int size;
    private final BucketedSort sort;
    private final Metrics metrics;

    TopMetricsAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        int size,
        SortBuilder<?> sort,
        MetricValues[] metricValues
    ) throws IOException {
        super(name, context, parent, metadata);
        this.size = size;
        this.metrics = new TopMetricsAggregator.Metrics(metricValues);
        /*
         * If we're only collecting a single value then only provided *that*
         * value to the sort so that swaps and loads are just a little faster
         * in that *very* common case.
         */
        BucketedSort.ExtraData values = metrics.values.length == 1 ? metrics.values[0] : metrics;
        this.sort = context.buildBucketedSort(sort, size, values);
    }

    @Override
    public boolean hasMetric(String name) {
        if (size != 1) {
            throw new IllegalArgumentException("[top_metrics] can only the be target if [size] is [1] but was [" + size + "]");
        }
        for (MetricValues values : metrics.values) {
            if (values.name.equals(name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        assert size == 1;
        /*
         * Since size is always 1 we know that the index into the values
         * array is same same as the bucket ordinal. Also, this will always
         * be called after we've collected a bucket, so it won't just fetch
         * garbage.
         */
        return metrics.metric(name, owningBucketOrd);
    }

    @Override
    public ScoreMode scoreMode() {
        boolean needs = sort.needsScores() || metrics.needsScores();
        return needs ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        assert sub.isNoop() : "Expected noop but was " + sub.toString();

        BucketedSort.Leaf leafSort = sort.forLeaf(ctx);

        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                leafSort.collect(doc, bucket);
            }

            @Override
            public void setScorer(Scorable s) throws IOException {
                leafSort.setScorer(s);
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        List<InternalTopMetrics.TopMetric> topMetrics = sort.getValues(bucket, metrics.resultBuilder(sort.getFormat()));
        assert topMetrics.size() <= size;
        return new InternalTopMetrics(name, sort.getOrder(), metrics.names(), size, topMetrics, metadata());
    }

    @Override
    public InternalTopMetrics buildEmptyAggregation() {
        return InternalTopMetrics.buildEmptyAggregation(name, metrics.names(), metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(sort, metrics);
    }

    static class Metrics implements BucketedSort.ExtraData, Releasable {
        private final MetricValues[] values;

        Metrics(MetricValues[] values) {
            this.values = values;
        }

        boolean needsScores() {
            for (int i = 0; i < values.length; i++) {
                if (values[i].needsScores()) {
                    return true;
                }
            }
            return false;
        }

        double metric(String name, long index) {
            for (MetricValues value : values) {
                if (value.name.equals(name)) {
                    return value.doubleValue(index);
                }
            }
            throw new IllegalArgumentException("[" + name + "] not found");
        }

        BucketedSort.ResultBuilder<InternalTopMetrics.TopMetric> resultBuilder(DocValueFormat sortFormat) {
            return (index, sortValue) -> {
                List<InternalTopMetrics.MetricValue> result = new ArrayList<>(values.length);
                for (int i = 0; i < values.length; i++) {
                    result.add(values[i].metricValue(index));
                }
                return new InternalTopMetrics.TopMetric(sortFormat, sortValue, result);
            };
        }

        List<String> names() {
            return Arrays.stream(values).map(v -> v.name).collect(toList());
        }

        @Override
        public void swap(long lhs, long rhs) {
            for (int i = 0; i < values.length; i++) {
                values[i].swap(lhs, rhs);
            }
        }

        @Override
        public Loader loader(LeafReaderContext ctx) throws IOException {
            Loader[] loaders = new Loader[values.length];
            for (int i = 0; i < values.length; i++) {
                loaders[i] = values[i].loader(ctx);
            }
            return (index, doc) -> {
                for (int i = 0; i < loaders.length; i++) {
                    loaders[i].loadFromDoc(index, doc);
                }
            };
        }

        @Override
        public void close() {
            Releasables.close(values);
        }
    }

    @FunctionalInterface
    interface MetricValuesSupplier {
        MetricValues build(int size, BigArrays bigArrays, String name, ValuesSourceConfig config);
    }

    abstract static class MetricValues implements BucketedSort.ExtraData, Releasable {
        protected final String name;

        MetricValues(String name) {
            this.name = name;
        }

        abstract boolean needsScores();

        abstract double doubleValue(long index);

        abstract InternalTopMetrics.MetricValue metricValue(long index) throws IOException;
    }

    private abstract static class CollectingMetricValues extends MetricValues {
        protected final BigArrays bigArrays;
        protected final ValuesSourceConfig config;

        CollectingMetricValues(BigArrays bigArrays, String name, ValuesSourceConfig config) {
            super(name);
            this.bigArrays = bigArrays;
            this.config = config;
        }

        @Override
        public final boolean needsScores() {
            return config.getValuesSource().needsScores();
        }
    }

    static MetricValues buildMetricValues(
        ValuesSourceRegistry registry,
        BigArrays bigArrays,
        int size,
        String name,
        ValuesSourceConfig config
    ) {
        if (false == config.hasValues()) {
            // `config` doesn't have the name if the
            return new AlwaysNullMetricValues(name);
        }
        MetricValuesSupplier supplier = registry.getAggregator(REGISTRY_KEY, config);
        return supplier.build(size, bigArrays, name, config);
    }

    static MetricValues buildNumericMetricValues(int size, BigArrays bigArrays, String name, ValuesSourceConfig config) {
        ValuesSource.Numeric numeric = (ValuesSource.Numeric) config.getValuesSource();
        if (numeric.isFloatingPoint()) {
            return new DoubleMetricValues(size, bigArrays, name, config);
        }
        return new LongMetricValues(size, bigArrays, name, config);
    }

    /**
     * Loads metrics for floating point numbers.
     */
    static class DoubleMetricValues extends CollectingMetricValues {
        private final ValuesSource.Numeric valuesSource;
        private DoubleArray values;

        DoubleMetricValues(int size, BigArrays bigArrays, String name, ValuesSourceConfig config) {
            super(bigArrays, name, config);
            valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            values = bigArrays.newDoubleArray(size, false);
        }

        @Override
        public double doubleValue(long index) {
            return values.get(index);
        }

        @Override
        public MetricValue metricValue(long index) {
            double value = values.get(index);
            if (Double.isNaN(value)) {
                // Use NaN as a sentinel for "missing"
                return null;
            }
            return new MetricValue(config.format(), SortValue.from(value));
        }

        @Override
        public void swap(long lhs, long rhs) {
            double tmp = values.get(lhs);
            values.set(lhs, values.get(rhs));
            values.set(rhs, tmp);
        }

        @Override
        public Loader loader(LeafReaderContext ctx) throws IOException {
            // TODO allow configuration of value mode
            NumericDoubleValues metricValues = MultiValueMode.AVG.select(valuesSource.doubleValues(ctx));
            return (index, doc) -> {
                if (index >= values.size()) {
                    values = bigArrays.grow(values, index + 1);
                }
                // Use NaN as a sentinel for "missing"
                double metricValue = metricValues.advanceExact(doc) ? metricValues.doubleValue() : Double.NaN;
                values.set(index, metricValue);
            };
        }

        @Override
        public void close() {
            values.close();
        }
    }

    /**
     * Loads metrics for whole numbers.
     */
    static class LongMetricValues extends CollectingMetricValues {
        private final ValuesSource.Numeric valuesSource;
        /**
         * Tracks "missing" values in a {@link BitArray}. Unlike
         * {@link DoubleMetricValues}, we there isn't a sentinel value
         * that we can steel from the longs to represent missing that
         * won't lead to more trouble than it is worth. So we track
         * "missing" values explicitly.
         */
        private final MissingHelper empty;
        private LongArray values;

        LongMetricValues(int size, BigArrays bigArrays, String name, ValuesSourceConfig config) {
            super(bigArrays, name, config);
            valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            empty = new MissingHelper(bigArrays);
            values = bigArrays.newLongArray(size, false);
        }

        @Override
        public double doubleValue(long index) {
            if (empty.isEmpty(index)) {
                return Double.NaN;
            }
            return values.get(index);
        }

        @Override
        public MetricValue metricValue(long index) {
            if (empty.isEmpty(index)) {
                return null;
            }
            return new MetricValue(config.format(), SortValue.from(values.get(index)));
        }

        @Override
        public void swap(long lhs, long rhs) {
            long tmp = values.get(lhs);
            values.set(lhs, values.get(rhs));
            values.set(rhs, tmp);
            empty.swap(lhs, rhs);
        }

        @Override
        public Loader loader(LeafReaderContext ctx) throws IOException {
            // TODO allow configuration of value mode
            NumericDocValues metricValues = MultiValueMode.AVG.select(valuesSource.longValues(ctx));
            return (index, doc) -> {
                if (false == metricValues.advanceExact(doc)) {
                    empty.markMissing(index);
                    return;
                }
                if (index >= values.size()) {
                    values = bigArrays.grow(values, index + 1);
                }
                values.set(index, metricValues.longValue());
                empty.markNotMissing(index);
            };
        }

        @Override
        public void close() {
            Releasables.close(values, empty);
        }
    }

    /**
     * Loads metrics fields with segment ordinals.
     */
    static class SegmentOrdsValues extends CollectingMetricValues {
        private final ValuesSource.Bytes.WithOrdinals valuesSource;
        /**
         * Reference to the segment's doc values so we can convert the
         * {@link #segmentOrds} into strings on the way out.
         */
        private ObjectArray<SortedSetDocValues> segmentResolve;
        /**
         * Terms ordinal in the segment.
         */
        private LongArray segmentOrds;

        SegmentOrdsValues(int size, BigArrays bigArrays, String name, ValuesSourceConfig config) {
            super(bigArrays, name, config);
            if (false == config.hasOrdinals()) {
                throw new IllegalArgumentException(
                    "top_metrics can only collect bytes that have segment ordinals but " + config.getDescription() + " does not"
                );
            }
            valuesSource = (ValuesSource.Bytes.WithOrdinals) config.getValuesSource();
            segmentResolve = bigArrays.newObjectArray(size);
            segmentOrds = bigArrays.newLongArray(size, false);
        }

        @Override
        public double doubleValue(long index) {
            throw new IllegalArgumentException("pipeline aggregations may not refer to non-numeric metrics collected by top_metrics");
        }

        @Override
        public MetricValue metricValue(long index) throws IOException {
            long ord = segmentOrds.get(index);
            if (ord == -1) {
                return null;
            }
            SortedSetDocValues resolve = segmentResolve.get(index);
            return new MetricValue(config.format(), SortValue.from(BytesRef.deepCopyOf(resolve.lookupOrd(ord))));
        }

        @Override
        public void swap(long lhs, long rhs) {
            SortedSetDocValues tempSegmentResolve = segmentResolve.get(lhs);
            segmentResolve.set(lhs, segmentResolve.get(rhs));
            segmentResolve.set(rhs, tempSegmentResolve);
            long tmpSegmentOrd = segmentOrds.get(lhs);
            segmentOrds.set(lhs, segmentOrds.get(rhs));
            segmentOrds.set(rhs, tmpSegmentOrd);
        }

        @Override
        public Loader loader(LeafReaderContext ctx) throws IOException {
            SortedSetDocValues segmentOrdValues = valuesSource.ordinalsValues(ctx);
            // For now just return the value that sorts first.
            return (index, doc) -> {
                if (index >= segmentResolve.size()) {
                    segmentResolve = bigArrays.grow(segmentResolve, index + 1);
                }
                if (index >= segmentOrds.size()) {
                    segmentOrds = bigArrays.grow(segmentOrds, index + 1);
                }
                if (false == segmentOrdValues.advanceExact(doc)) {
                    segmentResolve.set(index, null);
                    segmentOrds.set(index, -1);
                    return;
                }
                segmentResolve.set(index, segmentOrdValues);
                segmentOrds.set(index, segmentOrdValues.nextOrd());
            };
        }

        @Override
        public void close() {
            Releasables.close(segmentResolve, segmentOrds);
        }
    }

    /**
     * {@linkplain MetricValues} implementation for unmapped fields
     * that always returns {@code null} or {@code NaN}.
     */
    static class AlwaysNullMetricValues extends MetricValues {
        AlwaysNullMetricValues(String name) {
            super(name);
        }

        @Override
        public double doubleValue(long index) {
            return Double.NaN;
        }

        @Override
        public MetricValue metricValue(long index) {
            return null;
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public void swap(long lhs, long rhs) {}

        @Override
        public Loader loader(LeafReaderContext ctx) throws IOException {
            return (index, doc) -> {};
        }

        @Override
        public void close() {}
    }

}
