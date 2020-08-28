/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.xpack.analytics.topmetrics.InternalTopMetrics.MetricValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

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

    TopMetricsAggregator(String name, SearchContext context, Aggregator parent, Map<String, Object> metadata, int size,
            SortBuilder<?> sort, List<MetricSource> metricSources) throws IOException {
        super(name, context, parent, metadata);
        this.size = size;
        metrics = new Metrics(size, context.getQueryShardContext().bigArrays(), metricSources);
        /*
         * If we're only collecting a single value then only provided *that*
         * value to the sort so that swaps and loads are just a little faster
         * in that *very* common case.
         */
        BucketedSort.ExtraData values = metricSources.size() == 1 ? metrics.values[0] : metrics;
        this.sort = sort.buildBucketedSort(context.getQueryShardContext(), size, values);
    }

    @Override
    public boolean hasMetric(String name) {
        if (size != 1) {
            throw new IllegalArgumentException("[top_metrics] can only the be target if [size] is [1] but was [" + size + "]");
        }
        for (MetricValues values : metrics.values) {
            if (values.name().equals(name)) {
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
        assert sub == LeafBucketCollector.NO_OP_COLLECTOR : "Expected noop but was " + sub.toString();

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

    /**
     * Information about each metric that this {@link Aggregator} uses to
     * load and format metric values.
     */
    static class MetricSource {
        private final String name;
        private final DocValueFormat format;
        private final ValuesSource.Numeric valuesSource;

        MetricSource(String name, DocValueFormat format, ValuesSource.Numeric valuesSource) {
            this.name = name;
            this.format = format;
            this.valuesSource = valuesSource;
        }

        String getName() {
            return name;
        }

        DocValueFormat getFormat() {
            return format;
        }
    }

    static class Metrics implements BucketedSort.ExtraData, Releasable {
        private final MetricValues[] values;

        Metrics(int size, BigArrays bigArrays, List<MetricSource> sources) {
            values = new MetricValues[sources.size()];
            int i = 0;
            for (MetricSource source : sources) {
                values[i++] = valuesFor(size, bigArrays, source);
            }
        }

        private static MetricValues valuesFor(int size, BigArrays bigArrays, MetricSource source) {
            if (source.valuesSource == null) {
                return new AlwaysNullMetricValues(source);
            }
            if (source.valuesSource.isFloatingPoint()) {
                return new DoubleMetricValues(size, bigArrays, source);
            }
            return new LongMetricValues(size, bigArrays, source);
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
                if (value.name().equals(name)) {
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
            return Arrays.stream(values).map(MetricValues::name).collect(toList());
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

    private abstract static class MetricValues implements BucketedSort.ExtraData, Releasable {
        protected final MetricSource source;

        MetricValues(MetricSource source) {
            this.source = source;
        }

        final String name() {
            return source.name;
        }

        abstract boolean needsScores();
        abstract double doubleValue(long index);
        abstract InternalTopMetrics.MetricValue metricValue(long index);
    }

    private abstract static class CollectingMetricValues extends MetricValues {
        protected final BigArrays bigArrays;

        CollectingMetricValues(BigArrays bigArrays, MetricSource source) {
            super(source);
            this.bigArrays = bigArrays;
        }

        @Override
        public final boolean needsScores() {
            return source.valuesSource.needsScores();
        }
    }

    /**
     * Loads metrics for floating point numbers.
     */
    static class DoubleMetricValues extends CollectingMetricValues {
        private DoubleArray values;

        DoubleMetricValues(int size, BigArrays bigArrays, MetricSource source) {
            super(bigArrays, source);
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
            return new MetricValue(source.format, SortValue.from(value));
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
            NumericDoubleValues metricValues = MultiValueMode.AVG.select(source.valuesSource.doubleValues(ctx));
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
        /**
         * Tracks "missing" values in a {@link BitArray}. Unlike
         * {@link DoubleMetricValues}, we there isn't a sentinel value
         * that we can steel from the longs to represent missing that
         * won't lead to more trouble than it is worth. So we track
         * "missing" values explicitly.
         */
        private final MissingHelper empty;
        private LongArray values;

        LongMetricValues(int size, BigArrays bigArrays, MetricSource source) {
            super(bigArrays, source);
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
            return new MetricValue(source.format, SortValue.from(values.get(index)));
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
            NumericDocValues metricValues = MultiValueMode.AVG.select(source.valuesSource.longValues(ctx));
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
     * {@linkplain MetricValues} implementation for unmapped fields
     * that always returns {@code null} or {@code NaN}.
     */
    static class AlwaysNullMetricValues extends MetricValues {
        AlwaysNullMetricValues(MetricSource source) {
            super(source);
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

    /**
     * Helps {@link LongMetricValues} track "empty" slots. It attempts to have
     * very low CPU overhead and no memory overhead when there *aren't* empty
     * values.
     */
    private static class MissingHelper implements Releasable {
        private final BigArrays bigArrays;
        private BitArray tracker;

        MissingHelper(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
        }

        void markMissing(long index) {
            int i = asInt(index);
            if (tracker == null) {
                tracker = new BitArray(i, bigArrays);
            }
            tracker.set(i);
        }

        void markNotMissing(long index) {
            if (tracker == null) {
                return;
            }
            tracker.clear(asInt(index));
        }

        void swap(long lhs, long rhs) {
            if (tracker == null) {
                return;
            }
            int l = asInt(lhs);
            int r = asInt(rhs);
            boolean backup = tracker.get(l);
            if (tracker.get(r)) {
                tracker.set(l);
            } else {
                tracker.clear(l);
            }
            if (backup) {
                tracker.set(r);
            } else {
                tracker.clear(r);
            }
        }

        boolean isEmpty(long index) {
            if (tracker == null) {
                return false;
            }
            return tracker.get(asInt(index));
        }

        @Override
        public void close() {
            if (tracker != null) {
                tracker.close();
            }
        }

        private int asInt(long index) {
            if (index > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("top_metrics can't track more than " + Integer.MAX_VALUE + " values.");
            }
            return (int) index;
        }
    }
}
