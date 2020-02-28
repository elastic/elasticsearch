/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
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

    TopMetricsAggregator(String name, SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData, int size,
            SortBuilder<?> sort, List<MultiValuesSourceFieldConfig> metricFields) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.size = size;
        metrics = new Metrics(size, context.getQueryShardContext(), metricFields);
        /*
         * If we're only collecting a single value then only provided *that*
         * value to the sort so that swaps and loads are just a little faster
         * in that *very* common case.
         */
        BucketedSort.ExtraData values = metricFields.size() == 1 ? metrics.values[0] : metrics;
        this.sort = sort.buildBucketedSort(context.getQueryShardContext(), size, values);
    }

    @Override
    public boolean hasMetric(String name) {
        if (size != 1) {
            throw new IllegalArgumentException("[top_metrics] can only the be target if [size] is [1] but was [" + size + "]");
        }
        return metrics.names.contains(name);
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
        return new InternalTopMetrics(name, sort.getOrder(), metrics.names, size, topMetrics, pipelineAggregators(), metaData());
    }

    @Override
    public InternalTopMetrics buildEmptyAggregation() {
        return InternalTopMetrics.buildEmptyAggregation(name, metrics.names, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(sort, metrics);
    }

    private static class Metrics implements BucketedSort.ExtraData, Releasable {
        private final List<String> names;
        private final MetricValues[] values;

        Metrics(int size, QueryShardContext ctx, List<MultiValuesSourceFieldConfig> fieldsConfig) {
            names = fieldsConfig.stream().map(MultiValuesSourceFieldConfig::getFieldName).collect(toList());
            values = new MetricValues[fieldsConfig.size()];
            int i = 0;
            for (MultiValuesSourceFieldConfig config : fieldsConfig) {
                ValuesSourceConfig<ValuesSource.Numeric> resolved = ValuesSourceConfig.resolve(ctx, ValueType.NUMERIC,
                        config.getFieldName(), config.getScript(), config.getMissing(), config.getTimeZone(), null);
                if (resolved == null) {
                    values[i++] = new MissingMetricValues();
                    continue;
                }
                ValuesSource.Numeric valuesSource = resolved.toValuesSource(ctx);
                if (valuesSource == null) {
                    values[i++] = new MissingMetricValues();
                    continue;
                }
                values[i++] = new CollectMetricValues(size, ctx.bigArrays(), valuesSource);
            }
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
            int valueIndex = names.indexOf(name);
            if (valueIndex < 0) {
                throw new IllegalArgumentException("[" + name + "] not found");
            }
            return values[valueIndex].value(index);
        }

        BucketedSort.ResultBuilder<InternalTopMetrics.TopMetric> resultBuilder(DocValueFormat sortFormat) {
            return (index, sortValue) -> {
                double[] result = new double[values.length];
                for (int i = 0; i < values.length; i++) {
                    result[i] = values[i].value(index);
                }
                return new InternalTopMetrics.TopMetric(sortFormat, sortValue, result);
            };
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

    private interface MetricValues extends BucketedSort.ExtraData, Releasable {
        boolean needsScores();
        double value(long index);
    }
    private static class CollectMetricValues implements MetricValues {
        private final BigArrays bigArrays;
        private final ValuesSource.Numeric metricValueSource;

        private DoubleArray values;

        CollectMetricValues(int size, BigArrays bigArrays, ValuesSource.Numeric metricValueSource) {
            this.bigArrays = bigArrays;
            this.metricValueSource = metricValueSource;
            values = bigArrays.newDoubleArray(size, false);
        }

        @Override
        public boolean needsScores() {
            return metricValueSource.needsScores();
        }

        @Override
        public double value(long index) {
            return values.get(index);
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
            NumericDoubleValues metricValues = MultiValueMode.AVG.select(metricValueSource.doubleValues(ctx));
            return (index, doc) -> {
                if (index >= values.size()) {
                    values = bigArrays.grow(values, index + 1);
                }
                double metricValue = metricValues.advanceExact(doc) ? metricValues.doubleValue() : Double.NaN; 
                values.set(index, metricValue);
            };
        }

        @Override
        public void close() {
            values.close();
        }
    }
    private static class MissingMetricValues implements MetricValues {
        @Override
        public double value(long index) {
            return Double.NaN;
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
        public void close() {
        }
    }
}
