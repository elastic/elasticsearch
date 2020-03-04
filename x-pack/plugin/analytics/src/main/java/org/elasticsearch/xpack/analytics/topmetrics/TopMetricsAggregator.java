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
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
    private final String metricName;
    private final BucketedSort sort;
    private final Values values;
    private final ValuesSource.Numeric metricValueSource;

    TopMetricsAggregator(String name, SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData, int size, String metricName,
            SortBuilder<?> sort, ValuesSource.Numeric metricValueSource) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.size = size;
        this.metricName = metricName;
        this.metricValueSource = metricValueSource;
        if (metricValueSource != null) {
            values = new Values(size, context.bigArrays(), metricValueSource);
            this.sort = sort.buildBucketedSort(context.getQueryShardContext(), size, values);
        } else {
            values = null;
            this.sort = null;
        }
    }

    @Override
    public boolean hasMetric(String name) {
        if (size != 1) {
            throw new IllegalArgumentException("[top_metrics] can only the be target if [size] is [1] but was [" + size + "]");
        }
        return metricName.equals(name);
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
        return values.values.get(owningBucketOrd);
    }

    @Override
    public ScoreMode scoreMode() {
        boolean needs = (sort != null && sort.needsScores()) || (metricValueSource != null && metricValueSource.needsScores());
        return needs ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        assert sub == LeafBucketCollector.NO_OP_COLLECTOR : "Expected noop but was " + sub.toString();

        if (metricValueSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
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
        if (metricValueSource == null) {
            return buildEmptyAggregation();
        }
        List<InternalTopMetrics.TopMetric> topMetrics = sort.getValues(bucket, values.resultBuilder(sort.getFormat()));
        assert topMetrics.size() <= size;
        return new InternalTopMetrics(name, sort.getOrder(), metricName, size, topMetrics, pipelineAggregators(), metaData());
    }

    @Override
    public InternalTopMetrics buildEmptyAggregation() {
        // The sort format and sort order aren't used in reduction so we pass the simplest thing.
        return InternalTopMetrics.buildEmptyAggregation(name, metricName, pipelineAggregators(),
                metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(sort, values);
    }

    private static class Values implements BucketedSort.ExtraData, Releasable {
        private final BigArrays bigArrays;
        private final ValuesSource.Numeric metricValueSource;

        private DoubleArray values;

        Values(int size, BigArrays bigArrays, ValuesSource.Numeric metricValueSource) {
            this.bigArrays = bigArrays;
            this.metricValueSource = metricValueSource;
            values = bigArrays.newDoubleArray(size, false);
        }

        BucketedSort.ResultBuilder<InternalTopMetrics.TopMetric> resultBuilder(DocValueFormat sortFormat) {
            return (index, sortValue) ->
                new InternalTopMetrics.TopMetric(sortFormat, sortValue, values.get(index));
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
}
