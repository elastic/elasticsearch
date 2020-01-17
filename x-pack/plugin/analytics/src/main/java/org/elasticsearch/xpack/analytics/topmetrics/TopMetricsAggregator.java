/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class TopMetricsAggregator extends MetricsAggregator {
    private final FieldComparator<? extends Number> sortComparator;
    private final SortOrder sortOrder;
    private final DocValueFormat sortFormat;
    private final boolean sortNeedsScores;
    private final String metricName;
    private final ValuesSource.Numeric metricValueSource;
    private DoubleArray values;

    TopMetricsAggregator(String name, SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData,
            FieldComparator<? extends Number> sortComparator, SortOrder sortOrder, DocValueFormat sortFormat, boolean sortNeedsScores,
            String metricName, ValuesSource.Numeric metricValueSource) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.sortComparator = sortComparator;
        this.sortOrder = sortOrder;
        this.sortFormat = sortFormat;
        this.sortNeedsScores = sortNeedsScores;
        this.metricName = metricName;
        this.metricValueSource = metricValueSource;
        if (metricValueSource != null) {
            values = context.bigArrays().newDoubleArray(2, false);
            values.fill(0, values.size(), Double.NaN);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        boolean needs = sortNeedsScores || (metricValueSource != null && metricValueSource.needsScores());
        return needs ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (metricValueSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        // TODO allow configuration of value mode
        NumericDoubleValues metricValues = MultiValueMode.AVG.select(metricValueSource.doubleValues(ctx));

        LeafFieldComparator leafCmp = sortComparator.getLeafComparator(ctx);
        int reverseMul = sortOrder == SortOrder.DESC ? -1 : 1;
        return new LeafBucketCollectorBase(sub, metricValues) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                long offset = bucket * 2;
                if (offset + 2 > values.size()) {
                    long oldSize = values.size();
                    values = context.bigArrays().grow(values, offset + 2);
                    values.fill(oldSize, values.size(), Double.NaN);
                }

                double bestSort = values.get(offset);
                // This generates a Double instance and throws it away. Sad, but it is the price we pay for using Lucene APIs.
                leafCmp.copy(0, doc);
                double sort = sortComparator.value(0).doubleValue();

                // The explicit check to NaN is important here because NaN should *always* lose
                if (Double.isNaN(bestSort) || reverseMul * Double.compare(bestSort, sort) > 0) {
                    values.set(offset, sort);
                    // TODO support for missing values
                    double metricValue = metricValues.advanceExact(doc) ? metricValues.doubleValue() : Double.NaN; 
                    values.set(offset + 1, metricValue);
                }
            }

            @Override
            public void setScorer(Scorable s) throws IOException {
                leafCmp.setScorer(s);
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        if (metricValueSource == null) {
            return buildEmptyAggregation();
        }
        long offset = bucket * 2;
        double sortValue = values.get(offset);
        double metricValue = values.get(offset + 1);
        return new InternalTopMetrics(name, sortFormat, sortOrder, sortValue, metricName, metricValue, pipelineAggregators(), metaData());
    }

    @Override
    public InternalTopMetrics buildEmptyAggregation() {
        return InternalTopMetrics.buildEmptyAggregation(name, sortFormat, sortOrder, metricName, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(values);
    }
}
