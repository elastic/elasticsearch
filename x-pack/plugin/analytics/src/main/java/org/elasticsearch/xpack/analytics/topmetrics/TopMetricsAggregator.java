/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortValue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class TopMetricsAggregator extends MetricsAggregator {
    private final BucketedSort sort;
    private final String metricName;
    private final ValuesSource.Numeric metricValueSource;
    private DoubleArray values;

    TopMetricsAggregator(String name, SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData, BucketedSort sort,
            String metricName, ValuesSource.Numeric metricValueSource) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.sort = sort;
        this.metricName = metricName;
        this.metricValueSource = metricValueSource;
        if (metricValueSource != null) {
            values = context.bigArrays().newDoubleArray(2, false);
            values.fill(0, values.size(), Double.NaN);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        boolean needs = (sort != null && sort.needsScores()) || (metricValueSource != null && metricValueSource.needsScores());
        return needs ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        assert sub == null;
        if (metricValueSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        BucketedSort.Leaf leafSort = sort.forLeaf(ctx);
        // TODO allow configuration of value mode
        NumericDoubleValues metricValues = MultiValueMode.AVG.select(metricValueSource.doubleValues(ctx));

        return new LeafBucketCollector() { // TODO do we need to extend *Base*? It doesn't look like we use it.
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (leafSort.collectIfCompetitive(doc, bucket)) {
                    if (bucket >= values.size()) {
                        long oldSize = values.size();
                        values = context.bigArrays().grow(values, bucket + 1);
                        values.fill(oldSize, values.size(), Double.NaN);
                    }
                    double metricValue = metricValues.advanceExact(doc) ? metricValues.doubleValue() : Double.NaN; 
                    values.set(bucket, metricValue);
                }
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
        double metricValue = values.get(bucket);
        SortValue sortValue = sort.getValue(bucket);
        return new InternalTopMetrics(name, sort.getFormat(), sort.getOrder(), sortValue, metricName, metricValue, pipelineAggregators(),
                metaData());
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
}
