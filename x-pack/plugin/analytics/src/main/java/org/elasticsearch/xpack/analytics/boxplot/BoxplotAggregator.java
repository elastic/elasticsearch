/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BoxplotAggregator extends NumericMetricsAggregator.MultiValue {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;
    protected ObjectArray<TDigestState> states;
    protected final double compression;

    BoxplotAggregator(String name, ValuesSource.Numeric valuesSource, DocValueFormat formatter, double compression,
                      SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                      Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.format = formatter;
        this.compression = compression;
        if (valuesSource != null) {
            states = context.bigArrays().newObjectArray(1);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                states = bigArrays.grow(states, bucket + 1);

                if (values.advanceExact(doc)) {
                    TDigestState state = getExistingOrNewHistogram(bigArrays, bucket);
                    if (values.advanceExact(doc)) {
                        final int valueCount = values.docValueCount();
                        for (int i = 0; i < valueCount; i++) {
                            state.add(values.nextValue());
                        }
                    }
                }
            }
        };
    }

    private TDigestState getExistingOrNewHistogram(final BigArrays bigArrays, long bucket) {
        states = bigArrays.grow(states, bucket + 1);
        TDigestState state = states.get(bucket);
        if (state == null) {
            state = new TDigestState(compression);
            states.set(bucket, state);
        }
        return state;
    }

    @Override
    public boolean hasMetric(String name) {
        try {
            InternalBoxplot.Metrics.resolve(name);
            return true;
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        TDigestState state = null;
        if (valuesSource != null && owningBucketOrd < states.size()) {
            state = states.get(owningBucketOrd);
        }
        return InternalBoxplot.Metrics.resolve(name).value(state);
    }


    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TDigestState state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalBoxplot(name, state, format, pipelineAggregators(), metaData());
        }
    }

    TDigestState getState(long bucketOrd) {
        if (valuesSource == null || bucketOrd >= states.size()) {
            return null;
        }
        return states.get(bucketOrd);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalBoxplot(name, new TDigestState(compression), format, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(states);
    }
}
