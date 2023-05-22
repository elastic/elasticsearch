/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.analytics.aggregations.support.HistogramValuesSource;

import java.io.IOException;
import java.util.Map;

public class BoxplotAggregator extends NumericMetricsAggregator.MultiValue {

    private final ValuesSource valuesSource;
    private final DocValueFormat format;
    protected ObjectArray<TDigestState> states;
    protected final double compression;

    BoxplotAggregator(
        String name,
        ValuesSourceConfig config,
        DocValueFormat formatter,
        double compression,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert config.hasValues();
        this.valuesSource = config.getValuesSource();
        this.format = formatter;
        this.compression = compression;
        states = context.bigArrays().newObjectArray(1);
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource instanceof HistogramValuesSource.Histogram) {
            final HistogramValues values = ((HistogramValuesSource.Histogram) valuesSource).getHistogramValues(
                aggCtx.getLeafReaderContext()
            );
            return new LeafBucketCollectorBase(sub, values) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    TDigestState state = getExistingOrNewHistogram(bigArrays(), bucket);
                    if (values.advanceExact(doc)) {
                        final HistogramValue sketch = values.histogram();
                        while (sketch.next()) {
                            state.add(sketch.value(), sketch.count());
                        }
                    }
                }
            };
        } else {
            final SortedNumericDoubleValues values = ((ValuesSource.Numeric) valuesSource).doubleValues(aggCtx.getLeafReaderContext());
            return new LeafBucketCollectorBase(sub, values) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    states = bigArrays().grow(states, bucket + 1);
                    if (values.advanceExact(doc)) {
                        TDigestState state = getExistingOrNewHistogram(bigArrays(), bucket);
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
        return InternalBoxplot.Metrics.hasMetric(name);
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        TDigestState state = null;
        if (owningBucketOrd < states.size()) {
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
            return new InternalBoxplot(name, state, format, metadata());
        }
    }

    TDigestState getState(long bucketOrd) {
        if (bucketOrd >= states.size()) {
            return null;
        }
        return states.get(bucketOrd);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalBoxplot.empty(name, compression, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(states);
    }
}
