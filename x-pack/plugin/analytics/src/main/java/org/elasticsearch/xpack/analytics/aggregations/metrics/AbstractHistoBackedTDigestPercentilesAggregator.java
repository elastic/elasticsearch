/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.xpack.analytics.aggregations.support.HistogramValuesSource;

import java.io.IOException;
import java.util.Map;

abstract class AbstractHistoBackedTDigestPercentilesAggregator extends NumericMetricsAggregator.MultiValue {

    private static int indexOfKey(double[] keys, double key) {
        return ArrayUtils.binarySearch(keys, key, 0.001);
    }

    protected final double[] keys;
    protected final ValuesSource valuesSource;
    protected final DocValueFormat formatter;
    protected ObjectArray<TDigestState> states;
    protected final double compression;
    protected final boolean keyed;

    AbstractHistoBackedTDigestPercentilesAggregator(String name, ValuesSource valuesSource, AggregationContext context, Aggregator parent,
                                         double[] keys, double compression, boolean keyed, DocValueFormat formatter,
                                         Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSource;
        this.keyed = keyed;
        this.formatter = formatter;
        this.states = context.bigArrays().newObjectArray(1);
        this.keys = keys;
        this.compression = compression;
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
        final HistogramValues values = ((HistogramValuesSource.Histogram)valuesSource).getHistogramValues(ctx);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                TDigestState state = getExistingOrNewHistogram(bigArrays(), bucket);
                if (values.advanceExact(doc)) {
                    final HistogramValue sketch = values.histogram();
                    while(sketch.next()) {
                        state.add(sketch.value(), sketch.count());
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
        return indexOfKey(keys, Double.parseDouble(name)) >= 0;
    }

    protected TDigestState getState(long bucketOrd) {
        if (bucketOrd >= states.size()) {
            return null;
        }
        final TDigestState state = states.get(bucketOrd);
        return state;
    }

    @Override
    protected void doClose() {
        Releasables.close(states);
    }

}
