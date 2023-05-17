/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.analytics.aggregations.support.HistogramValuesSource;

import java.io.IOException;
import java.util.Map;

abstract class AbstractHistoBackedHDRPercentilesAggregator extends NumericMetricsAggregator.MultiValue {

    protected final double[] keys;
    protected final ValuesSource valuesSource;
    protected final DocValueFormat format;
    protected ObjectArray<DoubleHistogram> states;
    protected final int numberOfSignificantValueDigits;
    protected final boolean keyed;

    AbstractHistoBackedHDRPercentilesAggregator(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        Aggregator parent,
        double[] keys,
        int numberOfSignificantValueDigits,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert config.hasValues();
        this.valuesSource = config.getValuesSource();
        this.keyed = keyed;
        this.format = formatter;
        this.states = context.bigArrays().newObjectArray(1);
        this.keys = keys;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        final HistogramValues values = ((HistogramValuesSource.Histogram) valuesSource).getHistogramValues(aggCtx.getLeafReaderContext());

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                DoubleHistogram state = getExistingOrNewHistogram(bigArrays(), bucket);
                if (values.advanceExact(doc)) {
                    final HistogramValue sketch = values.histogram();
                    while (sketch.next()) {
                        state.recordValueWithCount(sketch.value(), sketch.count());
                    }
                }
            }
        };
    }

    private DoubleHistogram getExistingOrNewHistogram(final BigArrays bigArrays, long bucket) {
        states = bigArrays.grow(states, bucket + 1);
        DoubleHistogram state = states.get(bucket);
        if (state == null) {
            state = new DoubleHistogram(numberOfSignificantValueDigits);
            /* Set the histogram to autosize so it can resize itself as
               the data range increases. Resize operations should be
               rare as the histogram buckets are exponential (on the top
               level). In the future we could expose the range as an
               option on the request so the histogram can be fixed at
               initialisation and doesn't need resizing.
             */
            state.setAutoResize(true);
            states.set(bucket, state);
        }
        return state;
    }

    @Override
    public boolean hasMetric(String name) {
        return PercentilesConfig.indexOfKey(keys, Double.parseDouble(name)) >= 0;
    }

    protected DoubleHistogram getState(long bucketOrd) {
        if (bucketOrd >= states.size()) {
            return null;
        }
        final DoubleHistogram state = states.get(bucketOrd);
        return state;
    }

    @Override
    protected void doClose() {
        Releasables.close(states);
    }

}
