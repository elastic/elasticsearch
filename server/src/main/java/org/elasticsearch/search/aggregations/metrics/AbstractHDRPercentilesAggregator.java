/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

abstract class AbstractHDRPercentilesAggregator extends NumericMetricsAggregator.MultiDoubleValue {

    protected final double[] keys;
    protected final DocValueFormat format;
    protected ObjectArray<DoubleHistogram> states;
    protected final int numberOfSignificantValueDigits;
    protected final boolean keyed;

    AbstractHDRPercentilesAggregator(
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
        super(name, config, context, parent, metadata);
        assert config.hasValues();
        this.keyed = keyed;
        this.format = formatter;
        this.states = context.bigArrays().newObjectArray(1);
        this.keys = keys;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(SortedNumericDoubleValues values, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final DoubleHistogram state = getExistingOrNewHistogram(bigArrays(), bucket);
                    for (int i = 0; i < values.docValueCount(); i++) {
                        state.recordValue(values.nextValue());
                    }
                }
            }
        };
    }

    @Override
    protected LeafBucketCollector getLeafCollector(NumericDoubleValues values, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final DoubleHistogram state = getExistingOrNewHistogram(bigArrays(), bucket);
                    state.recordValue(values.doubleValue());
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
        return states.get(bucketOrd);
    }

    @Override
    protected void doClose() {
        Releasables.close(states);
    }

}
