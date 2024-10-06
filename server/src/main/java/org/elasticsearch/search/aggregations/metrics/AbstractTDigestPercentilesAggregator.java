/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

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

abstract class AbstractTDigestPercentilesAggregator extends NumericMetricsAggregator.MultiDoubleValue {

    protected final double[] keys;
    protected final DocValueFormat formatter;
    protected ObjectArray<TDigestState> states;
    protected final double compression;
    protected final TDigestExecutionHint executionHint;
    protected final boolean keyed;

    AbstractTDigestPercentilesAggregator(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        Aggregator parent,
        double[] keys,
        double compression,
        TDigestExecutionHint executionHint,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, context, parent, metadata);
        assert config.hasValues();
        this.keyed = keyed;
        this.formatter = formatter;
        this.states = context.bigArrays().newObjectArray(1);
        this.keys = keys;
        this.compression = compression;
        this.executionHint = executionHint;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(SortedNumericDoubleValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final TDigestState state = getExistingOrNewHistogram(bigArrays(), bucket);
                    for (int i = 0; i < values.docValueCount(); i++) {
                        state.add(values.nextValue());
                    }
                }
            }
        };
    }

    @Override
    protected LeafBucketCollector getLeafCollector(NumericDoubleValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final TDigestState state = getExistingOrNewHistogram(bigArrays(), bucket);
                    state.add(values.doubleValue());
                }
            }
        };
    }

    private TDigestState getExistingOrNewHistogram(final BigArrays bigArrays, long bucket) {
        states = bigArrays.grow(states, bucket + 1);
        TDigestState state = states.get(bucket);
        if (state == null) {
            state = TDigestState.createWithoutCircuitBreaking(compression, executionHint);
            states.set(bucket, state);
        }
        return state;
    }

    @Override
    public boolean hasMetric(String name) {
        return PercentilesConfig.indexOfKey(keys, Double.parseDouble(name)) >= 0;
    }

    protected TDigestState getState(long bucketOrd) {
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
