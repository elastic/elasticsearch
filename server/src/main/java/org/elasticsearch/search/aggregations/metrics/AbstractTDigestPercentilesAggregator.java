/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.search.DoubleValues;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
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
    protected ObjectArray<HistogramUnionState> states;
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
                    final HistogramUnionState state = getExistingOrNewHistogram(bigArrays(), bucket);
                    for (int i = 0; i < values.docValueCount(); i++) {
                        state.add(values.nextValue());
                    }
                }
            }
        };
    }

    @Override
    protected LeafBucketCollector getLeafCollector(DoubleValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final HistogramUnionState state = getExistingOrNewHistogram(bigArrays(), bucket);
                    state.add(values.doubleValue());
                }
            }
        };
    }

    private HistogramUnionState getExistingOrNewHistogram(final BigArrays bigArrays, long bucket) {
        states = bigArrays.grow(states, bucket + 1);
        HistogramUnionState state = states.get(bucket);
        if (state == null) {
            state = HistogramUnionState.create(context.breaker(), executionHint, compression);
            states.set(bucket, state);
        }
        return state;
    }

    @Override
    public boolean hasMetric(String name) {
        return PercentilesConfig.indexOfKey(keys, Double.parseDouble(name)) >= 0;
    }

    protected HistogramUnionState getState(long bucketOrd) {
        if (bucketOrd >= states.size()) {
            return null;
        }
        return states.get(bucketOrd);
    }

    /**
     * Removes and returns the state for {@code bucketOrd}, transferring ownership to the caller.
     * The state's circuit-breaker bytes are returned to the breaker immediately, while the
     * aggregation context (and its {@code PreallocatedCircuitBreakerService}) is still open.
     * This is the only safe window to do so: {@code InternalAggregation} has no close lifecycle,
     * so by the time the result is serialized the breaker will already be closed.
     * After this call the aggregator no longer holds a reference, so {@link #doClose()} will not
     * attempt to close the state.
     */
    protected HistogramUnionState takeState(long bucketOrd) {
        if (bucketOrd >= states.size()) {
            return null;
        }
        HistogramUnionState state = states.get(bucketOrd);
        states.set(bucketOrd, null);
        if (state != null) {
            // Return bytes now, while the breaker is still open. The state's data remains
            // accessible for serialization and reduction; only breaker accounting is released.
            context.breaker().addWithoutBreaking(-state.ramBytesUsed());
        }
        return state;
    }

    @Override
    protected void doClose() {
        // Close any states that were not transferred to an InternalAggregation via takeState().
        // This returns their circuit-breaker bytes on the failure path (e.g. when a
        // CircuitBreakingException aborts collection before buildAggregation is called).
        // Guard against null: the circuit breaker may trip inside the constructor before states is assigned.
        if (states != null) {
            try {
                for (long i = 0; i < states.size(); i++) {
                    // Use closeWhileHandlingException so a failure on one slot does not prevent
                    // the remaining slots (and the container itself) from being closed.
                    Releasables.closeWhileHandlingException(states.get(i));
                }
            } finally {
                Releasables.close(states);
            }
        }
    }

}
