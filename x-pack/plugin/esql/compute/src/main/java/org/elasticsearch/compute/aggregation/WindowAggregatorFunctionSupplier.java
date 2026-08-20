/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

import java.time.Duration;
import java.util.List;

/**
 * A {@link AggregatorFunctionSupplier} that wraps another, and apply a window function on the final aggregation.
 * <p>
 *     When the window is not an exact multiple of the time bucket ({@code W = k * B + r} with {@code r > 0}), a
 *     second <em>partial</em> side carries, per bucket, the state of only the trailing {@code r} of that bucket.
 *     The final evaluation then merges the {@code k} full buckets covered by the window plus the boundary bucket's
 *     partial state. The partial state is produced by a separate, ordinary aggregate that filters its input to the
 *     trailing {@code r} rows of each bucket; this wrapper only consumes it. In the partial-input phase the planner
 *     passes the full state channels followed by that aggregate's state channels, and {@code partialSupplier} merely
 *     reads the already-filtered per-bucket states. With raw input (single-phase execution and tests) both sides read
 *     the same value columns and {@code partialSupplier} filters its rows itself.
 * </p>
 */
public record WindowAggregatorFunctionSupplier(
    AggregatorFunctionSupplier supplier,
    @Nullable AggregatorFunctionSupplier partialSupplier,
    Duration window,
    @Nullable Duration partial
) implements AggregatorFunctionSupplier {

    public WindowAggregatorFunctionSupplier {
        assert (partialSupplier == null) == (partial == null) : "partial supplier and partial duration must be set together";
        assert partial == null || (partial.isPositive() && partial.compareTo(window) < 0) : "invalid partial duration " + partial;
    }

    public WindowAggregatorFunctionSupplier(AggregatorFunctionSupplier supplier, Duration window) {
        this(supplier, null, window, null);
    }

    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        return supplier.nonGroupingIntermediateStateDesc();
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return supplier.groupingIntermediateStateDesc();
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        if (partialSupplier != null) {
            throw new UnsupportedOperationException("windowed aggregations require grouping by a time bucket");
        }
        return supplier.aggregator(driverContext, channels);
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        if (partialSupplier != null) {
            // the channel split between the full and the partial state depends on the mode
            throw new UnsupportedOperationException("use groupingAggregatorFactory(mode, channels) with a partial channel");
        }
        GroupingAggregatorFunction fn = supplier.groupingAggregator(driverContext, channels);
        return new WindowGroupingAggregatorFunction(fn, null, supplier, window, null);
    }

    @Override
    public GroupingAggregator.Factory groupingAggregatorFactory(AggregatorMode mode, List<Integer> channels) {
        if (partialSupplier == null) {
            return AggregatorFunctionSupplier.super.groupingAggregatorFactory(mode, channels);
        }
        return new GroupingAggregator.Factory() {
            @Override
            public GroupingAggregator apply(DriverContext driverContext) {
                final List<Integer> fullChannels;
                final List<Integer> partialChannels;
                if (mode.isInputPartial()) {
                    // the planner passes the full state channels followed by the partial sibling aggregate's state channels
                    int stateSize = supplier.groupingIntermediateStateDesc().size();
                    assert channels.size() == stateSize * 2 : "expected " + (stateSize * 2) + " channels, got " + channels;
                    fullChannels = channels.subList(0, stateSize);
                    partialChannels = channels.subList(stateSize, stateSize * 2);
                } else {
                    // raw input: both channels read the same value columns; the partial supplier filters its rows
                    fullChannels = channels;
                    partialChannels = channels;
                }
                GroupingAggregatorFunction fullFn = null;
                GroupingAggregatorFunction partialFn = null;
                try {
                    fullFn = supplier.groupingAggregator(driverContext, fullChannels);
                    partialFn = partialSupplier.groupingAggregator(driverContext, partialChannels);
                    var fn = new WindowGroupingAggregatorFunction(fullFn, partialFn, supplier, window, partial);
                    fullFn = null;
                    partialFn = null;
                    return new GroupingAggregator(fn, mode);
                } finally {
                    Releasables.closeExpectNoException(fullFn, partialFn);
                }
            }

            @Override
            public String describe() {
                return WindowAggregatorFunctionSupplier.this.describe();
            }
        };
    }

    @Override
    public String describe() {
        return "Window[agg=" + supplier.describe() + ", window=" + window + (partial == null ? "" : ", partial=" + partial) + "]";
    }
}
