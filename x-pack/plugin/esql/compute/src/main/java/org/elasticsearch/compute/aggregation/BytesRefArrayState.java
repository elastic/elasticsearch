/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Aggregator state for an array of BytesRefs. It is created in a mode where it
 * won't track the {@code groupId}s that are sent to it and it is the
 * responsibility of the caller to only fetch values for {@code groupId}s
 * that it has sent using the {@code selected} parameter when building the
 * results. This is fine when there are no {@code null} values in the input
 * data. But once there are null values in the input data it is
 * <strong>much</strong> more convenient to only send non-null values and
 * the tracking built into the grouping code can't track that. In that case
 * call {@link #enableGroupIdTracking} to transition the state into a mode
 * where it'll track which {@code groupIds} have been written.
 * <p>
 *     This class is a specialized version of the {@code X-ArrayState.java.st} template.
 * </p>
 */
public final class BytesRefArrayState implements GroupingAggregatorState, Releasable {
    private final BigArrays bigArrays;
    private final CircuitBreaker breaker;
    private final String breakerLabel;
    private ObjectArray<BreakingBytesRefBuilder> values;
    /**
     * If false, no group id is expected to have nulls.
     * If true, they may have nulls.
     */
    private boolean groupIdTrackingEnabled;

    BytesRefArrayState(BigArrays bigArrays, CircuitBreaker breaker, String breakerLabel) {
        this.bigArrays = bigArrays;
        this.breaker = breaker;
        this.breakerLabel = breakerLabel;
        this.values = bigArrays.newObjectArray(0);
    }

    BytesRef get(int groupId) {
        return values.get(groupId).bytesRefView();
    }

    void set(int groupId, BytesRef value) {
        ensureCapacity(groupId);

        var currentBuilder = values.get(groupId);
        if (currentBuilder == null) {
            currentBuilder = new BreakingBytesRefBuilder(breaker, breakerLabel, value.length);
            values.set(groupId, currentBuilder);
        }

        currentBuilder.copyBytes(value);
    }

    Block toValuesBlock(IntVector selected, DriverContext driverContext) {
        if (false == groupIdTrackingEnabled) {
            try (var builder = driverContext.blockFactory().newBytesRefVectorBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    var value = get(group);
                    builder.appendBytesRef(value);
                }
                return builder.build().asBlock();
            }
        }
        try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    var value = get(group);
                    builder.appendBytesRef(value);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private void ensureCapacity(int groupId) {
        var minSize = groupId + 1;
        if (minSize > values.size()) {
            long prevSize = values.size();
            values = bigArrays.grow(values, minSize);
        }
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        assert blocks.length >= offset + 2;
        try (
            var valuesBuilder = driverContext.blockFactory().newBytesRefVectorBuilder(selected.getPositionCount());
            var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
        ) {
            var emptyBytesRef = new BytesRef();
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    var value = get(group);
                    valuesBuilder.appendBytesRef(value);
                } else {
                    valuesBuilder.appendBytesRef(emptyBytesRef); // TODO can we just use null?
                }
                hasValueBuilder.appendBoolean(i, hasValue(group));
            }
            blocks[offset] = valuesBuilder.build().asBlock();
            blocks[offset + 1] = hasValueBuilder.build().asBlock();
        }
    }

    boolean hasValue(int groupId) {
        return groupId < values.size() && values.get(groupId) != null;
    }

    /**
     * Switches this array state into tracking which group ids are set. This is
     * idempotent and fast if already tracking so it's safe to, say, call it once
     * for every block of values that arrives containing {@code null}.
     *
     * <p>
     *     This class tracks seen group IDs differently from {@code AbstractArrayState}, as it just
     *     stores a flag to know if optimizations can be made.
     * </p>
     */
    void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
        this.groupIdTrackingEnabled = true;
    }

    @Override
    public void close() {
        for (int i = 0; i < values.size(); i++) {
            Releasables.closeWhileHandlingException(values.get(i));
        }

        Releasables.close(values);
    }
}
