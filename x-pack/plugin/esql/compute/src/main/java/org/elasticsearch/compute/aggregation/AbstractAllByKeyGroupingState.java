/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasables;

/**
 * Base class for the grouping state of all "all first/last by key" aggregators.
 * Tracks the null-value and null-key flags per group.
 * Subclasses add the key array itself, typed to the key element type.
 * <p>
 * Whether a group has ever been collected is tracked via {@link #hasValue(int)} from
 * {@link AbstractArrayState}, driven by {@link #trackGroupId(int)} calls in
 * {@code collectValue}. The {@code nullValue} bit only distinguishes null values
 * from non-null values; it does not encode "never seen".
 * </p>
 */
public abstract class AbstractAllByKeyGroupingState extends AbstractArrayState {

    /**
     * Markers for groups whose value is {@code null}. A set bit means the value is null.
     * Lazily initialized on the first call to {@link #markNullValue(int)};
     * {@code null} means no group has ever had a null value.
     */
    private BitArray nullValue;

    /**
     * Markers for groups whose key is {@code null}. A set bit means the key is null.
     * Lazily initialized on the first call to {@link #markNullKey(int)};
     * {@code null} means no group has ever had a null key.
     */
    private BitArray nullKey;

    protected AbstractAllByKeyGroupingState(BigArrays bigArrays) {
        super(bigArrays);
        enableGroupIdTracking(new SeenGroupIds.Empty());
    }

    protected final boolean nullValue(int group) {
        return nullValue != null && nullValue.get(group);
    }

    protected final void markNullValue(int group) {
        if (nullValue == null) {
            nullValue = new BitArray(group + 1, bigArrays);
        }
        nullValue.set(group);
    }

    protected final void clearNullValue(int group) {
        if (nullValue != null) {
            nullValue.clear(group);
        }
    }

    protected final boolean nullKey(int group) {
        return nullKey != null && nullKey.get(group);
    }

    protected final void markNullKey(int group) {
        if (nullKey == null) {
            nullKey = new BitArray(group + 1, bigArrays);
        }
        nullKey.set(group);
    }

    protected final void clearNullKey(int group) {
        if (nullKey != null) {
            nullKey.clear(group);
        }
    }

    /**
     * Called when a new group id is seen to grow subclass arrays to accommodate {@code group}.
     * Both {@link #nullValue} and {@link #nullKey} are lazy {@link BitArray}s that handle
     * out-of-range indices safely, so no grow is needed in this base class.
     */
    protected abstract void grow(int group);

    @Override
    public void close() {
        Releasables.close(nullValue, nullKey, super::close);
    }
}
