/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.Sorter;

import java.util.function.LongConsumer;

/**
 * Base class for building {@link SortedNumericDocValues} instances based on unsorted content.
 */
public abstract class SortingNumericDocValues extends SortedNumericDocValues {

    private int count;
    protected long[] values;
    protected int valuesCursor;
    private final Sorter sorter;
    private LongConsumer circuitBreakerConsumer;

    protected SortingNumericDocValues() {
        this(l -> {});
    }

    protected SortingNumericDocValues(LongConsumer circuitBreakerConsumer) {
        values = new long[1];
        valuesCursor = 0;
        sorter = new InPlaceMergeSorter() {

            @Override
            protected void swap(int i, int j) {
                final long tmp = values[i];
                values[i] = values[j];
                values[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                return Long.compare(values[i], values[j]);
            }
        };
        this.circuitBreakerConsumer = circuitBreakerConsumer;
        // account for initial values size of 1
        this.circuitBreakerConsumer.accept(Long.BYTES);
    }

    /**
     * Set the {@link #docValueCount()} and ensure that the {@link #values} array can
     * store at least that many entries.
     */
    protected final void resize(int newSize) {
        count = newSize;
        valuesCursor = 0;

        if (newSize <= getArrayLength()) {
            return;
        }

        // Array is expected to grow so increment the circuit breaker
        // to include both the additional bytes used by the grown array
        // as well as the overhead of keeping both arrays in memory while
        // copying.
        long oldValuesSizeInBytes = (long) getArrayLength() * Long.BYTES;
        int newValuesLength = ArrayUtil.oversize(newSize, Long.BYTES);
        circuitBreakerConsumer.accept((long) newValuesLength * Long.BYTES);

        // resize
        growExact(newValuesLength);

        // account for freeing the old values array
        circuitBreakerConsumer.accept(-oldValuesSizeInBytes);
    }

    /** Grow the array in a method so we can override it during testing */
    protected void growExact(int newValuesLength) {
        values = ArrayUtil.growExact(values, newValuesLength);
    }

    /** Get the size of the internal array using a method so we can override it during testing */
    protected int getArrayLength() {
        return values.length;
    }

    /**
     * Sort values that are stored between offsets <code>0</code> and
     * {@link #count} of {@link #values}.
     */
    protected final void sort() {
        sorter.sort(0, count);
    }

    @Override
    public final int docValueCount() {
        return count;
    }

    @Override
    public final long nextValue() {
        return values[valuesCursor++];
    }
}
