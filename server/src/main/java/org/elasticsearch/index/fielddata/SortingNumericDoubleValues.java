/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.Sorter;

/**
 * Base class for building {@link SortedNumericDoubleValues} instances based on unsorted content.
 */
public abstract class SortingNumericDoubleValues extends SortedNumericDoubleValues {

    private int count;
    private int valuesCursor;
    protected double[] values;
    private final Sorter sorter;

    protected SortingNumericDoubleValues() {
        values = new double[1];
        valuesCursor = 0;
        sorter = new InPlaceMergeSorter() {

            @Override
            protected void swap(int i, int j) {
                final double tmp = values[i];
                values[i] = values[j];
                values[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                return Double.compare(values[i], values[j]);
            }
        };
    }

    /**
     * Set the {@link #docValueCount()} and ensure that the {@link #values} array can
     * store at least that many entries.
     */
    protected final void resize(int newSize) {
        count = newSize;
        values = ArrayUtil.grow(values, count);
        valuesCursor = 0;
    }

    /**
     * Sort values that are stored between offsets <code>0</code> and
     * {@link #docValueCount} of {@link #values}.
     */
    protected final void sort() {
        sorter.sort(0, count);
    }

    @Override
    public final int docValueCount() {
        return count;
    }

    @Override
    public final double nextValue() {
        return values[valuesCursor++];
    }
}
