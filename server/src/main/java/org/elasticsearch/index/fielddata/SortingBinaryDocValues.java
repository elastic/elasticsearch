/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.Sorter;

import java.util.Arrays;

/**
 * Base class for building {@link SortedBinaryDocValues} instances based on unsorted content.
 */
public abstract class SortingBinaryDocValues extends SortedBinaryDocValues {

    private int index;
    protected int count;
    protected BytesRefBuilder[] values;
    private final Sorter sorter;

    protected SortingBinaryDocValues() {
        values = new BytesRefBuilder[] { new BytesRefBuilder() };
        sorter = new InPlaceMergeSorter() {

            @Override
            protected void swap(int i, int j) {
                ArrayUtil.swap(values, i, j);
            }

            @Override
            protected int compare(int i, int j) {
                return values[i].get().compareTo(values[j].get());
            }
        };
    }

    /**
     * Make sure the {@link #values} array can store at least {@link #count} entries.
     */
    protected final void grow() {
        if (values.length < count) {
            final int oldLen = values.length;
            final int newLen = ArrayUtil.oversize(count, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            values = Arrays.copyOf(values, newLen);
            for (int i = oldLen; i < newLen; ++i) {
                values[i] = new BytesRefBuilder();
            }
        }
    }

    /**
     * Sort values that are stored between offsets <code>0</code> and
     * {@link #count} of {@link #values}.
     */
    protected final void sort() {
        sorter.sort(0, count);
        index = 0;
    }

    @Override
    public int docValueCount() {
        return count;
    }

    @Override
    public final BytesRef nextValue() {
        assert index < count;
        return values[index++].get();
    }
}
