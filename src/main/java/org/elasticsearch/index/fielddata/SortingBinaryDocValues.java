/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.*;

import java.util.Arrays;

/**
 * Base class for building {@link SortedBinaryDocValues} instances based on unsorted content.
 */
public abstract class SortingBinaryDocValues extends SortedBinaryDocValues {

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
    }

    @Override
    public final int count() {
        return count;
    }

    @Override
    public final BytesRef valueAt(int index) {
        return values[index].get();
    }
}
