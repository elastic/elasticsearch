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

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.Sorter;

/**
 * Base class for building {@link SortedNumericDocValues} instances based on unsorted content.
 */
public abstract class SortingNumericDocValues extends SortedNumericDocValues {

    private int count;
    protected long[] values;
    protected int valuesCursor;
    private final Sorter sorter;

    protected SortingNumericDocValues() {
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
