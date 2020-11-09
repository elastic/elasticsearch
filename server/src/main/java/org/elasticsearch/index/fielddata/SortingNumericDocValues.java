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

        if (newSize <= values.length) {
            return;
        }

        // Array is expected to grow so increment the circuit breaker
        // to include both the additional bytes used by the grown array
        // as well as the overhead of keeping both arrays in memory while
        // copying.
        long oldValuesSizeInBytes = values.length * Long.BYTES;
        int newValuesLength = ArrayUtil.oversize(newSize, Long.BYTES);
        circuitBreakerConsumer.accept(newValuesLength * Long.BYTES);

        // resize
        values = ArrayUtil.growExact(values, newValuesLength);

        // account for freeing the old values array
        circuitBreakerConsumer.accept(-oldValuesSizeInBytes);
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
