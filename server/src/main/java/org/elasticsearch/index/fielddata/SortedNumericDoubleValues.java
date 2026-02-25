/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;

/**
 * Clone of {@link SortedNumericDocValues} for double values.
 */
public abstract class SortedNumericDoubleValues {

    /** Sole constructor. (For invocation by subclass
     * constructors, typically implicit.) */
    protected SortedNumericDoubleValues() {}

    /** Advance the iterator to exactly {@code target} and return whether
     *  {@code target} has a value.
     *  {@code target} must be greater than or equal to the current
     *  doc ID and must be a valid doc ID, ie. &ge; 0 and
     *  &lt; {@code maxDoc}.*/
    public abstract boolean advanceExact(int target) throws IOException;

    /**
     * Iterates to the next value in the current document. Do not call this more than
     * {@link #docValueCount} times for the document.
     */
    public abstract double nextValue() throws IOException;

    /**
     * Retrieves the number of values for the current document.  This must always
     * be greater than zero.
     * It is illegal to call this method after {@link #advanceExact(int)}
     * returned {@code false}.
     */
    public abstract int docValueCount();

    /**
     * Converts a {@link SortedNumericDoubleValues} values to a singly valued {@link DoubleValues}
     * if possible
     */
    public static DoubleValues unwrapSingleton(SortedNumericDoubleValues values) {
        if (values instanceof SortedNumericDoubleValues.SingletonSortedNumericDoubleValues sv) {
            return sv.values;
        }
        return null;
    }

    /**
     * Converts a {@link DoubleValues} to a {@link SortedNumericDoubleValues}
     */
    public static SortedNumericDoubleValues singleton(DoubleValues values) {
        return new SortedNumericDoubleValues.SingletonSortedNumericDoubleValues(values);
    }

    private static class SingletonSortedNumericDoubleValues extends SortedNumericDoubleValues {

        private final DoubleValues values;

        private SingletonSortedNumericDoubleValues(DoubleValues values) {
            this.values = values;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return values.advanceExact(target);
        }

        @Override
        public double nextValue() throws IOException {
            return values.doubleValue();
        }

        @Override
        public int docValueCount() {
            return 1;
        }
    }

    /**
     * Converts a {@link SortedNumericDocValues} iterator to a {@link SortedNumericDoubleValues}
     *
     * Note that if the wrapped iterator can be unwrapped to a singleton {@link NumericDocValues}
     * instance, then the returned {@link SortedNumericDoubleValues} can also be unwrapped to
     * a {@link DoubleValues} instance via {@link #unwrapSingleton(SortedNumericDoubleValues)}
     */
    public static SortedNumericDoubleValues wrap(SortedNumericDocValues values) {
        NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            return new SortedNumericDoubleValues.SingletonSortedNumericDoubleValues(new DoubleValues() {
                @Override
                public double doubleValue() throws IOException {
                    return NumericUtils.sortableLongToDouble(singleton.longValue());
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return singleton.advanceExact(doc);
                }
            });
        }
        return new SortedNumericDoubleValues() {
            @Override
            public boolean advanceExact(int target) throws IOException {
                return values.advanceExact(target);
            }

            @Override
            public double nextValue() throws IOException {
                return NumericUtils.sortableLongToDouble(values.nextValue());
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }
        };
    }
}
