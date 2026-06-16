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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Clone of {@link SortedNumericDocValues} for double values.
 */
public abstract class SortedNumericDoubleValues {

    private final boolean isSingleton;
    private final DocIdSetIterator docIdSetIterator;
    private DoubleValues doubleValues;

    protected SortedNumericDoubleValues(DocIdSetIterator docIdSetIterator) {
        this(false, docIdSetIterator);
    }

    protected SortedNumericDoubleValues(boolean isSingleton, DocIdSetIterator docIdSetIterator) {
        this.isSingleton = isSingleton;
        this.docIdSetIterator = docIdSetIterator;
    }

    /** Advance the iterator to exactly {@code target} and return whether
     *  {@code target} has a value.
     *  {@code target} must be greater than or equal to the current
     *  doc ID and must be a valid doc ID, ie. &ge; 0 and
     *  &lt; {@code maxDoc}.*/
    public abstract boolean advanceExact(int target) throws IOException;

    /**
     * Retrieves the number of values for the current document.  This must always
     * be greater than zero.
     * It is illegal to call this method after {@link #advanceExact(int)}
     * returned {@code false}.
     */
    public abstract int docValueCount();

    /**
     * Iterates to the next value in the current document. Do not call this more than
     * {@link #docValueCount} times for the document.
     */
    public abstract double nextValue() throws IOException;

    public boolean isSingleton() {
        return isSingleton;
    }

    /**
     * Converts a {@link SortedNumericDoubleValues} values to a singly valued {@link DoubleValues}
     * if possible
     */
    public DoubleValues asDoubleValues() {
        if (isSingleton && doubleValues == null) {
            var singleton = this;
            doubleValues = new DoubleValues() {
                @Override
                public double doubleValue() throws IOException {
                    return singleton.nextValue();
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return singleton.advanceExact(doc);
                }
            };
        }
        return doubleValues;
    }

    /**
     * @return an iterator over doc ids working togerther with {@link #advanceExact(int)} and {@link #nextValue()}
     *         or null if not supported.
     */
    @Nullable
    public DocIdSetIterator docIdIterator() {
        return docIdSetIterator;
    }

    /**
     * Converts a {@link SortedNumericDoubleValues} values to a singly valued {@link DoubleValues}
     * if possible
     */
    public static DoubleValues unwrapSingleton(SortedNumericDoubleValues values) {
        return values != null ? values.asDoubleValues() : null;
    }

    /**
     * Converts a {@link DoubleValues} to a {@link SortedNumericDoubleValues}
     */
    public static SortedNumericDoubleValues singleton(DoubleValues values) {
        return new SortedNumericDoubleValues(true, null) {
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

            @Override
            public DoubleValues asDoubleValues() {
                return values;
            }
        };
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
        return new SortedNumericDoubleValues(singleton != null, values) {
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
                return singleton != null ? 1 : values.docValueCount();
            }
        };
    }

    public abstract static class SortedNumericLongWrapper extends SortedNumericDoubleValues {
        private final SortedNumericLongValues longValues;

        protected SortedNumericLongWrapper(SortedNumericLongValues longValues) {
            super(longValues.isSingleton(), longValues.docIdIterator());
            this.longValues = longValues;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return longValues.advanceExact(target);
        }

        @Override
        public int docValueCount() {
            return longValues.docValueCount();
        }

        /** Return the wrapped values. */
        public SortedNumericLongValues getLongValues() {
            return longValues;
        }
    }
}
