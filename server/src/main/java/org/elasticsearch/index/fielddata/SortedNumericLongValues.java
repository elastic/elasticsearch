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
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LongValues;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Clone of {@link SortedNumericDocValues} for long values.
 */
public abstract class SortedNumericLongValues {

    private final boolean isSingleton;
    private final DocIdSetIterator docIdSetIterator;
    private LongValues longValues;

    protected SortedNumericLongValues(DocIdSetIterator docIdSetIterator) {
        this(false, docIdSetIterator);
    }

    protected SortedNumericLongValues(boolean isSingleton, DocIdSetIterator docIdSetIterator) {
        this.isSingleton = isSingleton;
        this.docIdSetIterator = docIdSetIterator;
    }

    /**
     * A {@link SortedNumericLongValues} instance that does not have a value for any document
     */
    public static SortedNumericLongValues EMPTY = new SortedNumericLongValues(null) {
        @Override
        public boolean advanceExact(int target) {
            return false;
        }

        @Override
        public long nextValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docValueCount() {
            throw new UnsupportedOperationException();
        }
    };

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
    public abstract long nextValue() throws IOException;

    public boolean isSingleton() {
        return isSingleton;
    }

    @Nullable
    public DocIdSetIterator docIdIterator() {
        return docIdSetIterator;
    }

    /**
     * Converts a {@link SortedNumericLongValues} values to a singly valued {@link LongValues}
     * if possible
     */
    public LongValues asLongValues() {
        if (isSingleton && longValues == null) {
            var singleton = this;
            longValues = new LongValues() {
                @Override
                public long longValue() throws IOException {
                    return singleton.nextValue();
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return singleton.advanceExact(doc);
                }
            };
        }
        return longValues;
    }

    /**
     * Converts a {@link SortedNumericLongValues} values to a singly valued {@link LongValues}
     * if possible
     */
    public static LongValues unwrapSingleton(SortedNumericLongValues values) {
        return values != null ? values.asLongValues() : null;
    }

    /**
     * Converts a {@link LongValues} to a {@link SortedNumericLongValues}
     */
    public static SortedNumericLongValues singleton(LongValues values) {
        return singleton(values, null);
    }

    /**
     * Converts a {@link LongValues} to a {@link SortedNumericLongValues} and preserves
     * the associated {@link DocIdSetIterator}.
     */
    public static SortedNumericLongValues singleton(LongValues values, DocIdSetIterator docIdSetIterator) {
        return new SortedNumericLongValues(true, docIdSetIterator) {
            @Override
            public boolean advanceExact(int target) throws IOException {
                return values.advanceExact(target);
            }

            @Override
            public long nextValue() throws IOException {
                return values.longValue();
            }

            @Override
            public int docValueCount() {
                return 1;
            }

            @Override
            public LongValues asLongValues() {
                return values;
            }
        };
    }

    /**
     * Retrieves sorted numeric long values from sorted numeric doc values with specified field name.
     * Checks if the field is dense and returns a {@link DenseLongValues} instance if so.
     *
     * @param fieldName the name of the field for which to retrieve the numeric values
     * @param reader the leaf reader from which to retrieve the field's values
     * @return a {@link SortedNumericLongValues} instance representing the sorted numeric long values of the field
     * @throws IOException if an I/O error occurs while retrieving the values
     */
    public static SortedNumericLongValues getLongValues(String fieldName, LeafReader reader) throws IOException {
        SortedNumericDocValues values = DocValues.getSortedNumeric(reader, fieldName);
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            boolean isDense = false;
            DocValuesSkipper skipper = reader.getDocValuesSkipper(fieldName);
            if (skipper != null) {
                isDense = skipper.docCount() == reader.maxDoc();
            } else {
                // Checking field info for DocumentLeafReader
                FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(fieldName);
                PointValues pointValues = fieldInfo != null && fieldInfo.getPointIndexDimensionCount() != 0
                    ? reader.getPointValues(fieldName)
                    : null;
                if (pointValues != null) {
                    isDense = pointValues.getDocCount() == reader.maxDoc();
                }
            }
            final LongValues longValues;
            if (isDense) {
                longValues = new DenseLongValues() {
                    @Override
                    protected void doAdvanceExact(int doc) throws IOException {
                        singleton.advanceExact(doc);
                    }

                    @Override
                    public long longValue() throws IOException {
                        return singleton.longValue();
                    }
                };
            } else {
                longValues = new LongValues() {
                    @Override
                    public long longValue() throws IOException {
                        return singleton.longValue();
                    }

                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return singleton.advanceExact(doc);
                    }
                };
            }
            return singleton(longValues, singleton);
        } else {
            return wrap(values);
        }
    }

    /**
     * Converts a {@link SortedNumericDocValues} iterator to a {@link SortedNumericLongValues}
     *
     * Note that if the wrapped iterator can be unwrapped to a singleton {@link NumericDocValues}
     * instance, then the returned {@link SortedNumericLongValues} can also be unwrapped to
     * a {@link LongValues} instance via {@link #unwrapSingleton(SortedNumericLongValues)}
     */
    public static SortedNumericLongValues wrap(SortedNumericDocValues values) {
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            // It's more efficient to access singleton doc values via the unwrapped singleton
            return new SortedNumericLongValues(true, singleton) {
                @Override
                public boolean advanceExact(int target) throws IOException {
                    return singleton.advanceExact(target);
                }

                @Override
                public int docValueCount() {
                    return 1;
                }

                @Override
                public long nextValue() throws IOException {
                    return singleton.longValue();
                }
            };
        } else {
            return new SortedNumericLongValues(false, values) {
                @Override
                public boolean advanceExact(int target) throws IOException {
                    return values.advanceExact(target);
                }

                @Override
                public long nextValue() throws IOException {
                    return values.nextValue();
                }

                @Override
                public int docValueCount() {
                    return values.docValueCount();
                }
            };
        }
    }

    public abstract static class SortedNumericDoubleWrapper extends SortedNumericLongValues {
        private final SortedNumericDoubleValues doubleValues;

        protected SortedNumericDoubleWrapper(SortedNumericDoubleValues doubleValues) {
            super(doubleValues.isSingleton(), doubleValues.docIdIterator());
            this.doubleValues = doubleValues;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return doubleValues.advanceExact(target);
        }

        @Override
        public int docValueCount() {
            return doubleValues.docValueCount();
        }

        /** Return the wrapped values. */
        public SortedNumericDoubleValues getDoubleValues() {
            return doubleValues;
        }
    }
}
