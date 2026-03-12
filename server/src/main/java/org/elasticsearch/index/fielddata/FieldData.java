/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.geo.SpatialPoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods, similar to Lucene's {@link DocValues}.
 */
public enum FieldData {
    ;

    /**
     * Return a {@link SortedBinaryDocValues} that doesn't contain any value.
     */
    public static SortedBinaryDocValues emptySortedBinary() {
        return singleton(DocValues.emptyBinary());
    }

    /**
     * Return a {@link SortedNumericDoubleValues} that doesn't contain any value.
     */
    public static SortedNumericDoubleValues emptySortedNumericDoubles() {
        return singleton(DoubleValues.EMPTY);
    }

    /**
     * Returns a {@link DocValueBits} representing all documents from <code>values</code> that have a value.
     */
    public static DocValueBits docsWithValue(final SortedBinaryDocValues values) {
        return new DocValueBits() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }
        };
    }

    /**
     * Returns a {@link DocValueBits} representing all documents from <code>docValues</code>
     * that have a value.
     */
    public static DocValueBits docsWithValue(final SortedSetDocValues docValues) {
        return new DocValueBits() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return docValues.advanceExact(doc);
            }
        };
    }

    /**
     * Returns a {@link DocValueBits} representing all documents from <code>pointValues</code> that have
     * a value.
     */
    public static DocValueBits docsWithValue(final MultiPointValues<? extends SpatialPoint> pointValues) {
        return new DocValueBits() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return pointValues.advanceExact(doc);
            }
        };
    }

    /**
     * Returns a {@link DocValueBits} representing all documents from <code>doubleValues</code> that have a value.
     */
    public static DocValueBits docsWithValue(final SortedNumericDoubleValues doubleValues) {
        return new DocValueBits() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return doubleValues.advanceExact(doc);
            }
        };
    }

    /**
     * Returns a {@link DocValueBits} representing all documents from <code>docValues</code> that have
     * a value.
     */
    public static DocValueBits docsWithValue(final SortedNumericLongValues docValues) {
        return new DocValueBits() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return docValues.advanceExact(doc);
            }
        };
    }

    /**
     * Given a {@link SortedNumericDoubleValues}, return a
     * {@link SortedNumericDocValues} instance that will translate double values
     * to sortable long bits using
     * {@link org.apache.lucene.util.NumericUtils#doubleToSortableLong(double)}.
     */
    public static SortedNumericLongValues toSortableLongBits(SortedNumericDoubleValues values) {
        final DoubleValues singleton = unwrapSingleton(values);
        if (singleton != null) {
            final LongValues longBits;
            if (singleton instanceof SortableLongBitsToNumericDoubleValues) {
                longBits = ((SortableLongBitsToNumericDoubleValues) singleton).getLongValues();
            } else {
                longBits = new SortableLongBitsNumericDocValues(singleton);
            }
            return SortedNumericLongValues.singleton(longBits);
        } else {
            if (values instanceof SortableLongBitsToSortedNumericDoubleValues) {
                return ((SortableLongBitsToSortedNumericDoubleValues) values).getLongValues();
            } else {
                return new SortableLongBitsSortedNumericDocValues(values);
            }
        }
    }

    /**
     * Given a {@link SortedNumericLongValues}, return a {@link SortedNumericDoubleValues}
     * instance that will translate long values to doubles using
     * {@link org.apache.lucene.util.NumericUtils#sortableLongToDouble(long)}.
     */
    public static SortedNumericDoubleValues sortableLongBitsToDoubles(SortedNumericLongValues values) {
        final LongValues singleton = SortedNumericLongValues.unwrapSingleton(values);
        if (singleton != null) {
            final DoubleValues doubles;
            if (singleton instanceof SortableLongBitsNumericDocValues) {
                doubles = ((SortableLongBitsNumericDocValues) singleton).getDoubleValues();
            } else {
                doubles = new SortableLongBitsToNumericDoubleValues(singleton);
            }
            return singleton(doubles);
        } else {
            if (values instanceof SortableLongBitsSortedNumericDocValues) {
                return ((SortableLongBitsSortedNumericDocValues) values).getDoubleValues();
            } else {
                return new SortableLongBitsToSortedNumericDoubleValues(values);
            }
        }
    }

    /**
     * Wrap the provided {@link SortedNumericDocValues} instance to cast all values to doubles.
     */
    public static SortedNumericDoubleValues castToDouble(final SortedNumericLongValues values) {
        final LongValues singleton = SortedNumericLongValues.unwrapSingleton(values);
        if (singleton != null) {
            return singleton(new DoubleCastedValues(singleton));
        } else {
            return new SortedDoubleCastedValues(values);
        }
    }

    /**
     * Wrap the provided {@link SortedNumericDoubleValues} instance to cast all values to longs.
     */
    public static SortedNumericLongValues castToLong(final SortedNumericDoubleValues values) {
        final DoubleValues singleton = unwrapSingleton(values);
        if (singleton != null) {
            return SortedNumericLongValues.singleton(new LongCastedValues(singleton));
        } else {
            return new SortedLongCastedValues(values);
        }
    }

    /**
     * Returns a multi-valued view over the provided {@link DoubleValues}.
     */
    public static SortedNumericDoubleValues singleton(DoubleValues values) {
        return new SingletonSortedNumericDoubleValues(values);
    }

    /**
     * Returns a single-valued view of the {@link SortedNumericDoubleValues},
     * if it was previously wrapped with {@link DocValues#singleton(NumericDocValues)},
     * or null.
     */
    public static DoubleValues unwrapSingleton(SortedNumericDoubleValues values) {
        if (values instanceof SingletonSortedNumericDoubleValues) {
            return ((SingletonSortedNumericDoubleValues) values).getNumericDoubleValues();
        }
        return null;
    }

    /**
     * Returns a single-valued view of the {@link MultiGeoPointValues},
     * if the wrapped {@link SortedNumericDocValues} is a singleton.
     */
    public static GeoPointValues unwrapSingleton(MultiGeoPointValues values) {
        return values.getPointValues();
    }

    /**
     * Returns a multi-valued view over the provided {@link BinaryDocValues}.
     */
    public static SortedBinaryDocValues singleton(BinaryDocValues values) {
        return new SingletonSortedBinaryDocValues(values);
    }

    /**
     * Returns a single-valued view of the {@link SortedBinaryDocValues},
     * if it was previously wrapped with {@link #singleton(BinaryDocValues)},
     * or null.
     */
    public static BinaryDocValues unwrapSingleton(SortedBinaryDocValues values) {
        if (values instanceof SingletonSortedBinaryDocValues) {
            return ((SingletonSortedBinaryDocValues) values).getBinaryDocValues();
        }
        return null;
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is very slow!
     */
    public static SortedBinaryDocValues toString(final SortedNumericLongValues values) {
        {
            final LongValues singleton = SortedNumericLongValues.unwrapSingleton(values);
            if (singleton != null) {
                return FieldData.singleton(toString(singleton));
            }
        }
        return toString(new ToStringValues() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }

            @Override
            public void get(List<CharSequence> list) throws IOException {
                for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                    list.add(Long.toString(values.nextValue()));
                }
            }
        });
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is very slow!
     */
    public static BinaryDocValues toString(final LongValues values) {
        return toString(new ToStringValue() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }

            @Override
            public CharSequence get() throws IOException {
                return Long.toString(values.longValue());
            }
        });
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is very slow!
     */
    public static SortedBinaryDocValues toString(final SortedNumericDoubleValues values) {
        {
            final DoubleValues singleton = FieldData.unwrapSingleton(values);
            if (singleton != null) {
                return FieldData.singleton(toString(singleton));
            }
        }
        return toString(new ToStringValues() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }

            @Override
            public void get(List<CharSequence> list) throws IOException {
                for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                    list.add(Double.toString(values.nextValue()));
                }
            }
        });
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is very slow!
     */
    public static BinaryDocValues toString(final DoubleValues values) {
        return toString(new ToStringValue() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }

            @Override
            public CharSequence get() throws IOException {
                return Double.toString(values.doubleValue());
            }
        });
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is slow!
     */
    public static SortedBinaryDocValues toString(final SortedSetDocValues values) {
        {
            final SortedDocValues singleton = DocValues.unwrapSingleton(values);
            if (singleton != null) {
                return FieldData.singleton(toString(singleton));
            }
        }
        return new SortedBinaryDocValues() {

            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public BytesRef nextValue() throws IOException {
                return values.lookupOrd(values.nextOrd());
            }
        };
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is slow!
     */
    public static BinaryDocValues toString(final SortedDocValues values) {
        return new AbstractBinaryDocValues() {

            @Override
            public BytesRef binaryValue() throws IOException {
                return values.lookupOrd(values.ordValue());
            }

            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }
        };
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is very slow!
     */
    public static SortedBinaryDocValues toString(final MultiGeoPointValues values) {
        {
            final GeoPointValues singleton = FieldData.unwrapSingleton(values);
            if (singleton != null) {
                return FieldData.singleton(toString(singleton));
            }
        }
        return toString(new ToStringValues() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }

            @Override
            public void get(List<CharSequence> list) throws IOException {
                for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                    list.add(values.nextValue().toString());
                }
            }
        });
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is very slow!
     */
    public static BinaryDocValues toString(final GeoPointValues values) {
        return toString(new ToStringValue() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }

            @Override
            public CharSequence get() throws IOException {
                return values.pointValue().toString();
            }
        });
    }

    private static SortedBinaryDocValues toString(final ToStringValues toStringValues) {
        return new SortingBinaryDocValues() {

            final List<CharSequence> list = new ArrayList<>();

            @Override
            public boolean advanceExact(int docID) throws IOException {
                if (toStringValues.advanceExact(docID) == false) {
                    return false;
                }
                list.clear();
                toStringValues.get(list);
                count = list.size();
                grow();
                for (int i = 0; i < count; ++i) {
                    final CharSequence s = list.get(i);
                    values[i].copyChars(s);
                }
                sort();
                return true;
            }

        };
    }

    private static BinaryDocValues toString(final ToStringValue toStringValue) {
        return new AbstractBinaryDocValues() {
            private final BytesRefBuilder builder = new BytesRefBuilder();

            @Override
            public BytesRef binaryValue() {
                return builder.toBytesRef();
            }

            @Override
            public boolean advanceExact(int docID) throws IOException {
                if (toStringValue.advanceExact(docID)) {
                    builder.clear();
                    builder.copyChars(toStringValue.get());
                    return true;
                }
                return false;
            }
        };
    }

    private interface ToStringValues {

        /**
         * Advance this instance to the given document id
         * @return true if there is a value for this document
         */
        boolean advanceExact(int doc) throws IOException;

        /** Fill the list of {@link CharSequence} with the list of values for the current document. */
        void get(List<CharSequence> values) throws IOException;

    }

    private interface ToStringValue {

        /**
         * Advance this instance to the given document id
         * @return true if there is a value for this document
         */
        boolean advanceExact(int doc) throws IOException;

        /** return the {@link CharSequence} for the current document. */
        CharSequence get() throws IOException;

    }

    private static class DoubleCastedValues extends DoubleValues {

        private final LongValues values;

        DoubleCastedValues(LongValues values) {
            this.values = values;
        }

        @Override
        public double doubleValue() throws IOException {
            return values.longValue();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return values.advanceExact(doc);
        }

    }

    private static class SortedDoubleCastedValues extends SortedNumericDoubleValues {

        private final SortedNumericLongValues values;

        SortedDoubleCastedValues(SortedNumericLongValues in) {
            this.values = in;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return values.advanceExact(target);
        }

        @Override
        public double nextValue() throws IOException {
            return values.nextValue();
        }

        @Override
        public int docValueCount() {
            return values.docValueCount();
        }

    }

    private static class LongCastedValues extends LongValues {

        private final DoubleValues values;

        LongCastedValues(DoubleValues values) {
            this.values = values;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return values.advanceExact(target);
        }

        @Override
        public long longValue() throws IOException {
            return (long) values.doubleValue();
        }
    }

    private static class SortedLongCastedValues extends SortedNumericLongValues {

        private final SortedNumericDoubleValues values;

        SortedLongCastedValues(SortedNumericDoubleValues in) {
            this.values = in;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return values.advanceExact(target);
        }

        @Override
        public int docValueCount() {
            return values.docValueCount();
        }

        @Override
        public long nextValue() throws IOException {
            return (long) values.nextValue();
        }

    }

    /**
     * A {@link LongValues} instance that does not have a value for any document
     */
    public static LongValues EMPTY = new LongValues() {
        @Override
        public long longValue() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return false;
        }
    };

    /**
     * Return a {@link NumericDocValues} instance that has a value for every
     * document, returns the same value as {@code values} if there is a value
     * for the current document and {@code missing} otherwise.
     */
    public static DenseLongValues replaceMissing(LongValues values, long missing) {
        return new DenseLongValues() {

            private long value;

            @Override
            public void doAdvanceExact(int target) throws IOException {
                if (values.advanceExact(target)) {
                    value = values.longValue();
                } else {
                    value = missing;
                }
            }

            @Override
            public long longValue() {
                return value;
            }
        };
    }

    /**
     * Return a {@link DoubleValues} instance that has a value for every
     * document, returns the same value as {@code values} if there is a value
     * for the current document and {@code missing} otherwise.
     */
    public static DenseDoubleValues replaceMissing(DoubleValues values, double missing) {
        return new DenseDoubleValues() {

            private double value;

            @Override
            public void doAdvanceExact(int target) throws IOException {
                if (values.advanceExact(target)) {
                    value = values.doubleValue();
                } else {
                    value = missing;
                }
            }

            @Override
            public double doubleValue() {
                return value;
            }
        };
    }
}
