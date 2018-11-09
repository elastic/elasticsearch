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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;

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
     * Return a {@link NumericDoubleValues} that doesn't contain any value.
     */
    public static NumericDoubleValues emptyNumericDouble() {
        return new NumericDoubleValues() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return false;
            }

            @Override
            public double doubleValue() throws IOException {
                throw new UnsupportedOperationException();
            }

        };
    }

    /**
     * Return a {@link SortedNumericDoubleValues} that doesn't contain any value.
     */
    public static SortedNumericDoubleValues emptySortedNumericDoubles() {
        return singleton(emptyNumericDouble());
    }

    public static GeoPointValues emptyGeoPoint() {
        return new GeoPointValues() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return false;
            }

            @Override
            public GeoPoint geoPointValue() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Return a {@link SortedNumericDoubleValues} that doesn't contain any value.
     */
    public static MultiGeoPointValues emptyMultiGeoPoints() {
        return singleton(emptyGeoPoint());
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
    public static DocValueBits docsWithValue(final MultiGeoPointValues pointValues) {
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
    public static DocValueBits docsWithValue(final SortedNumericDocValues docValues) {
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
    public static SortedNumericDocValues toSortableLongBits(SortedNumericDoubleValues values) {
        final NumericDoubleValues singleton = unwrapSingleton(values);
        if (singleton != null) {
            final NumericDocValues longBits;
            if (singleton instanceof SortableLongBitsToNumericDoubleValues) {
                longBits = ((SortableLongBitsToNumericDoubleValues) singleton).getLongValues();
            } else {
                longBits = new SortableLongBitsNumericDocValues(singleton);
            }
            return DocValues.singleton(longBits);
        } else {
            if (values instanceof SortableLongBitsToSortedNumericDoubleValues) {
                return ((SortableLongBitsToSortedNumericDoubleValues) values).getLongValues();
            } else {
                return new SortableLongBitsSortedNumericDocValues(values);
            }
        }
    }

    /**
     * Given a {@link SortedNumericDocValues}, return a {@link SortedNumericDoubleValues}
     * instance that will translate long values to doubles using
     * {@link org.apache.lucene.util.NumericUtils#sortableLongToDouble(long)}.
     */
    public static SortedNumericDoubleValues sortableLongBitsToDoubles(SortedNumericDocValues values) {
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            final NumericDoubleValues doubles;
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
    public static SortedNumericDoubleValues castToDouble(final SortedNumericDocValues values) {
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            return singleton(new DoubleCastedValues(singleton));
        } else {
            return new SortedDoubleCastedValues(values);
        }
    }

    /**
     * Wrap the provided {@link SortedNumericDoubleValues} instance to cast all values to longs.
     */
    public static SortedNumericDocValues castToLong(final SortedNumericDoubleValues values) {
        final NumericDoubleValues singleton = unwrapSingleton(values);
        if (singleton != null) {
            return DocValues.singleton(new LongCastedValues(singleton));
        } else {
            return new SortedLongCastedValues(values);
        }
    }

    /**
     * Returns a multi-valued view over the provided {@link NumericDoubleValues}.
     */
    public static SortedNumericDoubleValues singleton(NumericDoubleValues values) {
        return new SingletonSortedNumericDoubleValues(values);
    }

    /**
     * Returns a single-valued view of the {@link SortedNumericDoubleValues},
     * if it was previously wrapped with {@link DocValues#singleton(NumericDocValues)},
     * or null.
     */
    public static NumericDoubleValues unwrapSingleton(SortedNumericDoubleValues values) {
        if (values instanceof SingletonSortedNumericDoubleValues) {
            return ((SingletonSortedNumericDoubleValues) values).getNumericDoubleValues();
        }
        return null;
    }

    /**
     * Returns a multi-valued view over the provided {@link GeoPointValues}.
     */
    public static MultiGeoPointValues singleton(GeoPointValues values) {
        return new SingletonMultiGeoPointValues(values);
    }

    /**
     * Returns a single-valued view of the {@link MultiGeoPointValues},
     * if it was previously wrapped with {@link #singleton(GeoPointValues)},
     * or null.
     */
    public static GeoPointValues unwrapSingleton(MultiGeoPointValues values) {
        if (values instanceof SingletonMultiGeoPointValues) {
            return ((SingletonMultiGeoPointValues) values).getGeoPointValues();
        }
        return null;
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
     * Returns whether the provided values *might* be multi-valued. There is no
     * guarantee that this method will return {@code false} in the single-valued case.
     */
    public static boolean isMultiValued(SortedSetDocValues values) {
        return DocValues.unwrapSingleton(values) == null;
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is very slow!
     */
    public static SortedBinaryDocValues toString(final SortedNumericDocValues values) {
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
    public static SortedBinaryDocValues toString(final SortedNumericDoubleValues values) {
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
     * NOTE: this is slow!
     */
    public static SortedBinaryDocValues toString(final SortedSetDocValues values) {
        return new SortedBinaryDocValues() {
            private int count = 0;

            @Override
            public boolean advanceExact(int doc) throws IOException {
                if (values.advanceExact(doc) == false) {
                    return false;
                }
                for (int i = 0; ; ++i) {
                    if (values.nextOrd() == SortedSetDocValues.NO_MORE_ORDS) {
                        count = i;
                        break;
                    }
                }
                // reset the iterator on the current doc
                boolean advanced = values.advanceExact(doc);
                assert advanced;
                return true;
            }

            @Override
            public int docValueCount() {
                return count;
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
     * NOTE: this is very slow!
     */
    public static SortedBinaryDocValues toString(final MultiGeoPointValues values) {
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

    private interface ToStringValues {

        /**
         * Advance this instance to the given document id
         * @return true if there is a value for this document
         */
        boolean advanceExact(int doc) throws IOException;

        /** Fill the list of charsquences with the list of values for the current document. */
        void get(List<CharSequence> values) throws IOException;

    }

    private static class DoubleCastedValues extends NumericDoubleValues {

        private final NumericDocValues values;

        DoubleCastedValues(NumericDocValues values) {
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

        private final SortedNumericDocValues values;

        SortedDoubleCastedValues(SortedNumericDocValues in) {
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

    private static class LongCastedValues extends AbstractNumericDocValues {

        private final NumericDoubleValues values;
        private int docID = -1;

        LongCastedValues(NumericDoubleValues values) {
            this.values = values;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            docID = target;
            return values.advanceExact(target);
        }

        @Override
        public long longValue() throws IOException {
            return (long) values.doubleValue();
        }

        @Override
        public int docID() {
            return docID;
        }
    }

    private static class SortedLongCastedValues extends AbstractSortedNumericDocValues {

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
     * Return a {@link NumericDocValues} instance that has a value for every
     * document, returns the same value as {@code values} if there is a value
     * for the current document and {@code missing} otherwise.
     */
    public static NumericDocValues replaceMissing(NumericDocValues values, long missing) {
        return new AbstractNumericDocValues() {

            private long value;

            @Override
            public int docID() {
                return values.docID();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                if (values.advanceExact(target)) {
                    value = values.longValue();
                } else {
                    value = missing;
                }
                return true;
            }

            @Override
            public long longValue() throws IOException {
                return value;
            }
        };
    }

    /**
     * Return a {@link NumericDoubleValues} instance that has a value for every
     * document, returns the same value as {@code values} if there is a value
     * for the current document and {@code missing} otherwise.
     */
    public static NumericDoubleValues replaceMissing(NumericDoubleValues values, double missing) {
        return new NumericDoubleValues() {

            private double value;

            @Override
            public boolean advanceExact(int target) throws IOException {
                if (values.advanceExact(target)) {
                    value = values.doubleValue();
                } else {
                    value = missing;
                }
                return true;
            }

            @Override
            public double doubleValue() throws IOException {
                return value;
            }
        };
    }
}
