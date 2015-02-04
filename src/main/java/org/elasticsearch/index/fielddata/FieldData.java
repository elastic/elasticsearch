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

import org.apache.lucene.index.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.geo.GeoPoint;

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
    public static SortedBinaryDocValues emptySortedBinary(int maxDoc) {
        return singleton(DocValues.emptyBinary(), new Bits.MatchNoBits(maxDoc));
    }

    /**
     * Return a {@link NumericDoubleValues} that doesn't contain any value.
     */
    public static NumericDoubleValues emptyNumericDouble() {
        return new NumericDoubleValues() {
            @Override
            public double get(int docID) {
                return 0;
            }

        };
    }

    /**
     * Return a {@link SortedNumericDoubleValues} that doesn't contain any value.
     */
    public static SortedNumericDoubleValues emptySortedNumericDoubles(int maxDoc) {
        return singleton(emptyNumericDouble(), new Bits.MatchNoBits(maxDoc));
    }

    public static GeoPointValues emptyGeoPoint() {
        final GeoPoint point = new GeoPoint();
        return new GeoPointValues() {
            @Override
            public GeoPoint get(int docID) {
                return point;
            }
        };
    }

    /**
     * Return a {@link SortedNumericDoubleValues} that doesn't contain any value.
     */
    public static MultiGeoPointValues emptyMultiGeoPoints(int maxDoc) {
        return singleton(emptyGeoPoint(), new Bits.MatchNoBits(maxDoc));
    }

    /**
     * Returns a {@link Bits} representing all documents from <code>dv</code> that have a value.
     */
    public static Bits docsWithValue(final SortedBinaryDocValues dv, final int maxDoc) {
      return new Bits() {
        @Override
        public boolean get(int index) {
          dv.setDocument(index);
          return dv.count() != 0;
        }

        @Override
        public int length() {
          return maxDoc;
        }
      };
    }

    /**
     * Returns a Bits representing all documents from <code>dv</code> that have a value.
     */
    public static Bits docsWithValue(final MultiGeoPointValues dv, final int maxDoc) {
      return new Bits() {
        @Override
        public boolean get(int index) {
          dv.setDocument(index);
          return dv.count() != 0;
        }

        @Override
        public int length() {
          return maxDoc;
        }
      };
    }

    /**
     * Returns a Bits representing all documents from <code>dv</code> that have a value.
     */
    public static Bits docsWithValue(final SortedNumericDoubleValues dv, final int maxDoc) {
      return new Bits() {
        @Override
        public boolean get(int index) {
          dv.setDocument(index);
          return dv.count() != 0;
        }

        @Override
        public int length() {
          return maxDoc;
        }
      };
    }

    /**
     * Given a {@link SortedNumericDoubleValues}, return a {@link SortedNumericDocValues}
     * instance that will translate double values to sortable long bits using
     * {@link NumericUtils#doubleToSortableLong(double)}.
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
            final Bits docsWithField = unwrapSingletonBits(values);
            return DocValues.singleton(longBits, docsWithField);
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
     * {@link NumericUtils#sortableLongToDouble(long)}.
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
            final Bits docsWithField = DocValues.unwrapSingletonBits(values);
            return singleton(doubles, docsWithField);
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
            final Bits docsWithField = DocValues.unwrapSingletonBits(values);
            return singleton(new DoubleCastedValues(singleton), docsWithField);
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
            final Bits docsWithField = unwrapSingletonBits(values);
            return DocValues.singleton(new LongCastedValues(singleton), docsWithField);
        } else {
            return new SortedLongCastedValues(values);
        }
    }

    /**
     * Returns a multi-valued view over the provided {@link NumericDoubleValues}.
     */
    public static SortedNumericDoubleValues singleton(NumericDoubleValues values, Bits docsWithField) {
        return new SingletonSortedNumericDoubleValues(values, docsWithField);
    }

    /**
     * Returns a single-valued view of the {@link SortedNumericDoubleValues},
     * if it was previously wrapped with {@link #singleton(NumericDocValues, Bits)},
     * or null.
     * @see #unwrapSingletonBits(SortedNumericDocValues)
     */
    public static NumericDoubleValues unwrapSingleton(SortedNumericDoubleValues values) {
        if (values instanceof SingletonSortedNumericDoubleValues) {
            return ((SingletonSortedNumericDoubleValues) values).getNumericDoubleValues();
        }
        return null;
    }

    /**
     * Returns the documents with a value for the {@link SortedNumericDoubleValues},
     * if it was previously wrapped with {@link #singleton(NumericDoubleValues, Bits)},
     * or null.
     */
    public static Bits unwrapSingletonBits(SortedNumericDoubleValues dv) {
      if (dv instanceof SingletonSortedNumericDoubleValues) {
        return ((SingletonSortedNumericDoubleValues)dv).getDocsWithField();
      } else {
        return null;
      }
    }

    /**
     * Returns a multi-valued view over the provided {@link GeoPointValues}.
     */
    public static MultiGeoPointValues singleton(GeoPointValues values, Bits docsWithField) {
        return new SingletonMultiGeoPointValues(values, docsWithField);
    }

    /**
     * Returns a single-valued view of the {@link MultiGeoPointValues},
     * if it was previously wrapped with {@link #singleton(GeoPointValues, Bits)},
     * or null.
     * @see #unwrapSingletonBits(MultiGeoPointValues)
     */
    public static GeoPointValues unwrapSingleton(MultiGeoPointValues values) {
        if (values instanceof SingletonMultiGeoPointValues) {
            return ((SingletonMultiGeoPointValues) values).getGeoPointValues();
        }
        return null;
    }

    /**
     * Returns the documents with a value for the {@link MultiGeoPointValues},
     * if it was previously wrapped with {@link #singleton(GeoPointValues, Bits)},
     * or null.
     */
    public static Bits unwrapSingletonBits(MultiGeoPointValues values) {
        if (values instanceof SingletonMultiGeoPointValues) {
            return ((SingletonMultiGeoPointValues) values).getDocsWithField();
        }
        return null;
    }

    /**
     * Returns a multi-valued view over the provided {@link BinaryDocValues}.
     */
    public static SortedBinaryDocValues singleton(BinaryDocValues values, Bits docsWithField) {
        return new SingletonSortedBinaryDocValues(values, docsWithField);
    }

    /**
     * Returns a single-valued view of the {@link SortedBinaryDocValues},
     * if it was previously wrapped with {@link #singleton(BinaryDocValues, Bits)},
     * or null.
     * @see #unwrapSingletonBits(SortedBinaryDocValues)
     */
    public static BinaryDocValues unwrapSingleton(SortedBinaryDocValues values) {
        if (values instanceof SingletonSortedBinaryDocValues) {
            return ((SingletonSortedBinaryDocValues) values).getBinaryDocValues();
        }
        return null;
    }

    /**
     * Returns the documents with a value for the {@link SortedBinaryDocValues},
     * if it was previously wrapped with {@link #singleton(BinaryDocValues, Bits)},
     * or null.
     */
    public static Bits unwrapSingletonBits(SortedBinaryDocValues values) {
        if (values instanceof SingletonSortedBinaryDocValues) {
            return ((SingletonSortedBinaryDocValues) values).getDocsWithField();
        }
        return null;
    }

    /**
     * Returns whether the provided values *might* be multi-valued. There is no
     * guarantee that this method will return <tt>false</tt> in the single-valued case.
     */
    public static boolean isMultiValued(SortedSetDocValues values) {
        return DocValues.unwrapSingleton(values) == null;
    }

    /**
     * Returns whether the provided values *might* be multi-valued. There is no
     * guarantee that this method will return <tt>false</tt> in the single-valued case.
     */
    public static boolean isMultiValued(SortedNumericDocValues values) {
        return DocValues.unwrapSingleton(values) == null;
    }

    /**
     * Returns whether the provided values *might* be multi-valued. There is no
     * guarantee that this method will return <tt>false</tt> in the single-valued case.
     */
    public static boolean isMultiValued(SortedNumericDoubleValues values) {
        return unwrapSingleton(values) == null;
    }

    /**
     * Returns whether the provided values *might* be multi-valued. There is no
     * guarantee that this method will return <tt>false</tt> in the single-valued case.
     */
    public static boolean isMultiValued(SortedBinaryDocValues values) {
        return unwrapSingleton(values) != null;
    }

    /**
     * Returns whether the provided values *might* be multi-valued. There is no
     * guarantee that this method will return <tt>false</tt> in the single-valued case.
     */
    public static boolean isMultiValued(MultiGeoPointValues values) {
        return unwrapSingleton(values) == null;
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is very slow!
     */
    public static SortedBinaryDocValues toString(final SortedNumericDocValues values) {
        return toString(new ToStringValues() {
            @Override
            public void get(int docID, List<CharSequence> list) {
                values.setDocument(docID);
                for (int i = 0, count = values.count(); i < count; ++i) {
                    list.add(Long.toString(values.valueAt(i)));
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
            public void get(int docID, List<CharSequence> list) {
                values.setDocument(docID);
                for (int i = 0, count = values.count(); i < count; ++i) {
                    list.add(Double.toString(values.valueAt(i)));
                }
            }
        });
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is slow!
     */
    public static SortedBinaryDocValues toString(final RandomAccessOrds values) {
        return new SortedBinaryDocValues() {

            @Override
            public BytesRef valueAt(int index) {
                return values.lookupOrd(values.ordAt(index));
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.cardinality();
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
            public void get(int docID, List<CharSequence> list) {
                values.setDocument(docID);
                for (int i = 0, count = values.count(); i < count; ++i) {
                    list.add(values.valueAt(i).toString());
                }
            }
        });
    }

    /**
     * If <code>dv</code> is an instance of {@link RandomAccessOrds}, then return
     * it, otherwise wrap it into a slow wrapper that implements random access.
     */
    public static RandomAccessOrds maybeSlowRandomAccessOrds(final SortedSetDocValues dv) {
        if (dv instanceof RandomAccessOrds) {
            return (RandomAccessOrds) dv;
        } else {
            assert DocValues.unwrapSingleton(dv) == null : "this method expect singleton to return random-access ords";
            return new RandomAccessOrds() {

                int cardinality;
                long[] ords = new long[0];
                int ord;

                @Override
                public void setDocument(int docID) {
                    cardinality = 0;
                    dv.setDocument(docID);
                    for (long ord = dv.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = dv.nextOrd()) {
                        ords = ArrayUtil.grow(ords, cardinality + 1);
                        ords[cardinality++] = ord;
                    }
                    ord = 0;
                }

                @Override
                public long nextOrd() {
                    return ords[ord++];
                }

                @Override
                public BytesRef lookupOrd(long ord) {
                    return dv.lookupOrd(ord);
                }

                @Override
                public long getValueCount() {
                    return dv.getValueCount();
                }

                @Override
                public long ordAt(int index) {
                    return ords[index];
                }

                @Override
                public int cardinality() {
                    return cardinality;
                }
            };
        }
    }

    private static SortedBinaryDocValues toString(final ToStringValues toStringValues) {
        return new SortingBinaryDocValues() {

            final List<CharSequence> list = new ArrayList<>();

            @Override
            public void setDocument(int docID) {
                list.clear();
                toStringValues.get(docID, list);
                count = list.size();
                grow();
                for (int i = 0; i < count; ++i) {
                    final CharSequence s = list.get(i);
                    values[i].copyChars(s);
                }
                sort();
            }

        };
    }

    private static interface ToStringValues {

        void get(int docID, List<CharSequence> values);

    }

    private static class DoubleCastedValues extends NumericDoubleValues {

        private final NumericDocValues values;

        DoubleCastedValues(NumericDocValues values) {
            this.values = values;
        }

        @Override
        public double get(int docID) {
            return values.get(docID);
        }

    }

    private static class SortedDoubleCastedValues extends SortedNumericDoubleValues {

        private final SortedNumericDocValues values;

        SortedDoubleCastedValues(SortedNumericDocValues in) {
            this.values = in;
        }

        @Override
        public double valueAt(int index) {
            return values.valueAt(index);
        }

        @Override
        public void setDocument(int doc) {
            values.setDocument(doc);
        }

        @Override
        public int count() {
            return values.count();
        }

    }

    private static class LongCastedValues extends NumericDocValues {

        private final NumericDoubleValues values;

        LongCastedValues(NumericDoubleValues values) {
            this.values = values;
        }

        @Override
        public long get(int docID) {
            return (long) values.get(docID);
        }

    }

    private static class SortedLongCastedValues extends SortedNumericDocValues {

        private final SortedNumericDoubleValues values;

        SortedLongCastedValues(SortedNumericDoubleValues in) {
            this.values = in;
        }

        @Override
        public long valueAt(int index) {
            return (long) values.valueAt(index);
        }

        @Override
        public void setDocument(int doc) {
            values.setDocument(doc);
        }

        @Override
        public int count() {
            return values.count();
        }

    }
}
