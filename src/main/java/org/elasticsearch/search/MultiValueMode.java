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


package org.elasticsearch.search;

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.util.Locale;

/**
 * Defines what values to pick in the case a document contains multiple values for a particular field.
 */
public enum MultiValueMode {

    /**
     * Sum of all the values.
     */
    SUM {
        /**
         * Returns the sum of the two values
         */
        @Override
        public double apply(double a, double b) {
            return a + b;
        }

        /**
         * Returns the sum of the two values
         */
        @Override
        public long apply(long a, long b) {
            return a + b;
        }
    },

    /**
     * Average of all the values.
     */
    AVG {

        /**
         * Returns the sum of the two values
         */
        @Override
        public double apply(double a, double b) {
            return a + b;
        }

        /**
         * Returns the sum of the two values
         */
        @Override
        public long apply(long a, long b) {
            return a + b;
        }

        /**
         * Returns <code>a / Math.max(1.0d, numValues)</code>
         */
        @Override
        public double reduce(double a, int numValues) {
            return a / Math.max(1.0d, (double) numValues);
        }

        /**
         * Returns <code>Math.round(a / Math.max(1.0, numValues))</code>
         */
        @Override
        public long reduce(long a, int numValues) {
            if (numValues <= 1) {
                // without this the average might be different from the original
                // values on a single-valued field due to the precision loss of the double
                return a;
            } else {
                return Math.round(a / Math.max(1.0, numValues));
            }
        }
    },

    /**
     * Pick the lowest value.
     */
    MIN {
        /**
         * Equivalent to {@link Math#min(double, double)}
         */
        @Override
        public double apply(double a, double b) {
            return Math.min(a, b);
        }

        /**
         * Equivalent to {@link Math#min(long, long)}
         */
        @Override
        public long apply(long a, long b) {
            return Math.min(a, b);
        }

        @Override
        public int applyOrd(int ord1, int ord2) {
            return Math.min(ord1, ord2);
        }

        @Override
        public BytesRef apply(BytesRef a, BytesRef b) {
            return a.compareTo(b) <= 0 ? a : b;
        }

        /**
         * Returns {@link Double#POSITIVE_INFINITY}
         */
        @Override
        public double startDouble() {
            return Double.POSITIVE_INFINITY;
        }

        /**
         * Returns {@link Long#MAX_VALUE}
         */
        @Override
        public long startLong() {
            return Long.MAX_VALUE;
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue) {
            if (values.count() > 0) {
                return values.valueAt(0);
            } else {
                return missingValue;
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue) {
            if (values.count() > 0) {
                return values.valueAt(0);
            } else {
                return missingValue;
            }
        }

        @Override
        protected BytesRef pick(SortedBinaryDocValues values, BytesRef missingValue) {
            if (values.count() > 0) {
                return values.valueAt(0);
            } else {
                return missingValue;
            }
        }

        @Override
        protected int pick(RandomAccessOrds values) {
            if (values.cardinality() > 0) {
                return (int) values.ordAt(0);
            } else {
                return -1;
            }
        }
    },

    /**
     * Pick the highest value.
     */
    MAX {
        /**
         * Equivalent to {@link Math#max(double, double)}
         */
        @Override
        public double apply(double a, double b) {
            return Math.max(a, b);
        }

        /**
         * Equivalent to {@link Math#max(long, long)}
         */
        @Override
        public long apply(long a, long b) {
            return Math.max(a, b);
        }

        @Override
        public int applyOrd(int ord1, int ord2) {
            return Math.max(ord1, ord2);
        }

        @Override
        public BytesRef apply(BytesRef a, BytesRef b) {
            return a.compareTo(b) > 0 ? a : b;
        }

        /**
         * Returns {@link Double#NEGATIVE_INFINITY}
         */
        @Override
        public double startDouble() {
            return Double.NEGATIVE_INFINITY;
        }


        /**
         * Returns {@link Long#MIN_VALUE}
         */
        @Override
        public long startLong() {
            return Long.MIN_VALUE;
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue) {
            final int count = values.count();
            if (count > 0) {
                return values.valueAt(count - 1);
            } else {
                return missingValue;
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue) {
            final int count = values.count();
            if (count > 0) {
                return values.valueAt(count - 1);
            } else {
                return missingValue;
            }
        }

        @Override
        protected BytesRef pick(SortedBinaryDocValues values, BytesRef missingValue) {
            final int count = values.count();
            if (count > 0) {
                return values.valueAt(count - 1);
            } else {
                return missingValue;
            }
        }

        @Override
        protected int pick(RandomAccessOrds values) {
            final int count = values.cardinality();
            if (count > 0) {
                return (int) values.ordAt(count - 1);
            } else {
                return -1;
            }
        }
    };

    /**
     * Applies the sort mode and returns the result. This method is meant to be
     * a binary function that is commonly used in a loop to find the relevant
     * value for the sort mode in a list of values. For instance if the sort mode
     * is {@link MultiValueMode#MAX} this method is equivalent to {@link Math#max(double, double)}.
     *
     * Note: all implementations are idempotent.
     *
     * @param a an argument
     * @param b another argument
     * @return the result of the function.
     */
    public abstract double apply(double a, double b);

    /**
     * Applies the sort mode and returns the result. This method is meant to be
     * a binary function that is commonly used in a loop to find the relevant
     * value for the sort mode in a list of values. For instance if the sort mode
     * is {@link MultiValueMode#MAX} this method is equivalent to {@link Math#max(long, long)}.
     *
     * Note: all implementations are idempotent.
     *
     * @param a an argument
     * @param b another argument
     * @return the result of the function.
     */
    public abstract long apply(long a, long b);

    public int applyOrd(int ord1, int ord2) {
        throw new UnsupportedOperationException();
    }

    public BytesRef apply(BytesRef a, BytesRef b) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns an initial value for the sort mode that is guaranteed to have no impact if passed
     * to {@link #apply(double, double)}. This value should be used as the initial value if the
     * sort mode is applied to a non-empty list of values. For instance:
     * <pre>
     *     double relevantValue = sortMode.startDouble();
     *     for (int i = 0; i < array.length; i++) {
     *         relevantValue = sortMode.apply(array[i], relevantValue);
     *     }
     * </pre>
     *
     * Note: This method return <code>0</code> by default.
     *
     * @return an initial value for the sort mode.
     */
    public double startDouble() {
        return 0;
    }

    /**
     * Returns an initial value for the sort mode that is guaranteed to have no impact if passed
     * to {@link #apply(long, long)}. This value should be used as the initial value if the
     * sort mode is applied to a non-empty list of values. For instance:
     * <pre>
     *     long relevantValue = sortMode.startLong();
     *     for (int i = 0; i < array.length; i++) {
     *         relevantValue = sortMode.apply(array[i], relevantValue);
     *     }
     * </pre>
     *
     * Note: This method return <code>0</code> by default.
     * @return an initial value for the sort mode.
     */
    public long startLong() {
        return 0;
    }

    /**
     * Returns the aggregated value based on the sort mode. For instance if {@link MultiValueMode#AVG} is used
     * this method divides the given value by the number of values. The default implementation returns
     * the first argument.
     *
     * Note: all implementations are idempotent.
     */
    public double reduce(double a, int numValues) {
        return a;
    }

    /**
     * Returns the aggregated value based on the sort mode. For instance if {@link MultiValueMode#AVG} is used
     * this method divides the given value by the number of values. The default implementation returns
     * the first argument.
     *
     * Note: all implementations are idempotent.
     */
    public long reduce(long a, int numValues) {
        return a;
    }

    /**
     * A case insensitive version of {@link #valueOf(String)}
     *
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException if the given string doesn't match a sort mode or is <code>null</code>.
     */
    public static MultiValueMode fromString(String sortMode) {
        try {
            return valueOf(sortMode.toUpperCase(Locale.ROOT));
        } catch (Throwable t) {
            throw new ElasticsearchIllegalArgumentException("Illegal sort_mode " + sortMode);
        }
    }

    protected long pick(SortedNumericDocValues values, long missingValue) {
        final int count = values.count();
        if (count == 0) {
            return missingValue;
        } else {
            long aggregate = startLong();
            for (int i = 0; i < count; ++i) {
                aggregate = apply(aggregate, values.valueAt(i));
            }
            return reduce(aggregate, count);
        }
    }

    /**
     * Return a {@link NumericDocValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     */
    public NumericDocValues select(final SortedNumericDocValues values, final long missingValue) {
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            final Bits docsWithField = DocValues.unwrapSingletonBits(values);
            if (docsWithField == null || missingValue == 0) {
                return singleton;
            } else {
                return new NumericDocValues() {
                    @Override
                    public long get(int docID) {
                        final long value = singleton.get(docID);
                        if (value == 0 && docsWithField.get(docID) == false) {
                            return missingValue;
                        }
                        return value;
                    }
                };
            }
        } else {
            return new NumericDocValues() {
                @Override
                public long get(int docID) {
                    values.setDocument(docID);
                    return pick(values, missingValue);
                }
            };
        }
    }

    /**
     * Return a {@link NumericDocValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     *
     * For every root document, the values of its inner documents will be aggregated.
     * If none of the inner documents has a value, then <code>missingValue</code> is returned.
     *
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     */
    public NumericDocValues select(final SortedNumericDocValues values, final long missingValue, final BitSet rootDocs, final DocIdSet innerDocSet, int maxDoc) throws IOException {
        if (rootDocs == null || innerDocSet == null) {
            return select(DocValues.emptySortedNumeric(maxDoc), missingValue);
        }
        final DocIdSetIterator innerDocs = innerDocSet.iterator();
        if (innerDocs == null) {
            return select(DocValues.emptySortedNumeric(maxDoc), missingValue);
        }

        return new NumericDocValues() {

            int lastSeenRootDoc = 0;
            long lastEmittedValue = missingValue;

            @Override
            public long get(int rootDoc) {
                assert rootDocs.get(rootDoc) : "can only sort root documents";
                assert rootDoc >= lastSeenRootDoc : "can only evaluate current and upcoming root docs";
                // If via compareBottom this method has previously invoked for the same rootDoc then we need to use the
                // last seen value, because innerDocs can't re-iterate over nested child docs it has already emitted,
                // because DocIdSetIterator can only advance forwards.
                if (rootDoc == lastSeenRootDoc) {
                    return lastEmittedValue;
                }
                try {
                    final int prevRootDoc = rootDocs.prevSetBit(rootDoc - 1);
                    final int firstNestedDoc;
                    if (innerDocs.docID() > prevRootDoc) {
                        firstNestedDoc = innerDocs.docID();
                    } else {
                        firstNestedDoc = innerDocs.advance(prevRootDoc + 1);
                    }

                    long accumulated = startLong();
                    int numValues = 0;

                    for (int doc = firstNestedDoc; doc < rootDoc; doc = innerDocs.nextDoc()) {
                        values.setDocument(doc);
                        final int count = values.count();
                        for (int i = 0; i < count; ++i) {
                            final long value = values.valueAt(i);
                            accumulated = apply(accumulated, value);
                        }
                        numValues += count;
                    }
                    lastSeenRootDoc = rootDoc;
                    if (numValues == 0) {
                        lastEmittedValue = missingValue;
                    } else {
                        lastEmittedValue = reduce(accumulated, numValues);
                    }
                    return lastEmittedValue;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    protected double pick(SortedNumericDoubleValues values, double missingValue) {
        final int count = values.count();
        if (count == 0) {
            return missingValue;
        } else {
            double aggregate = startDouble();
            for (int i = 0; i < count; ++i) {
                aggregate = apply(aggregate, values.valueAt(i));
            }
            return reduce(aggregate, count);
        }
    }

    /**
     * Return a {@link NumericDoubleValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     */
    public NumericDoubleValues select(final SortedNumericDoubleValues values, final double missingValue) {
        final NumericDoubleValues singleton = FieldData.unwrapSingleton(values);
        if (singleton != null) {
            final Bits docsWithField = FieldData.unwrapSingletonBits(values);
            if (docsWithField == null || missingValue == 0) {
                return singleton;
            } else {
                return new NumericDoubleValues() {
                    @Override
                    public double get(int docID) {
                        final double value = singleton.get(docID);
                        if (value == 0 && docsWithField.get(docID) == false) {
                            return missingValue;
                        }
                        return value;
                    }
                };
            }
        } else {
            return new NumericDoubleValues() {
                @Override
                public double get(int docID) {
                    values.setDocument(docID);
                    return pick(values, missingValue);
                }
            };
        }
    }

    /**
     * Return a {@link NumericDoubleValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     *
     * For every root document, the values of its inner documents will be aggregated.
     * If none of the inner documents has a value, then <code>missingValue</code> is returned.
     *
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     */
    public NumericDoubleValues select(final SortedNumericDoubleValues values, final double missingValue, final BitSet rootDocs, final DocIdSet innerDocSet, int maxDoc) throws IOException {
        if (rootDocs == null || innerDocSet == null) {
            return select(FieldData.emptySortedNumericDoubles(maxDoc), missingValue);
        }

        final DocIdSetIterator innerDocs = innerDocSet.iterator();
        if (innerDocs == null) {
            return select(FieldData.emptySortedNumericDoubles(maxDoc), missingValue);
        }

        return new NumericDoubleValues() {

            int lastSeenRootDoc = 0;
            double lastEmittedValue = missingValue;

            @Override
            public double get(int rootDoc) {
                assert rootDocs.get(rootDoc) : "can only sort root documents";
                assert rootDoc >= lastSeenRootDoc : "can only evaluate current and upcoming root docs";
                if (rootDoc == lastSeenRootDoc) {
                    return lastEmittedValue;
                }
                try {
                    final int prevRootDoc = rootDocs.prevSetBit(rootDoc - 1);
                    final int firstNestedDoc;
                    if (innerDocs.docID() > prevRootDoc) {
                        firstNestedDoc = innerDocs.docID();
                    } else {
                        firstNestedDoc = innerDocs.advance(prevRootDoc + 1);
                    }

                    double accumulated = startDouble();
                    int numValues = 0;

                    for (int doc = firstNestedDoc; doc > prevRootDoc && doc < rootDoc; doc = innerDocs.nextDoc()) {
                        values.setDocument(doc);
                        final int count = values.count();
                        for (int i = 0; i < count; ++i) {
                            final double value = values.valueAt(i);
                            accumulated = apply(accumulated, value);
                        }
                        numValues += count;
                    }

                    lastSeenRootDoc = rootDoc;
                    if (numValues == 0) {
                        lastEmittedValue = missingValue;
                    } else {
                        lastEmittedValue = reduce(accumulated, numValues);
                    }
                    return lastEmittedValue;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    protected BytesRef pick(SortedBinaryDocValues values, BytesRef missingValue) {
        throw new ElasticsearchIllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link BinaryDocValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     */
    public BinaryDocValues select(final SortedBinaryDocValues values, final BytesRef missingValue) {
        final BinaryDocValues singleton = FieldData.unwrapSingleton(values);
        if (singleton != null) {
            final Bits docsWithField = FieldData.unwrapSingletonBits(values);
            if (docsWithField == null) {
                return singleton;
            } else {
                return new BinaryDocValues() {
                    @Override
                    public BytesRef get(int docID) {
                        final BytesRef value = singleton.get(docID);
                        if (value.length == 0 && docsWithField.get(docID) == false) {
                            return missingValue;
                        }
                        return value;
                    }
                };
            }
        } else {
            return new BinaryDocValues() {
                @Override
                public BytesRef get(int docID) {
                    values.setDocument(docID);
                    return pick(values, missingValue);
                }
            };
        }
    }

    /**
     * Return a {@link BinaryDocValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     *
     * For every root document, the values of its inner documents will be aggregated.
     * If none of the inner documents has a value, then <code>missingValue</code> is returned.
     *
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     */
    public BinaryDocValues select(final SortedBinaryDocValues values, final BytesRef missingValue, final BitSet rootDocs, final DocIdSet innerDocSet, int maxDoc) throws IOException {
        if (rootDocs == null || innerDocSet == null) {
            return select(FieldData.emptySortedBinary(maxDoc), missingValue);
        }

        final DocIdSetIterator innerDocs = innerDocSet.iterator();
        if (innerDocs == null) {
            return select(FieldData.emptySortedBinary(maxDoc), missingValue);
        }

        final BinaryDocValues selectedValues = select(values, new BytesRef());
        final Bits docsWithValue;
        if (FieldData.unwrapSingleton(values) != null) {
            docsWithValue = FieldData.unwrapSingletonBits(values);
        } else {
            docsWithValue = FieldData.docsWithValue(values, maxDoc);
        }
        return new BinaryDocValues() {

            final BytesRefBuilder spare = new BytesRefBuilder();

            int lastSeenRootDoc = 0;
            BytesRef lastEmittedValue = missingValue;

            @Override
            public BytesRef get(int rootDoc) {
                assert rootDocs.get(rootDoc) : "can only sort root documents";
                assert rootDoc >= lastSeenRootDoc : "can only evaluate current and upcoming root docs";
                if (rootDoc == lastSeenRootDoc) {
                    return lastEmittedValue;
                }

                try {
                    final int prevRootDoc = rootDocs.prevSetBit(rootDoc - 1);
                    final int firstNestedDoc;
                    if (innerDocs.docID() > prevRootDoc) {
                        firstNestedDoc = innerDocs.docID();
                    } else {
                        firstNestedDoc = innerDocs.advance(prevRootDoc + 1);
                    }

                    BytesRefBuilder accumulated = null;

                    for (int doc = firstNestedDoc; doc > prevRootDoc && doc < rootDoc; doc = innerDocs.nextDoc()) {
                        values.setDocument(doc);
                        final BytesRef innerValue = selectedValues.get(doc);
                        if (innerValue.length > 0 || docsWithValue == null || docsWithValue.get(doc)) {
                            if (accumulated == null) {
                                spare.copyBytes(innerValue);
                                accumulated = spare;
                            } else {
                                final BytesRef applied = apply(accumulated.get(), innerValue);
                                if (applied == innerValue) {
                                    accumulated.copyBytes(innerValue);
                                }
                            }
                        }
                    }

                    lastSeenRootDoc = rootDoc;
                    if (accumulated == null) {
                        lastEmittedValue = missingValue;
                    } else {
                        lastEmittedValue = accumulated.get();
                    }
                    return lastEmittedValue;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    protected int pick(RandomAccessOrds values) {
        throw new ElasticsearchIllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link SortedDocValues} instance that can be used to sort documents
     * with this mode and the provided values.
     */
    public SortedDocValues select(final RandomAccessOrds values) {
        if (values.getValueCount() >= Integer.MAX_VALUE) {
            throw new UnsupportedOperationException("fields containing more than " + (Integer.MAX_VALUE-1) + " unique terms are unsupported");
        }

        final SortedDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            return singleton;
        } else {
            return new SortedDocValues() {
                @Override
                public int getOrd(int docID) {
                    values.setDocument(docID);
                    return pick(values);
                }

                @Override
                public BytesRef lookupOrd(int ord) {
                    return values.lookupOrd(ord);
                }

                @Override
                public int getValueCount() {
                    return (int) values.getValueCount();
                }
            };
        }
    }

    /**
     * Return a {@link SortedDocValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     *
     * For every root document, the values of its inner documents will be aggregated.
     *
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     */
    public SortedDocValues select(final RandomAccessOrds values, final BitSet rootDocs, final DocIdSet innerDocSet) throws IOException {
        if (rootDocs == null || innerDocSet == null) {
            return select(DocValues.emptySortedSet());
        }

        final DocIdSetIterator innerDocs = innerDocSet.iterator();
        if (innerDocs == null) {
            return select(DocValues.emptySortedSet());
        }

        final SortedDocValues selectedValues = select(values);
        return new SortedDocValues() {

            int lastSeenRootDoc = 0;
            int lastEmittedOrd = -1;

            @Override
            public BytesRef lookupOrd(int ord) {
                return selectedValues.lookupOrd(ord);
            }

            @Override
            public int getValueCount() {
                return selectedValues.getValueCount();
            }

            @Override
            public int getOrd(int rootDoc) {
                assert rootDocs.get(rootDoc) : "can only sort root documents";
                assert rootDoc >= lastSeenRootDoc : "can only evaluate current and upcoming root docs";
                if (rootDoc == lastSeenRootDoc) {
                    return lastEmittedOrd;
                }

                try {
                    final int prevRootDoc = rootDocs.prevSetBit(rootDoc - 1);
                    final int firstNestedDoc;
                    if (innerDocs.docID() > prevRootDoc) {
                        firstNestedDoc = innerDocs.docID();
                    } else {
                        firstNestedDoc = innerDocs.advance(prevRootDoc + 1);
                    }
                    int ord = -1;

                    for (int doc = firstNestedDoc; doc > prevRootDoc && doc < rootDoc; doc = innerDocs.nextDoc()) {
                        final int innerOrd = selectedValues.getOrd(doc);
                        if (innerOrd != -1) {
                            if (ord == -1) {
                                ord = innerOrd;
                            } else {
                                ord = applyOrd(ord, innerOrd);
                            }
                        }
                    }

                    lastSeenRootDoc = rootDoc;
                    return lastEmittedOrd = ord;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
