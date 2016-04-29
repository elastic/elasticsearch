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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.util.Locale;

/**
 * Defines what values to pick in the case a document contains multiple values for a particular field.
 */
public enum MultiValueMode implements Writeable {

    /**
     * Pick the sum of all the values.
     */
    SUM {
        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, int doc) {
            values.setDocument(doc);
            final int count = values.count();
            if (count > 0) {
                long total = 0;
                for (int index = 0; index < count; ++index) {
                    total += values.valueAt(index);
                }
                return total;
            } else {
                return missingValue;
            }
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                int totalCount = 0;
                long totalValue = 0;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    values.setDocument(doc);
                    final int count = values.count();
                    for (int index = 0; index < count; ++index) {
                        totalValue += values.valueAt(index);
                    }
                    totalCount += count;
                }
                return totalCount > 0 ? totalValue : missingValue;
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, int doc) {
            values.setDocument(doc);
            final int count = values.count();
            if (count > 0) {
                double total = 0;
                for (int index = 0; index < count; ++index) {
                    total += values.valueAt(index);
                }
                return total;
            } else {
                return missingValue;
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                int totalCount = 0;
                double totalValue = 0;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    values.setDocument(doc);
                    final int count = values.count();
                    for (int index = 0; index < count; ++index) {
                        totalValue += values.valueAt(index);
                    }
                    totalCount += count;
                }
                return totalCount > 0 ? totalValue : missingValue;
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected double pick(UnsortedNumericDoubleValues values, double missingValue, int doc) {
            values.setDocument(doc);
            final int count = values.count();
            if (count > 0) {
                double total = 0;
                for (int index = 0; index < count; ++index) {
                    total += values.valueAt(index);
                }
                return total;
            } else {
                return missingValue;
            }
        }
    },

    /**
     * Pick the average of all the values.
     */
    AVG {
        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, int doc) {
            values.setDocument(doc);
            final int count = values.count();
            if (count > 0) {
                long total = 0;
                for (int index = 0; index < count; ++index) {
                    total += values.valueAt(index);
                }
                return count > 1 ? Math.round((double)total/(double)count) : total;
            } else {
                return missingValue;
            }
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                int totalCount = 0;
                long totalValue = 0;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    values.setDocument(doc);
                    final int count = values.count();
                    for (int index = 0; index < count; ++index) {
                        totalValue += values.valueAt(index);
                    }
                    totalCount += count;
                }
                if (totalCount < 1) {
                    return missingValue;
                }
                return totalCount > 1 ? Math.round((double)totalValue/(double)totalCount) : totalValue;
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, int doc) {
            values.setDocument(doc);
            final int count = values.count();
            if (count > 0) {
                double total = 0;
                for (int index = 0; index < count; ++index) {
                    total += values.valueAt(index);
                }
                return total/count;
            } else {
                return missingValue;
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                int totalCount = 0;
                double totalValue = 0;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    values.setDocument(doc);
                    final int count = values.count();
                    for (int index = 0; index < count; ++index) {
                        totalValue += values.valueAt(index);
                    }
                    totalCount += count;
                }
                if (totalCount < 1) {
                    return missingValue;
                }
                return totalValue/totalCount;
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected double pick(UnsortedNumericDoubleValues values, double missingValue, int doc) {
            values.setDocument(doc);
            final int count = values.count();
            if (count > 0) {
                double total = 0;
                for (int index = 0; index < count; ++index) {
                    total += values.valueAt(index);
                }
                return total/count;
            } else {
                return missingValue;
            }
        }
    },

    /**
     * Pick the median of the values.
     */
    MEDIAN {
        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, int doc) {
            values.setDocument(doc);
            int count = values.count();
            if (count > 0) {
                if (count % 2 == 0) {
                    count /= 2;
                    return Math.round((values.valueAt(count - 1) + values.valueAt(count))/2.0);
                } else {
                    count /= 2;
                    return values.valueAt(count);
                }
            } else {
                return missingValue;
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, int doc) {
            values.setDocument(doc);
            int count = values.count();
            if (count > 0) {
                if (count % 2 == 0) {
                    count /= 2;
                    return (values.valueAt(count - 1) + values.valueAt(count))/2.0;
                } else {
                    count /= 2;
                    return values.valueAt(count);
                }
            } else {
                return missingValue;
            }
        }
    },

    /**
     * Pick the lowest value.
     */
    MIN {
        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, int doc) {
            values.setDocument(doc);
            final int count = values.count();
            return count > 0 ? values.valueAt(0) : missingValue;
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                int totalCount = 0;
                long minValue = Long.MAX_VALUE;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    values.setDocument(doc);
                    final int count = values.count();
                    if (count > 0) {
                        minValue = Math.min(minValue, values.valueAt(0));
                    }
                    totalCount += count;
                }
                return totalCount > 0 ? minValue : missingValue;
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, int doc) {
            values.setDocument(doc);
            int count = values.count();
            return count > 0 ? values.valueAt(0) : missingValue;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                int totalCount = 0;
                double minValue = Double.MAX_VALUE;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    values.setDocument(doc);
                    final int count = values.count();
                    if (count > 0) {
                        minValue = Math.min(minValue, values.valueAt(0));
                    }
                    totalCount += count;
                }
                return totalCount > 0 ? minValue : missingValue;
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected BytesRef pick(SortedBinaryDocValues values, BytesRef missingValue, int doc) {
            values.setDocument(doc);
            final int count = values.count();
            return count > 0 ? values.valueAt(0) : missingValue;
        }

        @Override
        protected BytesRef pick(BinaryDocValues values, BytesRefBuilder builder, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                BytesRefBuilder value = null;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    final BytesRef innerValue = values.get(doc);
                    if (innerValue != null) {
                        if (value == null) {
                            builder.copyBytes(innerValue);
                            value = builder;
                        } else {
                            final BytesRef min = value.get().compareTo(innerValue) <= 0 ? value.get() : innerValue;
                            if (min == innerValue) {
                                value.copyBytes(min);
                            }
                        }
                    }
                }
                return value == null ? null : value.get();
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected int pick(RandomAccessOrds values, int doc) {
            values.setDocument(doc);
            return values.cardinality() > 0 ? (int)values.ordAt(0) : -1;
        }

        @Override
        protected int pick(SortedDocValues values, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                int ord = -1;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    final int innerOrd = values.getOrd(doc);
                    if (innerOrd != -1) {
                        ord = ord == -1  ? innerOrd : Math.min(ord, innerOrd);
                    }
                }
                return ord;
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected double pick(UnsortedNumericDoubleValues values, double missingValue, int doc) {
            values.setDocument(doc);
            int count = values.count();
            double min = Double.MAX_VALUE;
            for (int index = 0; index < count; ++index) {
                min = Math.min(values.valueAt(index), min);
            }
            return count > 0 ? min : missingValue;
        }
    },

    /**
     * Pick the highest value.
     */
    MAX {
        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, int doc) {
            values.setDocument(doc);
            final int count = values.count();
            return count > 0 ? values.valueAt(count - 1) : missingValue;
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                int totalCount = 0;
                long maxValue = Long.MIN_VALUE;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    values.setDocument(doc);
                    final int count = values.count();
                    if (count > 0) {
                        maxValue = Math.max(maxValue, values.valueAt(count - 1));
                    }
                    totalCount += count;
                }
                return totalCount > 0 ? maxValue : missingValue;
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, int doc) {
            values.setDocument(doc);
            final int count = values.count();
            return count > 0 ? values.valueAt(count - 1) : missingValue;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                int totalCount = 0;
                double maxValue = Double.MIN_VALUE;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    values.setDocument(doc);
                    final int count = values.count();
                    if (count > 0) {
                        maxValue = Math.max(maxValue, values.valueAt(count - 1));
                    }
                    totalCount += count;
                }
                return totalCount > 0 ? maxValue : missingValue;
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected BytesRef pick(SortedBinaryDocValues values, BytesRef missingValue, int doc) {
            values.setDocument(doc);
            final int count = values.count();
            return count > 0 ? values.valueAt(count - 1) : missingValue;
        }

        @Override
        protected BytesRef pick(BinaryDocValues values, BytesRefBuilder builder, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                BytesRefBuilder value = null;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    final BytesRef innerValue = values.get(doc);
                    if (innerValue != null) {
                        if (value == null) {
                            builder.copyBytes(innerValue);
                            value = builder;
                        } else {
                            final BytesRef max = value.get().compareTo(innerValue) > 0 ? value.get() : innerValue;
                            if (max == innerValue) {
                                value.copyBytes(max);
                            }
                        }
                    }
                }
                return value == null ? null : value.get();
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected int pick(RandomAccessOrds values, int doc) {
            values.setDocument(doc);
            final int count = values.cardinality();
            return count > 0 ? (int)values.ordAt(count - 1) : -1;
        }

        @Override
        protected int pick(SortedDocValues values, DocIdSetIterator docItr, int startDoc, int endDoc) {
            try {
                int ord = -1;
                for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                    final int innerOrd = values.getOrd(doc);
                    if (innerOrd != -1) {
                        ord = Math.max(ord, innerOrd);
                    }
                }
                return ord;
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        protected double pick(UnsortedNumericDoubleValues values, double missingValue, int doc) {
            values.setDocument(doc);
            int count = values.count();
            double max = Double.MIN_VALUE;
            for (int index = 0; index < count; ++index) {
                max = Math.max(values.valueAt(index), max);
            }
            return count > 0 ? max : missingValue;
        }
    };

    /**
     * A case insensitive version of {@link #valueOf(String)}
     *
     * @throws IllegalArgumentException if the given string doesn't match a sort mode or is <code>null</code>.
     */
    public static MultiValueMode fromString(String sortMode) {
        try {
            return valueOf(sortMode.toUpperCase(Locale.ROOT));
        } catch (Throwable t) {
            throw new IllegalArgumentException("Illegal sort mode: " + sortMode);
        }
    }

    /**
     * Return a {@link NumericDocValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     *
     * Allowed Modes: SUM, AVG, MEDIAN, MIN, MAX
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
                    return pick(values, missingValue, docID);
                }
            };
        }
    }

    protected long pick(SortedNumericDocValues values, long missingValue, int doc) {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link NumericDocValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     *
     * For every root document, the values of its inner documents will be aggregated.
     * If none of the inner documents has a value, then <code>missingValue</code> is returned.
     *
     * Allowed Modes: SUM, AVG, MIN, MAX
     *
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     *       The returned instance can only be evaluate the current and upcoming docs
     */
    public NumericDocValues select(final SortedNumericDocValues values, final long missingValue, final BitSet rootDocs, final DocIdSetIterator innerDocs, int maxDoc) throws IOException {
        if (rootDocs == null || innerDocs == null) {
            return select(DocValues.emptySortedNumeric(maxDoc), missingValue);
        }

        return new NumericDocValues() {

            int lastSeenRootDoc = 0;
            long lastEmittedValue = missingValue;

            @Override
            public long get(int rootDoc) {
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

                    lastSeenRootDoc = rootDoc;
                    lastEmittedValue = pick(values, missingValue, innerDocs, firstNestedDoc, rootDoc);
                    return lastEmittedValue;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    protected long pick(SortedNumericDocValues values, long missingValue, DocIdSetIterator docItr, int startDoc, int endDoc) {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link NumericDoubleValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     *
     * Allowed Modes: SUM, AVG, MEDIAN, MIN, MAX
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
                    return pick(values, missingValue, docID);
                }
            };
        }
    }

    protected double pick(SortedNumericDoubleValues values, double missingValue, int doc) {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link NumericDoubleValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     *
     * For every root document, the values of its inner documents will be aggregated.
     * If none of the inner documents has a value, then <code>missingValue</code> is returned.
     *
     * Allowed Modes: SUM, AVG, MIN, MAX
     *
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     *       The returned instance can only be evaluate the current and upcoming docs
     */
    public NumericDoubleValues select(final SortedNumericDoubleValues values, final double missingValue, final BitSet rootDocs, final DocIdSetIterator innerDocs, int maxDoc) throws IOException {
        if (rootDocs == null || innerDocs == null) {
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

                    lastSeenRootDoc = rootDoc;
                    lastEmittedValue = pick(values, missingValue, innerDocs, firstNestedDoc, rootDoc);
                    return lastEmittedValue;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    protected double pick(SortedNumericDoubleValues values, double missingValue, DocIdSetIterator docItr, int startDoc, int endDoc) {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link BinaryDocValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     *
     * Allowed Modes: MIN, MAX
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
                    return pick(values, missingValue, docID);
                }
            };
        }
    }

    protected BytesRef pick(SortedBinaryDocValues values, BytesRef missingValue, int doc) {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link BinaryDocValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     *
     * For every root document, the values of its inner documents will be aggregated.
     * If none of the inner documents has a value, then <code>missingValue</code> is returned.
     *
     * Allowed Modes: MIN, MAX
     *
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     *       The returned instance can only be evaluate the current and upcoming docs
     */
    public BinaryDocValues select(final SortedBinaryDocValues values, final BytesRef missingValue, final BitSet rootDocs, final DocIdSetIterator innerDocs, int maxDoc) throws IOException {
        if (rootDocs == null || innerDocs == null) {
            return select(FieldData.emptySortedBinary(maxDoc), missingValue);
        }
        final BinaryDocValues selectedValues = select(values, null);

        return new BinaryDocValues() {

            final BytesRefBuilder builder = new BytesRefBuilder();

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

                    lastSeenRootDoc = rootDoc;
                    lastEmittedValue = pick(selectedValues, builder, innerDocs, firstNestedDoc, rootDoc);
                    if (lastEmittedValue == null) {
                        lastEmittedValue = missingValue;
                    }
                    return lastEmittedValue;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    protected BytesRef pick(BinaryDocValues values, BytesRefBuilder builder, DocIdSetIterator docItr, int startDoc, int endDoc) {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link SortedDocValues} instance that can be used to sort documents
     * with this mode and the provided values.
     *
     * Allowed Modes: MIN, MAX
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
                    return pick(values, docID);
                }

                @Override
                public BytesRef lookupOrd(int ord) {
                    return values.lookupOrd(ord);
                }

                @Override
                public int getValueCount() {
                    return (int)values.getValueCount();
                }
            };
        }
    }

    protected int pick(RandomAccessOrds values, int doc) {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link SortedDocValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     *
     * For every root document, the values of its inner documents will be aggregated.
     *
     * Allowed Modes: MIN, MAX
     *
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     *       The returned instance can only be evaluate the current and upcoming docs
     */
    public SortedDocValues select(final RandomAccessOrds values, final BitSet rootDocs, final DocIdSetIterator innerDocs) throws IOException {
        if (rootDocs == null || innerDocs == null) {
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

                    lastSeenRootDoc = rootDoc;
                    return lastEmittedOrd = pick(selectedValues, innerDocs, firstNestedDoc, rootDoc);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    protected int pick(SortedDocValues values, DocIdSetIterator docItr, int startDoc, int endDoc) {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link NumericDoubleValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     *
     * Allowed Modes: SUM, AVG, MIN, MAX
     */
    public NumericDoubleValues select(final UnsortedNumericDoubleValues values, final double missingValue) {
        return new NumericDoubleValues() {
            @Override
            public double get(int docID) {
                return pick(values, missingValue, docID);
            }
        };
    }

    protected double pick(UnsortedNumericDoubleValues values, final double missingValue, int doc) {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Interface allowing custom value generators to be used in MultiValueMode.
     */
    public interface UnsortedNumericDoubleValues {
        int count();
        void setDocument(int docId);
        double valueAt(int index);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.ordinal());
    }

    public static MultiValueMode readMultiValueModeFrom(StreamInput in) throws IOException {
        int ordinal = in.readVInt();
        if (ordinal < 0 || ordinal >= values().length) {
            throw new IOException("Unknown MultiValueMode ordinal [" + ordinal + "]");
        }
        return values()[ordinal];
    }
}
