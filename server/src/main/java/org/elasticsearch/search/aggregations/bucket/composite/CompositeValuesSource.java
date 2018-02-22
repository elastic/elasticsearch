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
package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.function.Function;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

/**
 * A wrapper for {@link ValuesSource} that can record and compare values produced during a collection.
 */
abstract class CompositeValuesSource<T extends Comparable<T>> {
    protected final int size;
    protected final int reverseMul;
    protected T afterValue;

    /**
     * Ctr
     * @param size The number of values to record.
     * @param reverseMul -1 if the natural order ({@link SortOrder#ASC} should be reversed.
     */
    CompositeValuesSource(int size, int reverseMul) {
        this.size = size;
        this.reverseMul = reverseMul;
        this.afterValue = null;
    }

    /**
     * The type of this source.
     */
    abstract String type();

    /**
     * Copies the current value in <code>slot</code>.
     */
    abstract void copyCurrent(int slot);

    /**
     * Compares the value in <code>from</code> with the value in <code>to</code>.
     */
    abstract int compare(int from, int to);

    /**
     * Compares the current value with the value in <code>slot</code>.
     */
    abstract int compareCurrent(int slot);

    /**
     * Compares the current value with the after value set in this source.
     */
    abstract int compareCurrentWithAfter();

    /**
     * Sets the after value for this source. Values that compares smaller are filtered.
     */
    abstract void setAfter(Comparable<?> value);

    /**
     * Returns the after value set for this source.
     */
    T getAfter() {
        return afterValue;
    }

    /**
     * Transforms the value in <code>slot</code> to a {@link Comparable} object.
     */
    abstract T toComparable(int slot) throws IOException;

    /**
     * Gets the {@link LeafCollector} that will record the values of the visited documents.
     */
    abstract LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException;

    abstract LeafBucketCollector getLeafCollector(Comparable<?> value,
                                                  LeafReaderContext context, LeafBucketCollector next) throws IOException;

    /**
     * Creates a {@link CompositeValuesSource} that generates long values.
     */
    static CompositeValuesSource<Long> createLong(CheckedFunction<LeafReaderContext, SortedNumericDocValues,
            IOException> docValuesFunc, DocValueFormat format, int size, int reverseMul) {
        return new LongValuesSource(docValuesFunc, format, size, reverseMul);
    }

    /**
     * Creates a {@link CompositeValuesSource} that generates double values.
     */
    static CompositeValuesSource<Double> createDouble(CheckedFunction<LeafReaderContext, SortedNumericDoubleValues,
            IOException> docValuesFunc, int size, int reverseMul) {
        return new DoubleValuesSource(docValuesFunc, size, reverseMul);
    }

    /**
     * Creates a {@link CompositeValuesSource} that generates binary values.
     */
    static CompositeValuesSource<BytesRef> createBinary(CheckedFunction<LeafReaderContext, SortedBinaryDocValues,
            IOException> docValuesFunc, int size, int reverseMul) {
        return new BinaryValuesSource(docValuesFunc, size, reverseMul);
    }

    /**
     * Creates a {@link CompositeValuesSource} that generates global ordinal values.
     */
    static CompositeValuesSource<BytesRef> createGlobalOrdinals(CheckedFunction<LeafReaderContext, SortedSetDocValues,
            IOException> docValuesFunc, int size, int reverseMul) {
        return new GlobalOrdinalValuesSource(docValuesFunc, size, reverseMul);
    }

    /**
     * A {@link CompositeValuesSource} for global ordinals
     */
    private static class GlobalOrdinalValuesSource extends CompositeValuesSource<BytesRef> {
        private final CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc;
        private final long[] values;
        private SortedSetDocValues lookup;
        private long currentValue;
        private Long afterValueGlobalOrd;
        private boolean isTopValueInsertionPoint;

        GlobalOrdinalValuesSource(CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc,
                                  int size, int reverseMul) {
            super(size, reverseMul);
            this.docValuesFunc = docValuesFunc;
            this.values = new long[size];
        }

        @Override
        String type() {
            return "global_ordinals";
        }

        @Override
        void copyCurrent(int slot) {
            values[slot] = currentValue;
        }

        @Override
        int compare(int from, int to) {
            return Long.compare(values[from], values[to]) * reverseMul;
        }

        @Override
        int compareCurrent(int slot) {
            return Long.compare(currentValue, values[slot]) * reverseMul;
        }

        @Override
        int compareCurrentWithAfter() {
            int cmp = Long.compare(currentValue, afterValueGlobalOrd);
            if (cmp == 0 && isTopValueInsertionPoint) {
                // the top value is missing in this shard, the comparison is against
                // the insertion point of the top value so equality means that the value
                // is "after" the insertion point.
                return reverseMul;
            }
            return cmp * reverseMul;
        }

        @Override
        void setAfter(Comparable<?> value) {
            if (value instanceof BytesRef) {
                afterValue = (BytesRef) value;
            } else if (value instanceof String) {
                afterValue = new BytesRef(value.toString());
            } else {
                throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
            }
        }

        @Override
        BytesRef toComparable(int slot) throws IOException {
            return BytesRef.deepCopyOf(lookup.lookupOrd(values[slot]));
        }

        @Override
        LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
            final SortedSetDocValues dvs = docValuesFunc.apply(context);
            if (lookup == null) {
                initLookup(dvs);
            }
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (dvs.advanceExact(doc)) {
                        long ord;
                        while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                            currentValue = ord;
                            next.collect(doc, bucket);
                        }
                    }
                }
            };
        }

        @Override
        LeafBucketCollector getLeafCollector(Comparable<?> value, LeafReaderContext context, LeafBucketCollector next) throws IOException {
            if (value.getClass() != BytesRef.class) {
                throw new IllegalArgumentException("Expected BytesRef, got " + value.getClass());
            }
            BytesRef term = (BytesRef) value;
            final SortedSetDocValues dvs = docValuesFunc.apply(context);
            if (lookup == null) {
                initLookup(dvs);
            }
            return new LeafBucketCollector() {
                boolean currentValueIsSet = false;
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (!currentValueIsSet) {
                        if (dvs.advanceExact(doc)) {
                            long ord;
                            while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                                if (term.equals(lookup.lookupOrd(ord))) {
                                    currentValueIsSet = true;
                                    currentValue = ord;
                                    break;
                                }
                            }
                        }
                    }
                    assert currentValueIsSet;
                    next.collect(doc, bucket);
                }
            };
        }

        private void initLookup(SortedSetDocValues dvs) throws IOException {
            lookup = dvs;
            if (afterValue != null && afterValueGlobalOrd == null) {
                afterValueGlobalOrd = lookup.lookupTerm(afterValue);
                if (afterValueGlobalOrd < 0) {
                    // convert negative insert position
                    afterValueGlobalOrd = -afterValueGlobalOrd - 1;
                    isTopValueInsertionPoint = true;
                }
            }
        }
    }

    /**
     * A {@link CompositeValuesSource} for binary source ({@link BytesRef})
     */
    private static class BinaryValuesSource extends CompositeValuesSource<BytesRef> {
        private final CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValuesFunc;
        private final BytesRef[] values;
        private BytesRef currentValue;

        BinaryValuesSource(CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValuesFunc,
                           int size, int reverseMul) {
            super(size, reverseMul);
            this.docValuesFunc = docValuesFunc;
            this.values = new BytesRef[size];
        }

        @Override
        String type() {
            return "binary";
        }

        @Override
        public void copyCurrent(int slot) {
            values[slot] = BytesRef.deepCopyOf(currentValue);
        }

        @Override
        public int compare(int from, int to) {
            return compareValues(values[from], values[to]);
        }

        @Override
        int compareCurrent(int slot) {
            return compareValues(currentValue, values[slot]);
        }

        @Override
        int compareCurrentWithAfter() {
            return compareValues(currentValue, afterValue);
        }

        int compareValues(BytesRef v1, BytesRef v2) {
            return v1.compareTo(v2) * reverseMul;
        }

        @Override
        void setAfter(Comparable<?> value) {
            if (value.getClass() == BytesRef.class) {
                afterValue = (BytesRef) value;
            } else if (value.getClass() == String.class) {
                afterValue = new BytesRef((String) value);
            } else {
                throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
            }
        }

        @Override
        BytesRef toComparable(int slot) {
            return values[slot];
        }

        @Override
        LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
            final SortedBinaryDocValues dvs = docValuesFunc.apply(context);
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (dvs.advanceExact(doc)) {
                        int num = dvs.docValueCount();
                        for (int i = 0; i < num; i++) {
                            currentValue = dvs.nextValue();
                            next.collect(doc, bucket);
                        }
                    }
                }
            };
        }

        @Override
        LeafBucketCollector getLeafCollector(Comparable<?> value, LeafReaderContext context, LeafBucketCollector next) {
            if (value.getClass() != BytesRef.class) {
                throw new IllegalArgumentException("Expected BytesRef, got " + value.getClass());
            }
            final BytesRef filterValue = (BytesRef) value;
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    currentValue = filterValue;
                    next.collect(doc, bucket);
                }
            };
        }
    }

    /**
     * A {@link CompositeValuesSource} for longs.
     */
    private static class LongValuesSource extends CompositeValuesSource<Long> {
        private final CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValuesFunc;
        private final long[] values;
        private long currentValue;

        // handles "format" for date histogram source
        private final DocValueFormat format;

        LongValuesSource(CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValuesFunc,
                         DocValueFormat format, int size, int reverseMul) {
            super(size, reverseMul);
            this.docValuesFunc = docValuesFunc;
            this.values = new long[size];
            this.format = format;
        }

        @Override
        String type() {
            return "long";
        }

        @Override
        void copyCurrent(int slot) {
            values[slot] = currentValue;
        }

        @Override
        int compare(int from, int to) {
            return compareValues(values[from], values[to]);
        }

        @Override
        int compareCurrent(int slot) {
            return compareValues(currentValue, values[slot]);
        }

        @Override
        int compareCurrentWithAfter() {
            return compareValues(currentValue, afterValue);
        }

        private int compareValues(long v1, long v2) {
            return Long.compare(v1, v2) * reverseMul;
        }

        @Override
        void setAfter(Comparable<?> value) {
            if (value instanceof Number) {
                afterValue = ((Number) value).longValue();
            } else {
                // for date histogram source with "format", the after value is formatted
                // as a string so we need to retrieve the original value in milliseconds.
                afterValue = format.parseLong(value.toString(), false, () -> {
                    throw new IllegalArgumentException("now() is not supported in [after] key");
                });
            }
        }

        @Override
        Long toComparable(int slot) {
            return values[slot];
        }

        @Override
        LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
            final SortedNumericDocValues dvs = docValuesFunc.apply(context);
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (dvs.advanceExact(doc)) {
                        int num = dvs.docValueCount();
                        for (int i = 0; i < num; i++) {
                            currentValue = dvs.nextValue();
                            next.collect(doc, bucket);
                        }
                    }
                }
            };
        }

        @Override
        LeafBucketCollector getLeafCollector(Comparable<?> value, LeafReaderContext context, LeafBucketCollector next) {
            if (value.getClass() != Long.class) {
                throw new IllegalArgumentException("Expected Long, got " + value.getClass());
            }
            long filterValue = (Long) value;
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    currentValue = filterValue;
                    next.collect(doc, bucket);
                }
            };
        }
    }

    /**
     * A {@link CompositeValuesSource} for doubles.
     */
    private static class DoubleValuesSource extends CompositeValuesSource<Double> {
        private final CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValuesFunc;
        private final double[] values;
        private double currentValue;

        DoubleValuesSource(CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValuesFunc,
                           int size, int reverseMul) {
            super(size, reverseMul);
            this.docValuesFunc = docValuesFunc;
            this.values = new double[size];
        }

        @Override
        String type() {
            return "long";
        }

        @Override
        void copyCurrent(int slot) {
            values[slot] = currentValue;
        }

        @Override
        int compare(int from, int to) {
            return compareValues(values[from], values[to]);
        }

        @Override
        int compareCurrent(int slot) {
            return compareValues(currentValue, values[slot]);
        }

        @Override
        int compareCurrentWithAfter() {
            return compareValues(currentValue, afterValue);
        }

        private int compareValues(double v1, double v2) {
            return Double.compare(v1, v2) * reverseMul;
        }

        @Override
        void setAfter(Comparable<?> value) {
            if (value instanceof Number) {
                afterValue = ((Number) value).doubleValue();
            } else {
                afterValue = Double.parseDouble(value.toString());
            }
        }

        @Override
        Double toComparable(int slot) {
            return values[slot];
        }

        @Override
        LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
            final SortedNumericDoubleValues dvs = docValuesFunc.apply(context);
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (dvs.advanceExact(doc)) {
                        int num = dvs.docValueCount();
                        for (int i = 0; i < num; i++) {
                            currentValue = dvs.nextValue();
                            next.collect(doc, bucket);
                        }
                    }
                }
            };
        }

        @Override
        LeafBucketCollector getLeafCollector(Comparable<?> value, LeafReaderContext context, LeafBucketCollector next) {
            if (value.getClass() != Double.class) {
                throw new IllegalArgumentException("Expected Double, got " + value.getClass());
            }
            double filterValue = (Double) value;
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    currentValue = filterValue;
                    next.collect(doc, bucket);
                }
            };
        }
    }
}
