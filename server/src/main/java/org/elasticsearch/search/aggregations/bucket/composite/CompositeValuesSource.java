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
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.function.Predicate;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

/**
 * A wrapper for {@link ValuesSource} that can record and compare values produced during a collection.
 */
abstract class CompositeValuesSource<VS extends ValuesSource, T> {
    protected final VS vs;
    protected final int size;
    protected final int reverseMul;

    protected T afterValue;

    /**
     *
     * @param vs The original {@link ValuesSource}.
     * @param size The number of values to record.
     * @param reverseMul -1 if the natural order ({@link SortOrder#ASC} should be reversed.
     */
    CompositeValuesSource(VS vs, int size, int reverseMul) {
        this.vs = vs;
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

    abstract Comparable<T> getAfter();

    /**
     * Transforms the value in <code>slot</code> to a {@link Comparable} object.
     */
    abstract Comparable<T> toComparable(int slot) throws IOException;

    /**
     * Gets the {@link LeafCollector} that will record the values of the visited documents.
     */
    abstract LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException;

    abstract LeafBucketCollector getLeafCollector(Comparable<?> value, LeafBucketCollector next) throws IOException;

    /**
     * Creates a {@link CompositeValuesSource} that generates long values.
     */
    static CompositeValuesSource<ValuesSource.Numeric, Long> createLong(ValuesSource.Numeric vs, DocValueFormat format,
                                                                        int size, int reverseMul) {
        return new LongValuesSource(vs, format, size, reverseMul);
    }

    /**
     * Creates a {@link CompositeValuesSource} that generates double values.
     */
    static CompositeValuesSource<ValuesSource.Numeric, Double> createDouble(ValuesSource.Numeric vs, int size, int reverseMul) {
        return new DoubleValuesSource(vs, size, reverseMul);
    }

    /**
     * Creates a {@link CompositeValuesSource} that generates binary values.
     */
    static CompositeValuesSource<ValuesSource.Bytes, BytesRef> createBinary(ValuesSource.Bytes vs, int size, int reverseMul) {
        return new BinaryValuesSource(vs, size, reverseMul);
    }

    /**
     * A {@link CompositeValuesSource} for binary source ({@link BytesRef})
     */
    private static class BinaryValuesSource extends CompositeValuesSource<ValuesSource.Bytes, BytesRef> {
        private final BytesRef[] values;
        private BytesRef currentValue;

        BinaryValuesSource(ValuesSource.Bytes vs, int size, int reverseMul) {
            super(vs, size, reverseMul);
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
        Comparable<BytesRef> getAfter() {
            return afterValue;
        }

        @Override
        Comparable<BytesRef> toComparable(int slot) {
            return values[slot];
        }

        @Override
        LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
            final SortedBinaryDocValues dvs = vs.bytesValues(context);
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
        LeafBucketCollector getLeafCollector(Comparable<?> value, LeafBucketCollector next) {
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
    private static class LongValuesSource extends CompositeValuesSource<ValuesSource.Numeric, Long> {
        private final long[] values;
        private long currentValue;

        // handles "format" for date histogram source
        private final DocValueFormat format;

        LongValuesSource(ValuesSource.Numeric vs, DocValueFormat format, int size, int reverseMul) {
            super(vs, size, reverseMul);
            this.format = format;
            this.values = new long[size];
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
        Comparable<Long> getAfter() {
            return afterValue;
        }

        @Override
        Comparable<Long> toComparable(int slot) {
            return values[slot];
        }

        @Override
        LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
            final SortedNumericDocValues dvs = vs.longValues(context);
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
        LeafBucketCollector getLeafCollector(Comparable<?> value, LeafBucketCollector next) {
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
    private static class DoubleValuesSource extends CompositeValuesSource<ValuesSource.Numeric, Double> {
        private final double[] values;
        private double currentValue;

        DoubleValuesSource(ValuesSource.Numeric vs, int size, int reverseMul) {
            super(vs, size, reverseMul);
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
        Comparable<Double> getAfter() {
            return afterValue;
        }

        @Override
        Comparable<Double> toComparable(int slot) {
            return values[slot];
        }

        @Override
        LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
            final SortedNumericDoubleValues dvs = vs.doubleValues(context);
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
        LeafBucketCollector getLeafCollector(Comparable<?> value, LeafBucketCollector next) {
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
