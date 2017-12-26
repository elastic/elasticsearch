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
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalMapping;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

/**
 * A wrapper for {@link ValuesSource} that can record and compare values produced during a collection.
 */
abstract class CompositeValuesSource<VS extends ValuesSource, T extends Comparable<T>> {
    interface Collector {
        void collect(int doc) throws IOException;
    }

    protected final VS vs;
    protected final int size;
    protected final int reverseMul;
    protected T topValue;

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
    }

    /**
     * The type of this source.
     */
    abstract String type();

    /**
     * Moves the value in <code>from</code> in <code>to</code>.
     * The value present in <code>to</code> is overridden.
     */
    abstract void move(int from, int to);

    /**
     * Compares the value in <code>from</code> with the value in <code>to</code>.
     */
    abstract int compare(int from, int to);

    /**
     * Compares the value in <code>slot</code> with the top value in this source.
     */
    abstract int compareTop(int slot);

    /**
     * Sets the top value for this source. Values that compares smaller should not be recorded.
     */
    abstract void setTop(Comparable<?> value);

    /**
     * Transforms the value in <code>slot</code> to a {@link Comparable} object.
     */
    abstract Comparable<T> toComparable(int slot) throws IOException;

    /**
     * Gets the {@link LeafCollector} that will record the values of the visited documents.
     */
    abstract Collector getLeafCollector(LeafReaderContext context, Collector next) throws IOException;

    /**
     * Creates a {@link CompositeValuesSource} that generates long values.
     */
    static CompositeValuesSource<ValuesSource.Numeric, Long> wrapLong(ValuesSource.Numeric vs, int size, int reverseMul) {
        return new LongValuesSource(vs, size, reverseMul);
    }

    /**
     * Creates a {@link CompositeValuesSource} that generates double values.
     */
    static CompositeValuesSource<ValuesSource.Numeric, Double> wrapDouble(ValuesSource.Numeric vs, int size, int reverseMul) {
        return new DoubleValuesSource(vs, size, reverseMul);
    }

    /**
     * Creates a {@link CompositeValuesSource} that generates binary values.
     */
    static CompositeValuesSource<ValuesSource.Bytes, BytesRef> wrapBinary(ValuesSource.Bytes vs, int size, int reverseMul) {
        return new BinaryValuesSource(vs, size, reverseMul);
    }

    /**
     * Creates a {@link CompositeValuesSource} that generates global ordinal values.
     */
    static CompositeValuesSource<ValuesSource.Bytes.WithOrdinals, BytesRef> wrapGlobalOrdinals(ValuesSource.Bytes.WithOrdinals vs,
                                                                                               int size,
                                                                                               int reverseMul) {
        return new GlobalOrdinalValuesSource(vs, size, reverseMul);
    }

    /**
     * A {@link CompositeValuesSource} for global ordinals
     */
    private static class GlobalOrdinalValuesSource extends CompositeValuesSource<ValuesSource.Bytes.WithOrdinals, BytesRef> {
        private final long[] values;
        private SortedSetDocValues lookup;
        private Long topValueGlobalOrd;
        private boolean isTopValueInsertionPoint;

        GlobalOrdinalValuesSource(ValuesSource.Bytes.WithOrdinals vs, int size, int reverseMul) {
            super(vs, size, reverseMul);
            this.values = new long[size];
        }

        @Override
        String type() {
            return "global_ordinals";
        }

        @Override
        void move(int from, int to) {
            values[to] = values[from];
        }

        @Override
        int compare(int from, int to) {
            return Long.compare(values[from], values[to]) * reverseMul;
        }

        @Override
        int compareTop(int slot) {
            int cmp = Long.compare(values[slot], topValueGlobalOrd);
            if (cmp == 0 && isTopValueInsertionPoint) {
                // the top value is missing in this shard, the comparison is against
                // the insertion point of the top value so equality means that the value
                // is "after" the insertion point.
                return reverseMul;
            }
            return cmp * reverseMul;
        }

        @Override
        void setTop(Comparable<?> value) {
            if (value instanceof BytesRef) {
                topValue = (BytesRef) value;
            } else if (value instanceof String) {
                topValue = new BytesRef(value.toString());
            } else {
                throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
            }
        }

        @Override
        Comparable<BytesRef> toComparable(int slot) throws IOException {
            return BytesRef.deepCopyOf(lookup.lookupOrd(values[slot]));
        }

        @Override
        Collector getLeafCollector(LeafReaderContext context, Collector next) throws IOException {
            final SortedSetDocValues dvs = vs.globalOrdinalsValues(context);
            if (lookup == null) {
                lookup = dvs;
                if (topValue != null && topValueGlobalOrd == null) {
                    topValueGlobalOrd = lookup.lookupTerm(topValue);
                    if (topValueGlobalOrd < 0) {
                        // convert negative insert position
                        topValueGlobalOrd = -topValueGlobalOrd - 1;
                        isTopValueInsertionPoint = true;
                    }
                }
            }
            return doc -> {
                if (dvs.advanceExact(doc)) {
                    long ord;
                    while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                        values[0] = ord;
                        next.collect(doc);
                    }
                }
            };
        }

        private static long lookupGlobalOrdinals(GlobalOrdinalMapping mapping, BytesRef key) throws IOException {
            long low = 0;
            long high = mapping.getValueCount();

            while (low <= high) {
                long mid = (low + high) >>> 1;
                BytesRef midVal = mapping.lookupOrd(mid);
                int cmp = midVal.compareTo(key);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return mid;
                }
            }
            return low-1;
        }
    }

    /**
     * A {@link CompositeValuesSource} for binary source ({@link BytesRef})
     */
    private static class BinaryValuesSource extends CompositeValuesSource<ValuesSource.Bytes, BytesRef> {
        private final BytesRef[] values;

        BinaryValuesSource(ValuesSource.Bytes vs, int size, int reverseMul) {
            super(vs, size, reverseMul);
            this.values = new BytesRef[size];
        }

        @Override
        String type() {
            return "binary";
        }

        @Override
        public void move(int from, int to) {
            values[to] = BytesRef.deepCopyOf(values[from]);
        }

        @Override
        public int compare(int from, int to) {
            return values[from].compareTo(values[to]) * reverseMul;
        }

        @Override
        int compareTop(int slot) {
            return values[slot].compareTo(topValue) * reverseMul;
        }

        @Override
        void setTop(Comparable<?> value) {
            if (value.getClass() == BytesRef.class) {
                topValue = (BytesRef) value;
            } else if (value.getClass() == String.class) {
                topValue = new BytesRef((String) value);
            } else {
                throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
            }
        }

        @Override
        Comparable<BytesRef> toComparable(int slot) {
            return values[slot];
        }

        @Override
        Collector getLeafCollector(LeafReaderContext context, Collector next) throws IOException {
            final SortedBinaryDocValues dvs = vs.bytesValues(context);
            return doc -> {
                if (dvs.advanceExact(doc)) {
                    int num = dvs.docValueCount();
                    for (int i = 0; i < num; i++) {
                        values[0] = dvs.nextValue();
                        next.collect(doc);
                    }
                }
            };
        }
    }

    /**
     * A {@link CompositeValuesSource} for longs.
     */
    private static class LongValuesSource extends CompositeValuesSource<ValuesSource.Numeric, Long> {
        private final long[] values;

        LongValuesSource(ValuesSource.Numeric vs, int size, int reverseMul) {
            super(vs, size, reverseMul);
            this.values = new long[size];
        }

        @Override
        String type() {
            return "long";
        }

        @Override
        void move(int from, int to) {
            values[to] = values[from];
        }

        @Override
        int compare(int from, int to) {
            return Long.compare(values[from], values[to]) * reverseMul;
        }

        @Override
        int compareTop(int slot) {
            return Long.compare(values[slot], topValue) * reverseMul;
        }

        @Override
        void setTop(Comparable<?> value) {
            if (value instanceof Number) {
                topValue = ((Number) value).longValue();
            } else {
                topValue = Long.parseLong(value.toString());
            }
        }

        @Override
        Comparable<Long> toComparable(int slot) {
            return values[slot];
        }

        @Override
        Collector getLeafCollector(LeafReaderContext context, Collector next) throws IOException {
            final SortedNumericDocValues dvs = vs.longValues(context);
            return doc -> {
                if (dvs.advanceExact(doc)) {
                    int num = dvs.docValueCount();
                    for (int i = 0; i < num; i++) {
                        values[0] = dvs.nextValue();
                        next.collect(doc);
                    }
                }
            };
        }
    }

    /**
     * A {@link CompositeValuesSource} for doubles.
     */
    private static class DoubleValuesSource extends CompositeValuesSource<ValuesSource.Numeric, Double> {
        private final double[] values;

        DoubleValuesSource(ValuesSource.Numeric vs, int size, int reverseMul) {
            super(vs, size, reverseMul);
            this.values = new double[size];
        }

        @Override
        String type() {
            return "long";
        }

        @Override
        void move(int from, int to) {
            values[to] = values[from];
        }

        @Override
        int compare(int from, int to) {
            return Double.compare(values[from], values[to]) * reverseMul;
        }

        @Override
        int compareTop(int slot) {
            return Double.compare(values[slot], topValue) * reverseMul;
        }

        @Override
        void setTop(Comparable<?> value) {
            if (value instanceof Number) {
                topValue = ((Number) value).doubleValue();
            } else {
                topValue = Double.parseDouble(value.toString());
            }
        }

        @Override
        Comparable<Double> toComparable(int slot) {
            return values[slot];
        }

        @Override
        Collector getLeafCollector(LeafReaderContext context, Collector next) throws IOException {
            final SortedNumericDoubleValues dvs = vs.doubleValues(context);
            return doc -> {
                if (dvs.advanceExact(doc)) {
                    int num = dvs.docValueCount();
                    for (int i = 0; i < num; i++) {
                        values[0] = dvs.nextValue();
                        next.collect(doc);
                    }
                }
            };
        }
    }
}
