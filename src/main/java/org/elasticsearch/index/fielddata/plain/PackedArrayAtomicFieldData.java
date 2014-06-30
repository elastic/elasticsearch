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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.AppendingDeltaPackedLongBuffer;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 * {@link AtomicNumericFieldData} implementation which stores data in packed arrays to save memory.
 */
public abstract class PackedArrayAtomicFieldData extends AbstractAtomicNumericFieldData {

    public static PackedArrayAtomicFieldData empty() {
        return new Empty();
    }

    protected long size = -1;

    public PackedArrayAtomicFieldData() {
        super(false);
    }

    @Override
    public void close() {
    }

    static class Empty extends PackedArrayAtomicFieldData {

        @Override
        public LongValues getLongValues() {
            return LongValues.EMPTY;
        }

        @Override
        public DoubleValues getDoubleValues() {
            return DoubleValues.EMPTY;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public BytesValues getBytesValues() {
            return BytesValues.EMPTY;
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return ScriptDocValues.EMPTY_LONGS;
        }
    }

    public static class WithOrdinals extends PackedArrayAtomicFieldData {

        private final MonotonicAppendingLongBuffer values;
        private final Ordinals ordinals;

        public WithOrdinals(MonotonicAppendingLongBuffer values, Ordinals ordinals) {
            super();
            this.values = values;
            this.ordinals = ordinals;
        }

        @Override
        public long ramBytesUsed() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + values.ramBytesUsed() + ordinals.ramBytesUsed();
            }
            return size;
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values, ordinals.ordinals());
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values, ordinals.ordinals());
        }

        static class LongValues extends org.elasticsearch.index.fielddata.LongValues.WithOrdinals {

            private final MonotonicAppendingLongBuffer values;

            LongValues(MonotonicAppendingLongBuffer values, BytesValues.WithOrdinals ordinals) {
                super(ordinals);
                this.values = values;
            }

            @Override
            public long getValueByOrd(long ord) {
                assert ord != BytesValues.WithOrdinals.MISSING_ORDINAL;
                return values.get(ord);
            }
        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues.WithOrdinals {

            private final MonotonicAppendingLongBuffer values;

            DoubleValues(MonotonicAppendingLongBuffer values, BytesValues.WithOrdinals ordinals) {
                super(ordinals);
                this.values = values;
            }

            @Override
            public double getValueByOrd(long ord) {
                assert ord != BytesValues.WithOrdinals.MISSING_ORDINAL;
                return values.get(ord);
            }


        }
    }

    /**
     * A single valued case, where not all values are "set", so we have a special
     * value which encodes the fact that the document has no value.
     */
    public static class SingleSparse extends PackedArrayAtomicFieldData {

        private final PackedInts.Mutable values;
        private final long minValue;
        private final long missingValue;

        public SingleSparse(PackedInts.Mutable values, long minValue, long missingValue) {
            super();
            this.values = values;
            this.minValue = minValue;
            this.missingValue = missingValue;
        }

        @Override
        public long ramBytesUsed() {
            if (size == -1) {
                size = values.ramBytesUsed() + 2 * RamUsageEstimator.NUM_BYTES_LONG;
            }
            return size;
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values, minValue, missingValue);
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values, minValue, missingValue);
        }

        static class LongValues extends org.elasticsearch.index.fielddata.LongValues {

            private final PackedInts.Mutable values;
            private final long minValue;
            private final long missingValue;

            LongValues(PackedInts.Mutable values, long minValue, long missingValue) {
                super(false);
                this.values = values;
                this.minValue = minValue;
                this.missingValue = missingValue;
            }

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return values.get(docId) != missingValue ? 1 : 0;
            }

            @Override
            public long nextValue() {
                return minValue + values.get(docId);
            }
        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues {

            private final PackedInts.Mutable values;
            private final long minValue;
            private final long missingValue;

            DoubleValues(PackedInts.Mutable values, long minValue, long missingValue) {
                super(false);
                this.values = values;
                this.minValue = minValue;
                this.missingValue = missingValue;
            }

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return values.get(docId) != missingValue ? 1 : 0;
            }

            @Override
            public double nextValue() {
                return minValue + values.get(docId);
            }
        }
    }

    /**
     * Assumes all the values are "set", and docId is used as the index to the value array.
     */
    public static class Single extends PackedArrayAtomicFieldData {

        private final PackedInts.Mutable values;
        private final long minValue;

        /**
         * Note, here, we assume that there is no offset by 1 from docId, so position 0
         * is the value for docId 0.
         */
        public Single(PackedInts.Mutable values, long minValue) {
            super();
            this.values = values;
            this.minValue = minValue;
        }

        @Override
        public long ramBytesUsed() {
            if (size == -1) {
                size = values.ramBytesUsed();
            }
            return size;
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values, minValue);
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values, minValue);
        }

        static class LongValues extends DenseLongValues {

            private final PackedInts.Mutable values;
            private final long minValue;

            LongValues(PackedInts.Mutable values, long minValue) {
                super(false);
                this.values = values;
                this.minValue = minValue;
            }


            @Override
            public long nextValue() {
                return minValue + values.get(docId);
            }


        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues {

            private final PackedInts.Mutable values;
            private final long minValue;

            DoubleValues(PackedInts.Mutable values, long minValue) {
                super(false);
                this.values = values;
                this.minValue = minValue;
            }

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return 1;
            }

            @Override
            public double nextValue() {
                return minValue + values.get(docId);
            }

        }
    }

    /**
     * A single valued case, where all values are "set" and are stored in a paged wise manner for better compression.
     */
    public static class PagedSingle extends PackedArrayAtomicFieldData {

        private final AppendingDeltaPackedLongBuffer values;

        /**
         * Note, here, we assume that there is no offset by 1 from docId, so position 0
         * is the value for docId 0.
         */
        public PagedSingle(AppendingDeltaPackedLongBuffer values) {
            super();
            this.values = values;
        }

        @Override
        public long ramBytesUsed() {
            if (size == -1) {
                size = values.ramBytesUsed();
            }
            return size;
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values);
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values);
        }

        static class LongValues extends DenseLongValues {

            private final AppendingDeltaPackedLongBuffer values;

            LongValues(AppendingDeltaPackedLongBuffer values) {
                super(false);
                this.values = values;
            }


            @Override
            public long nextValue() {
                return values.get(docId);
            }


        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues {

            private final AppendingDeltaPackedLongBuffer values;

            DoubleValues(AppendingDeltaPackedLongBuffer values) {
                super(false);
                this.values = values;
            }

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return 1;
            }

            @Override
            public double nextValue() {
                return values.get(docId);
            }

        }

    }

    /**
     * A single valued case, where not all values are "set", so we have a special
     * value which encodes the fact that the document has no value. The data is stored in
     * a paged wise manner for better compression.
     */
    public static class PagedSingleSparse extends PackedArrayAtomicFieldData {

        private final AppendingDeltaPackedLongBuffer values;
        private final FixedBitSet docsWithValue;

        public PagedSingleSparse(AppendingDeltaPackedLongBuffer values, FixedBitSet docsWithValue) {
            super();
            this.values = values;
            this.docsWithValue = docsWithValue;
        }

        @Override
        public long ramBytesUsed() {
            if (size == -1) {
                size = values.ramBytesUsed() + 2 * RamUsageEstimator.NUM_BYTES_LONG;
            }
            return size;
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values, docsWithValue);
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values, docsWithValue);
        }

        static class LongValues extends org.elasticsearch.index.fielddata.LongValues {

            private final AppendingDeltaPackedLongBuffer values;
            private final FixedBitSet docsWithValue;

            LongValues(AppendingDeltaPackedLongBuffer values, FixedBitSet docsWithValue) {
                super(false);
                this.values = values;
                this.docsWithValue = docsWithValue;
            }

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return docsWithValue.get(docId) ? 1 : 0;
            }

            @Override
            public long nextValue() {
                return values.get(docId);
            }
        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues {

            private final AppendingDeltaPackedLongBuffer values;
            private final FixedBitSet docsWithValue;

            DoubleValues(AppendingDeltaPackedLongBuffer values, FixedBitSet docsWithValue) {
                super(false);
                this.values = values;
                this.docsWithValue = docsWithValue;
            }

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return docsWithValue.get(docId) ? 1 : 0;
            }

            @Override
            public double nextValue() {
                return values.get(docId);
            }
        }
    }

}
