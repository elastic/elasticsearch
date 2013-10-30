/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 * {@link AtomicNumericFieldData} implementation which stores data in packed arrays to save memory.
 */
public abstract class PackedArrayAtomicFieldData extends AbstractAtomicNumericFieldData {

    public static PackedArrayAtomicFieldData empty(int numDocs) {
        return new Empty(numDocs);
    }

    private final int numDocs;

    protected long size = -1;

    public PackedArrayAtomicFieldData(int numDocs) {
        super(false);
        this.numDocs = numDocs;
    }

    @Override
    public void close() {
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    static class Empty extends PackedArrayAtomicFieldData {

        Empty(int numDocs) {
            super(numDocs);
        }

        @Override
        public LongValues getLongValues() {
            return LongValues.EMPTY;
        }

        @Override
        public DoubleValues getDoubleValues() {
            return DoubleValues.EMPTY;
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean isValuesOrdered() {
            return false;
        }

        @Override
        public long getMemorySizeInBytes() {
            return 0;
        }

        @Override
        public long getNumberUniqueValues() {
            return 0;
        }

        @Override
        public BytesValues getBytesValues(boolean needsHashes) {
            return BytesValues.EMPTY;
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return ScriptDocValues.EMPTY;
        }
    }

    public static class WithOrdinals extends PackedArrayAtomicFieldData {

        private final MonotonicAppendingLongBuffer values;
        private final Ordinals ordinals;

        public WithOrdinals(MonotonicAppendingLongBuffer values, int numDocs, Ordinals ordinals) {
            super(numDocs);
            this.values = values;
            this.ordinals = ordinals;
        }

        @Override
        public boolean isMultiValued() {
            return ordinals.isMultiValued();
        }

        @Override
        public boolean isValuesOrdered() {
            return true;
        }

        @Override
        public long getMemorySizeInBytes() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + RamUsageEstimator.NUM_BYTES_INT/*numDocs*/ + values.ramBytesUsed() + ordinals.getMemorySizeInBytes();
            }
            return size;
        }

        @Override
        public long getNumberUniqueValues() {
            return ordinals.getNumOrds();
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

            LongValues(MonotonicAppendingLongBuffer values, Ordinals.Docs ordinals) {
                super(ordinals);
                this.values = values;
            }

            @Override
            public long getValueByOrd(long ord) {
                assert ord != Ordinals.MISSING_ORDINAL;
                return values.get(ord - 1);
            }
        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues.WithOrdinals {

            private final MonotonicAppendingLongBuffer values;

            DoubleValues(MonotonicAppendingLongBuffer values, Ordinals.Docs ordinals) {
                super(ordinals);
                this.values = values;
            }

            @Override
            public double getValueByOrd(long ord) {
                assert ord != Ordinals.MISSING_ORDINAL;
                return values.get(ord - 1);
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
        private final long numOrds;

        public SingleSparse(PackedInts.Mutable values, long minValue, int numDocs, long missingValue, long numOrds) {
            super(numDocs);
            this.values = values;
            this.minValue = minValue;
            this.missingValue = missingValue;
            this.numOrds = numOrds;
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean isValuesOrdered() {
            return false;
        }

        @Override
        public long getNumberUniqueValues() {
            return numOrds;
        }

        @Override
        public long getMemorySizeInBytes() {
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
                return  minValue + values.get(docId);
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
                return  minValue + values.get(docId);
            }
        }
    }

    /**
     * Assumes all the values are "set", and docId is used as the index to the value array.
     */
    public static class Single extends PackedArrayAtomicFieldData {

        private final PackedInts.Mutable values;
        private final long minValue;
        private final long numOrds;

        /**
         * Note, here, we assume that there is no offset by 1 from docId, so position 0
         * is the value for docId 0.
         */
        public Single(PackedInts.Mutable values, long minValue, int numDocs, long numOrds) {
            super(numDocs);
            this.values = values;
            this.minValue = minValue;
            this.numOrds = numOrds;
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean isValuesOrdered() {
            return false;
        }

        @Override
        public long getNumberUniqueValues() {
            return numOrds;
        }

        @Override
        public long getMemorySizeInBytes() {
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
}
