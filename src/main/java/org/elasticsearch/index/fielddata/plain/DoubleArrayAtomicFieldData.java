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

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BigDoubleArrayList;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public abstract class DoubleArrayAtomicFieldData extends AbstractAtomicNumericFieldData {

    public static DoubleArrayAtomicFieldData empty(int numDocs) {
        return new Empty(numDocs);
    }

    private final int numDocs;

    protected long size = -1;

    public DoubleArrayAtomicFieldData(int numDocs) {
        super(true);
        this.numDocs = numDocs;
    }

    @Override
    public void close() {
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    static class Empty extends DoubleArrayAtomicFieldData {

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
        public long getNumberUniqueValues() {
            return 0;
        }

        @Override
        public long getMemorySizeInBytes() {
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

    public static class WithOrdinals extends DoubleArrayAtomicFieldData {

        private final BigDoubleArrayList values;
        private final Ordinals ordinals;

        public WithOrdinals(BigDoubleArrayList values, int numDocs, Ordinals ordinals) {
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
        public long getNumberUniqueValues() {
            return ordinals.getNumOrds();
        }

        @Override
        public long getMemorySizeInBytes() {
            if (size == -1) {
                size = RamUsageEstimator.NUM_BYTES_INT/*size*/ + RamUsageEstimator.NUM_BYTES_INT/*numDocs*/ + values.sizeInBytes() + ordinals.getMemorySizeInBytes();
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

            private final BigDoubleArrayList values;

            LongValues(BigDoubleArrayList values, Ordinals.Docs ordinals) {
                super(ordinals);
                this.values = values;
            }

            @Override
            public final long getValueByOrd(long ord) {
                assert ord != Ordinals.MISSING_ORDINAL;
                return (long) values.get(ord);
            }
        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues.WithOrdinals {

            private final BigDoubleArrayList values;

            DoubleValues(BigDoubleArrayList values, Ordinals.Docs ordinals) {
                super(ordinals);
                this.values = values;
            }

            @Override
            public double getValueByOrd(long ord) {
                assert ord != Ordinals.MISSING_ORDINAL;
                return values.get(ord);
            }
        }
    }

    /**
     * A single valued case, where not all values are "set", so we have a FixedBitSet that
     * indicates which values have an actual value.
     */
    public static class SingleFixedSet extends DoubleArrayAtomicFieldData {

        private final BigDoubleArrayList values;
        private final FixedBitSet set;
        private final long numOrds;

        public SingleFixedSet(BigDoubleArrayList values, int numDocs, FixedBitSet set, long numOrds) {
            super(numDocs);
            this.values = values;
            this.set = set;
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
                size = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + values.sizeInBytes() + RamUsageEstimator.sizeOf(set.getBits());
            }
            return size;
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values, set);
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values, set);
        }

        static class LongValues extends org.elasticsearch.index.fielddata.LongValues {

            private final BigDoubleArrayList values;
            private final FixedBitSet set;

            LongValues(BigDoubleArrayList values, FixedBitSet set) {
                super(false);
                this.values = values;
                this.set = set;
            }

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return  set.get(docId) ? 1 : 0;
            }

            @Override
            public long nextValue() {
                return (long) values.get(docId);
            }
            }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues {

            private final BigDoubleArrayList values;
            private final FixedBitSet set;

            DoubleValues(BigDoubleArrayList values, FixedBitSet set) {
                super(false);
                this.values = values;
                this.set = set;
            }

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return set.get(docId) ? 1 : 0;
            }

            @Override
            public double nextValue() {
                return values.get(docId);
            }
        }
    }

    /**
     * Assumes all the values are "set", and docId is used as the index to the value array.
     */
    public static class Single extends DoubleArrayAtomicFieldData {

        private final BigDoubleArrayList values;
        private final long numOrds;

        /**
         * Note, here, we assume that there is no offset by 1 from docId, so position 0
         * is the value for docId 0.
         */
        public Single(BigDoubleArrayList values, int numDocs, long numOrds) {
            super(numDocs);
            this.values = values;
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
                size = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + values.sizeInBytes();
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

        static final class LongValues extends DenseLongValues {

            private final BigDoubleArrayList values;

            LongValues(BigDoubleArrayList values) {
                super(false);
                this.values = values;
            }

            @Override
            public long nextValue() {
                return (long) values.get(docId);
            }
        }

        static final class DoubleValues extends DenseDoubleValues {

            private final BigDoubleArrayList values;

            DoubleValues(BigDoubleArrayList values) {
                super(false);
                this.values = values;
            }

            @Override
            public double nextValue() {
                return values.get(docId);
            }
            
        }
    }
}
