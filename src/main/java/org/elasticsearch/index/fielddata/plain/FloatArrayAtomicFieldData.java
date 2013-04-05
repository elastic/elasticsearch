/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public abstract class FloatArrayAtomicFieldData extends AtomicNumericFieldData {

    public static final FloatArrayAtomicFieldData EMPTY = new Empty();

    protected final float[] values;
    private final int numDocs;

    protected long size = -1;

    public FloatArrayAtomicFieldData(float[] values, int numDocs) {
        super(true);
        this.values = values;
        this.numDocs = numDocs;
    }

    @Override
    public void close() {
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    static class Empty extends FloatArrayAtomicFieldData {

        Empty() {
            super(null, 0);
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
        public BytesValues getBytesValues() {
            return BytesValues.EMPTY;
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return ScriptDocValues.EMPTY;
        }
    }

    public static class WithOrdinals extends FloatArrayAtomicFieldData {

        private final Ordinals ordinals;

        public WithOrdinals(float[] values, int numDocs, Ordinals ordinals) {
            super(values, numDocs);
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
                size = RamUsage.NUM_BYTES_INT/*size*/ + RamUsage.NUM_BYTES_INT/*numDocs*/ + RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length * RamUsage.NUM_BYTES_FLOAT) + ordinals.getMemorySizeInBytes();
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

            private final float[] values;

            LongValues(float[] values, Ordinals.Docs ordinals) {
                super(ordinals);
                this.values = values;
            }

            @Override
            public long getValueByOrd(int ord) {
                return (long) values[ord];
            }
        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues.WithOrdinals {

            private final float[] values;

            DoubleValues(float[] values, Ordinals.Docs ordinals) {
                super(ordinals);
                this.values = values;
            }

            @Override
            public double getValueByOrd(int ord) {
                return values[ord];
            }
        }
    }

    /**
     * A single valued case, where not all values are "set", so we have a FixedBitSet that
     * indicates which values have an actual value.
     */
    public static class SingleFixedSet extends FloatArrayAtomicFieldData {

        private final FixedBitSet set;

        public SingleFixedSet(float[] values, int numDocs, FixedBitSet set) {
            super(values, numDocs);
            this.set = set;
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
            if (size == -1) {
                size = RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length * RamUsage.NUM_BYTES_FLOAT) + (set.getBits().length * RamUsage.NUM_BYTES_LONG);
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

            private final float[] values;
            private final FixedBitSet set;

            LongValues(float[] values, FixedBitSet set) {
                super(false);
                this.values = values;
                this.set = set;
            }

            @Override
            public boolean hasValue(int docId) {
                return set.get(docId);
            }

            @Override
            public long getValue(int docId) {
                return (long) values[docId];
            }
        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues {

            private final float[] values;
            private final FixedBitSet set;

            DoubleValues(float[] values, FixedBitSet set) {
                super(false);
                this.values = values;
                this.set = set;
            }

            @Override
            public boolean hasValue(int docId) {
                return set.get(docId);
            }

            @Override
            public double getValue(int docId) {
                return (double) values[docId];
            }

        }

    }

    /**
     * Assumes all the values are "set", and docId is used as the index to the value array.
     */
    public static class Single extends FloatArrayAtomicFieldData {

        /**
         * Note, here, we assume that there is no offset by 1 from docId, so position 0
         * is the value for docId 0.
         */
        public Single(float[] values, int numDocs) {
            super(values, numDocs);
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
            if (size == -1) {
                size = RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length * RamUsage.NUM_BYTES_FLOAT);
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


        static class LongValues extends org.elasticsearch.index.fielddata.LongValues.Dense {

            private final float[] values;

            LongValues(float[] values) {
                super(false);
                this.values = values;
            }

            @Override
            public long getValue(int docId) {
                return (long) values[docId];
            }

        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues.Dense {

            private final float[] values;

            DoubleValues(float[] values) {
                super(false);
                this.values = values;
            }

            @Override
            public double getValue(int docId) {
                return (double) values[docId];
            }
        }
    }
}
