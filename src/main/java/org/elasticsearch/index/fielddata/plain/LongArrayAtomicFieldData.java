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
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.StringValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public abstract class LongArrayAtomicFieldData extends AtomicNumericFieldData {

    public static final LongArrayAtomicFieldData EMPTY = new Empty();

    protected final long[] values;
    private final int numDocs;

    protected long size = -1;

    public LongArrayAtomicFieldData(long[] values, int numDocs) {
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

    static class Empty extends LongArrayAtomicFieldData {

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
        public StringValues getStringValues() {
            return StringValues.EMPTY;
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return ScriptDocValues.EMPTY;
        }
    }

    public static class WithOrdinals extends LongArrayAtomicFieldData {

        private final Ordinals ordinals;

        public WithOrdinals(long[] values, int numDocs, Ordinals ordinals) {
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
                size = RamUsage.NUM_BYTES_INT/*size*/ + RamUsage.NUM_BYTES_INT/*numDocs*/ + +RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length * RamUsage.NUM_BYTES_DOUBLE) + ordinals.getMemorySizeInBytes();
            }
            return size;
        }

        @Override
        public StringValues getStringValues() {
            return new StringValues(values, ordinals.ordinals());
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return new ScriptDocValues.NumericLong(getLongValues());
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values, ordinals.ordinals());
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values, ordinals.ordinals());
        }

        static class StringValues implements org.elasticsearch.index.fielddata.StringValues {

            private final long[] values;
            private final Ordinals.Docs ordinals;
            private final ValuesIter valuesIter;

            StringValues(long[] values, Ordinals.Docs ordinals) {
                this.values = values;
                this.ordinals = ordinals;
                this.valuesIter = new ValuesIter(values);
            }

            @Override
            public boolean hasValue(int docId) {
                return ordinals.getOrd(docId) != 0;
            }

            @Override
            public boolean isMultiValued() {
                return ordinals.isMultiValued();
            }

            @Override
            public String getValue(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    return null;
                }
                return Long.toString(values[ord]);
            }

            @Override
            public Iter getIter(int docId) {
                return valuesIter.reset(ordinals.getIter(docId));
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                Ordinals.Docs.Iter iter = ordinals.getIter(docId);
                int ord = iter.next();
                if (ord == 0) {
                    proc.onMissing(docId);
                    return;
                }
                do {
                    proc.onValue(docId, Long.toString(values[ord]));
                } while ((ord = iter.next()) != 0);
            }

            static class ValuesIter implements Iter {

                private final long[] values;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(long[] values) {
                    this.values = values;
                }

                public ValuesIter reset(Ordinals.Docs.Iter ordsIter) {
                    this.ordsIter = ordsIter;
                    this.ord = ordsIter.next();
                    return this;
                }

                @Override
                public boolean hasNext() {
                    return ord != 0;
                }

                @Override
                public String next() {
                    String value = Long.toString(values[ord]);
                    ord = ordsIter.next();
                    return value;
                }
            }
        }

        static class LongValues extends org.elasticsearch.index.fielddata.LongValues.OrdBasedLongValues {

            private final long[] values;

            LongValues(long[] values, Ordinals.Docs ordinals) {
                super(ordinals);
                this.values = values;
            }
           
            @Override
            public long getByOrd(int ord) {
                return values[ord];
            }
        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues.OrdBasedDoubleValues {

            private final long[] values;

            DoubleValues(long[] values, Ordinals.Docs ordinals) {
                super(ordinals);
                this.values = values;
            }

            @Override
            protected double getByOrd(int ord) {
                return values[ord];
            }

           
        }
    }

    /**
     * A single valued case, where not all values are "set", so we have a FixedBitSet that
     * indicates which values have an actual value.
     */
    public static class SingleFixedSet extends LongArrayAtomicFieldData {

        private final FixedBitSet set;

        public SingleFixedSet(long[] values, int numDocs, FixedBitSet set) {
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
                size = RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length * RamUsage.NUM_BYTES_DOUBLE) + (set.getBits().length * RamUsage.NUM_BYTES_LONG);
            }
            return size;
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return new ScriptDocValues.NumericLong(getLongValues());
        }

        @Override
        public BytesValues getBytesValues() {
            return new BytesValues.StringBased(getStringValues());
        }

        @Override
        public StringValues getStringValues() {
            return new StringValues(values, set);
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values, set);
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values, set);
        }

        static class StringValues implements org.elasticsearch.index.fielddata.StringValues {

            private final long[] values;
            private final FixedBitSet set;
            private final Iter.Single iter = new Iter.Single();

            StringValues(long[] values, FixedBitSet set) {
                this.values = values;
                this.set = set;
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public boolean hasValue(int docId) {
                return set.get(docId);
            }

            @Override
            public String getValue(int docId) {
                if (set.get(docId)) {
                    return Long.toString(values[docId]);
                } else {
                    return null;
                }
            }

            @Override
            public Iter getIter(int docId) {
                if (set.get(docId)) {
                    return iter.reset(Long.toString(values[docId]));
                } else {
                    return Iter.Empty.INSTANCE;
                }
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                if (set.get(docId)) {
                    proc.onValue(docId, Long.toString(values[docId]));
                } else {
                    proc.onMissing(docId);
                }
            }
        }

        static class LongValues extends org.elasticsearch.index.fielddata.LongValues {

            private final long[] values;
            private final FixedBitSet set;

            LongValues(long[] values, FixedBitSet set) {
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
                return values[docId];
            }
        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues {

            private final long[] values;
            private final FixedBitSet set;

            DoubleValues(long[] values, FixedBitSet set) {
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
    public static class Single extends LongArrayAtomicFieldData {

        /**
         * Note, here, we assume that there is no offset by 1 from docId, so position 0
         * is the value for docId 0.
         */
        public Single(long[] values, int numDocs) {
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
                size = RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length * RamUsage.NUM_BYTES_DOUBLE);
            }
            return size;
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return new ScriptDocValues.NumericLong(getLongValues());
        }

        @Override
        public BytesValues getBytesValues() {
            return new BytesValues.StringBased(getStringValues());
        }

        @Override
        public StringValues getStringValues() {
            return new StringValues(values);
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values);
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values);
        }

        static class StringValues implements org.elasticsearch.index.fielddata.StringValues {

            private final long[] values;
            private final Iter.Single iter = new Iter.Single();

            StringValues(long[] values) {
                this.values = values;
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public boolean hasValue(int docId) {
                return true;
            }

            @Override
            public String getValue(int docId) {
                return Long.toString(values[docId]);
            }

            @Override
            public Iter getIter(int docId) {
                return iter.reset(Long.toString(values[docId]));
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                proc.onValue(docId, Long.toString(values[docId]));
            }
        }

        static class LongValues extends org.elasticsearch.index.fielddata.LongValues.DenseLongValues {

            private final long[] values;

            LongValues(long[] values) {
                super(false);
                this.values = values;
            }

            @Override
            public long getValue(int docId) {
                return values[docId];
            }

        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues.DenseDoubleValues {

            private final long[] values;

            DoubleValues(long[] values) {
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
