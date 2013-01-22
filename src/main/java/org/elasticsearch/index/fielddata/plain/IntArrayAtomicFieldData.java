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
import org.elasticsearch.index.fielddata.util.DoubleArrayRef;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.elasticsearch.index.fielddata.util.LongArrayRef;

/**
 */
public abstract class IntArrayAtomicFieldData implements AtomicNumericFieldData {

    public static final IntArrayAtomicFieldData EMPTY = new Empty();

    protected final int[] values;
    private final int numDocs;

    protected long size = -1;

    public IntArrayAtomicFieldData(int[] values, int numDocs) {
        this.values = values;
        this.numDocs = numDocs;
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    static class Empty extends IntArrayAtomicFieldData {

        Empty() {
            super(null, 0);
        }

        @Override
        public ByteValues getByteValues() {
            return ByteValues.EMPTY;
        }

        @Override
        public ShortValues getShortValues() {
            return ShortValues.EMPTY;
        }

        @Override
        public IntValues getIntValues() {
            return IntValues.EMPTY;
        }

        @Override
        public LongValues getLongValues() {
            return LongValues.EMPTY;
        }

        @Override
        public FloatValues getFloatValues() {
            return FloatValues.EMPTY;
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
        public HashedBytesValues getHashedBytesValues() {
            return HashedBytesValues.EMPTY;
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

    public static class WithOrdinals extends IntArrayAtomicFieldData {

        private final Ordinals ordinals;

        public WithOrdinals(int[] values, int numDocs, Ordinals ordinals) {
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
                size = RamUsage.NUM_BYTES_INT/*size*/ + RamUsage.NUM_BYTES_INT/*numDocs*/ + +RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length * RamUsage.NUM_BYTES_INT) + ordinals.getMemorySizeInBytes();
            }
            return size;
        }

        @Override
        public BytesValues getBytesValues() {
            return new BytesValues.StringBased(getStringValues());
        }

        @Override
        public HashedBytesValues getHashedBytesValues() {
            return new HashedBytesValues.StringBased(getStringValues());
        }

        @Override
        public StringValues getStringValues() {
            return new StringValues.IntBased(getIntValues());
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return new ScriptDocValues.NumericInteger(getIntValues());
        }

        @Override
        public ByteValues getByteValues() {
            return new ByteValues.LongBased(getLongValues());
        }

        @Override
        public ShortValues getShortValues() {
            return new ShortValues.LongBased(getLongValues());
        }

        @Override
        public IntValues getIntValues() {
            return new IntValues(values, ordinals.ordinals());
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values, ordinals.ordinals());
        }

        @Override
        public FloatValues getFloatValues() {
            return new FloatValues.DoubleBased(getDoubleValues());
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values, ordinals.ordinals());
        }

        static class IntValues implements org.elasticsearch.index.fielddata.IntValues {

            private final int[] values;
            private final Ordinals.Docs ordinals;

            private final IntArrayRef arrayScratch = new IntArrayRef(new int[1], 1);
            private final ValuesIter iter;

            IntValues(int[] values, Ordinals.Docs ordinals) {
                this.values = values;
                this.ordinals = ordinals;
                this.iter = new ValuesIter(values);
            }

            @Override
            public boolean isMultiValued() {
                return ordinals.isMultiValued();
            }

            @Override
            public boolean hasValue(int docId) {
                return ordinals.getOrd(docId) != 0;
            }

            @Override
            public int getValue(int docId) {
                return values[ordinals.getOrd(docId)];
            }

            @Override
            public int getValueMissing(int docId, int missingValue) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    return missingValue;
                } else {
                    return values[ord];
                }
            }

            @Override
            public IntArrayRef getValues(int docId) {
                IntArrayRef ords = ordinals.getOrds(docId);
                int size = ords.size();
                if (size == 0) return IntArrayRef.EMPTY;

                arrayScratch.reset(size);
                for (int i = ords.start; i < ords.end; i++) {
                    arrayScratch.values[arrayScratch.end++] = values[ords.values[i]];
                }
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                return iter.reset(ordinals.getIter(docId));
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
                    proc.onValue(docId, values[ord]);
                } while ((ord = iter.next()) != 0);
            }

            static class ValuesIter implements Iter {

                private final int[] values;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(int[] values) {
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
                public int next() {
                    int value = values[ord];
                    ord = ordsIter.next();
                    return value;
                }
            }
        }

        static class LongValues implements org.elasticsearch.index.fielddata.LongValues {

            private final int[] values;
            private final Ordinals.Docs ordinals;

            private final LongArrayRef arrayScratch = new LongArrayRef(new long[1], 1);
            private final ValuesIter iter;

            LongValues(int[] values, Ordinals.Docs ordinals) {
                this.values = values;
                this.ordinals = ordinals;
                this.iter = new ValuesIter(values);
            }

            @Override
            public boolean isMultiValued() {
                return ordinals.isMultiValued();
            }

            @Override
            public boolean hasValue(int docId) {
                return ordinals.getOrd(docId) != 0;
            }

            @Override
            public long getValue(int docId) {
                return (long) values[ordinals.getOrd(docId)];
            }

            @Override
            public long getValueMissing(int docId, long missingValue) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    return missingValue;
                } else {
                    return (long) values[ord];
                }
            }

            @Override
            public LongArrayRef getValues(int docId) {
                IntArrayRef ords = ordinals.getOrds(docId);
                int size = ords.size();
                if (size == 0) return LongArrayRef.EMPTY;

                arrayScratch.reset(size);
                for (int i = ords.start; i < ords.end; i++) {
                    arrayScratch.values[arrayScratch.end++] = (long) values[ords.values[i]];
                }
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                return iter.reset(ordinals.getIter(docId));
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
                    proc.onValue(docId, (long) values[ord]);
                } while ((ord = iter.next()) != 0);
            }

            static class ValuesIter implements Iter {

                private final int[] values;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(int[] values) {
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
                public long next() {
                    int value = values[ord];
                    ord = ordsIter.next();
                    return (long) value;
                }
            }
        }

        static class DoubleValues implements org.elasticsearch.index.fielddata.DoubleValues {

            private final int[] values;
            private final Ordinals.Docs ordinals;

            private final DoubleArrayRef arrayScratch = new DoubleArrayRef(new double[1], 1);
            private final ValuesIter iter;

            DoubleValues(int[] values, Ordinals.Docs ordinals) {
                this.values = values;
                this.ordinals = ordinals;
                this.iter = new ValuesIter(values);
            }

            @Override
            public boolean isMultiValued() {
                return ordinals.isMultiValued();
            }

            @Override
            public boolean hasValue(int docId) {
                return ordinals.getOrd(docId) != 0;
            }

            @Override
            public double getValue(int docId) {
                return (double) values[ordinals.getOrd(docId)];
            }

            @Override
            public double getValueMissing(int docId, double missingValue) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    return missingValue;
                } else {
                    return (double) values[ord];
                }
            }

            @Override
            public DoubleArrayRef getValues(int docId) {
                IntArrayRef ords = ordinals.getOrds(docId);
                int size = ords.size();
                if (size == 0) return DoubleArrayRef.EMPTY;

                arrayScratch.reset(size);
                for (int i = ords.start; i < ords.end; i++) {
                    arrayScratch.values[arrayScratch.end++] = (double) values[ords.values[i]];
                }
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                return iter.reset(ordinals.getIter(docId));
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
                    proc.onValue(docId, (double) values[ord]);
                } while ((ord = iter.next()) != 0);
            }

            static class ValuesIter implements Iter {

                private final int[] values;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(int[] values) {
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
                public double next() {
                    int value = values[ord];
                    ord = ordsIter.next();
                    return (double) value;
                }
            }
        }
    }

    /**
     * A single valued case, where not all values are "set", so we have a FixedBitSet that
     * indicates which values have an actual value.
     */
    public static class SingleFixedSet extends IntArrayAtomicFieldData {

        private final FixedBitSet set;

        public SingleFixedSet(int[] values, int numDocs, FixedBitSet set) {
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
            return new ScriptDocValues.NumericInteger(getIntValues());
        }

        @Override
        public BytesValues getBytesValues() {
            return new BytesValues.StringBased(getStringValues());
        }

        @Override
        public HashedBytesValues getHashedBytesValues() {
            return new HashedBytesValues.StringBased(getStringValues());
        }

        @Override
        public StringValues getStringValues() {
            return new StringValues.LongBased(getLongValues());
        }

        @Override
        public ByteValues getByteValues() {
            return new ByteValues.LongBased(getLongValues());
        }

        @Override
        public ShortValues getShortValues() {
            return new ShortValues.LongBased(getLongValues());
        }

        @Override
        public IntValues getIntValues() {
            return new IntValues(values, set);
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values, set);
        }

        @Override
        public FloatValues getFloatValues() {
            return new FloatValues.DoubleBased(getDoubleValues());
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values, set);
        }

        static class IntValues implements org.elasticsearch.index.fielddata.IntValues {

            private final int[] values;
            private final FixedBitSet set;

            private final IntArrayRef arrayScratch = new IntArrayRef(new int[1], 1);
            private final Iter.Single iter = new Iter.Single();

            IntValues(int[] values, FixedBitSet set) {
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
            public int getValue(int docId) {
                return values[docId];
            }

            @Override
            public int getValueMissing(int docId, int missingValue) {
                if (set.get(docId)) {
                    return values[docId];
                } else {
                    return missingValue;
                }
            }

            @Override
            public IntArrayRef getValues(int docId) {
                if (set.get(docId)) {
                    arrayScratch.values[0] = values[docId];
                    return arrayScratch;
                } else {
                    return IntArrayRef.EMPTY;
                }
            }

            @Override
            public Iter getIter(int docId) {
                if (set.get(docId)) {
                    return iter.reset(values[docId]);
                } else {
                    return Iter.Empty.INSTANCE;
                }
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                if (set.get(docId)) {
                    proc.onValue(docId, values[docId]);
                }
            }
        }

        static class LongValues implements org.elasticsearch.index.fielddata.LongValues {

            private final int[] values;
            private final FixedBitSet set;

            private final LongArrayRef arrayScratch = new LongArrayRef(new long[1], 1);
            private final Iter.Single iter = new Iter.Single();

            LongValues(int[] values, FixedBitSet set) {
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
            public long getValue(int docId) {
                return (long) values[docId];
            }

            @Override
            public long getValueMissing(int docId, long missingValue) {
                if (set.get(docId)) {
                    return (long) values[docId];
                } else {
                    return missingValue;
                }
            }

            @Override
            public LongArrayRef getValues(int docId) {
                if (set.get(docId)) {
                    arrayScratch.values[0] = (long) values[docId];
                    return arrayScratch;
                } else {
                    return LongArrayRef.EMPTY;
                }
            }

            @Override
            public Iter getIter(int docId) {
                if (set.get(docId)) {
                    return iter.reset((long) values[docId]);
                } else {
                    return Iter.Empty.INSTANCE;
                }
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                if (set.get(docId)) {
                    proc.onValue(docId, (long) values[docId]);
                }
            }
        }

        static class DoubleValues implements org.elasticsearch.index.fielddata.DoubleValues {

            private final int[] values;
            private final FixedBitSet set;

            private final DoubleArrayRef arrayScratch = new DoubleArrayRef(new double[1], 1);
            private final Iter.Single iter = new Iter.Single();

            DoubleValues(int[] values, FixedBitSet set) {
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
            public double getValue(int docId) {
                return (double) values[docId];
            }

            @Override
            public double getValueMissing(int docId, double missingValue) {
                if (set.get(docId)) {
                    return (double) values[docId];
                } else {
                    return missingValue;
                }
            }

            @Override
            public DoubleArrayRef getValues(int docId) {
                if (set.get(docId)) {
                    arrayScratch.values[0] = (double) values[docId];
                    return arrayScratch;
                } else {
                    return DoubleArrayRef.EMPTY;
                }
            }

            @Override
            public Iter getIter(int docId) {
                if (set.get(docId)) {
                    return iter.reset((double) values[docId]);
                } else {
                    return Iter.Empty.INSTANCE;
                }
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                if (set.get(docId)) {
                    proc.onValue(docId, (double) values[docId]);
                }
            }
        }
    }

    /**
     * Assumes all the values are "set", and docId is used as the index to the value array.
     */
    public static class Single extends IntArrayAtomicFieldData {

        /**
         * Note, here, we assume that there is no offset by 1 from docId, so position 0
         * is the value for docId 0.
         */
        public Single(int[] values, int numDocs) {
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
            return new ScriptDocValues.NumericInteger(getIntValues());
        }

        @Override
        public BytesValues getBytesValues() {
            return new BytesValues.StringBased(getStringValues());
        }

        @Override
        public HashedBytesValues getHashedBytesValues() {
            return new HashedBytesValues.StringBased(getStringValues());
        }

        @Override
        public StringValues getStringValues() {
            return new StringValues.LongBased(getLongValues());
        }

        @Override
        public ByteValues getByteValues() {
            return new ByteValues.LongBased(getLongValues());
        }

        @Override
        public ShortValues getShortValues() {
            return new ShortValues.LongBased(getLongValues());
        }

        @Override
        public IntValues getIntValues() {
            return new IntValues(values);
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values);
        }

        @Override
        public FloatValues getFloatValues() {
            return new FloatValues.DoubleBased(getDoubleValues());
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values);
        }

        static class IntValues implements org.elasticsearch.index.fielddata.IntValues {

            private final int[] values;

            private final IntArrayRef arrayScratch = new IntArrayRef(new int[1], 1);
            private final Iter.Single iter = new Iter.Single();

            IntValues(int[] values) {
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
            public int getValue(int docId) {
                return values[docId];
            }

            @Override
            public int getValueMissing(int docId, int missingValue) {
                return values[docId];
            }

            @Override
            public IntArrayRef getValues(int docId) {
                arrayScratch.values[0] = values[docId];
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                return iter.reset(values[docId]);
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                proc.onValue(docId, values[docId]);
            }
        }

        static class LongValues implements org.elasticsearch.index.fielddata.LongValues {

            private final int[] values;

            private final LongArrayRef arrayScratch = new LongArrayRef(new long[1], 1);
            private final Iter.Single iter = new Iter.Single();

            LongValues(int[] values) {
                assert values.length != 0;
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
            public long getValue(int docId) {
                return (long) values[docId];
            }

            @Override
            public long getValueMissing(int docId, long missingValue) {
                return (long) values[docId];
            }

            @Override
            public LongArrayRef getValues(int docId) {
                arrayScratch.values[0] = (long) values[docId];
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                return iter.reset((long) values[docId]);
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                proc.onValue(docId, (long) values[docId]);
            }
        }

        static class DoubleValues implements org.elasticsearch.index.fielddata.DoubleValues {

            private final int[] values;

            private final DoubleArrayRef arrayScratch = new DoubleArrayRef(new double[1], 1);
            private final Iter.Single iter = new Iter.Single();

            DoubleValues(int[] values) {
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
            public double getValue(int docId) {
                return values[docId];
            }

            @Override
            public double getValueMissing(int docId, double missingValue) {
                return (double) values[docId];
            }

            @Override
            public DoubleArrayRef getValues(int docId) {
                arrayScratch.values[0] = (double) values[docId];
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                return iter.reset((double) values[docId]);
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                proc.onValue(docId, (double) values[docId]);
            }
        }

    }
}
