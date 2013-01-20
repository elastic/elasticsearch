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
import org.elasticsearch.index.fielddata.util.ByteArrayRef;
import org.elasticsearch.index.fielddata.util.DoubleArrayRef;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.elasticsearch.index.fielddata.util.LongArrayRef;

/**
 */
public abstract class ByteArrayAtomicFieldData implements AtomicNumericFieldData {

    protected final byte[] values;
    private final int numDocs;

    protected long size = -1;

    public ByteArrayAtomicFieldData(byte[] values, int numDocs) {
        this.values = values;
        this.numDocs = numDocs;
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    public static class WithOrdinals extends ByteArrayAtomicFieldData {

        private final Ordinals ordinals;

        public WithOrdinals(byte[] values, int numDocs, Ordinals ordinals) {
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
                size = RamUsage.NUM_BYTES_INT/*size*/ + RamUsage.NUM_BYTES_INT/*numDocs*/ + RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length) + ordinals.getMemorySizeInBytes();
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
            return new StringValues.ByteBased(getByteValues());
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return new ScriptDocValues.NumericDouble(getDoubleValues());
        }

        @Override
        public ByteValues getByteValues() {
            return new ByteValues(values, ordinals.ordinals());
        }

        @Override
        public ShortValues getShortValues() {
            return new ShortValues.LongBased(getLongValues());
        }

        @Override
        public IntValues getIntValues() {
            return new IntValues.LongBased(getLongValues());
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

        static class ByteValues implements org.elasticsearch.index.fielddata.ByteValues {

            private final byte[] values;
            private final Ordinals.Docs ordinals;

            private final ByteArrayRef arrayScratch = new ByteArrayRef(new byte[1], 1);
            private final ValuesIter iter;

            ByteValues(byte[] values, Ordinals.Docs ordinals) {
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
            public byte getValue(int docId) {
                return values[ordinals.getOrd(docId)];
            }

            @Override
            public byte getValueMissing(int docId, byte missingValue) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    return missingValue;
                } else {
                    return values[ord];
                }
            }

            @Override
            public ByteArrayRef getValues(int docId) {
                IntArrayRef ords = ordinals.getOrds(docId);
                int size = ords.size();
                if (size == 0) return ByteArrayRef.EMPTY;

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

                private final byte[] values;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(byte[] values) {
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
                public byte next() {
                    byte value = values[ord];
                    ord = ordsIter.next();
                    return value;
                }
            }
        }

        static class LongValues implements org.elasticsearch.index.fielddata.LongValues {

            private final byte[] values;
            private final Ordinals.Docs ordinals;

            private final LongArrayRef arrayScratch = new LongArrayRef(new long[1], 1);
            private final ValuesIter iter;

            LongValues(byte[] values, Ordinals.Docs ordinals) {
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

                private final byte[] values;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(byte[] values) {
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
                    byte value = values[ord];
                    ord = ordsIter.next();
                    return (long) value;
                }
            }
        }

        static class DoubleValues implements org.elasticsearch.index.fielddata.DoubleValues {

            private final byte[] values;
            private final Ordinals.Docs ordinals;

            private final DoubleArrayRef arrayScratch = new DoubleArrayRef(new double[1], 1);
            private final ValuesIter iter;

            DoubleValues(byte[] values, Ordinals.Docs ordinals) {
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

                private final byte[] values;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(byte[] values) {
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
                    byte value = values[ord];
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
    public static class SingleFixedSet extends ByteArrayAtomicFieldData {

        private final FixedBitSet set;

        public SingleFixedSet(byte[] values, int numDocs, FixedBitSet set) {
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
                size = RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length) + (set.getBits().length * RamUsage.NUM_BYTES_LONG);
            }
            return size;
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return new ScriptDocValues.NumericDouble(getDoubleValues());
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
            return new StringValues.ByteBased(getByteValues());
        }

        @Override
        public ByteValues getByteValues() {
            return new ByteValues(values, set);
        }

        @Override
        public ShortValues getShortValues() {
            return new ShortValues.LongBased(getLongValues());
        }

        @Override
        public IntValues getIntValues() {
            return new IntValues.LongBased(getLongValues());
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

        static class ByteValues implements org.elasticsearch.index.fielddata.ByteValues {

            private final byte[] values;
            private final FixedBitSet set;

            private final ByteArrayRef arrayScratch = new ByteArrayRef(new byte[1], 1);
            private final Iter.Single iter = new Iter.Single();

            ByteValues(byte[] values, FixedBitSet set) {
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
            public byte getValue(int docId) {
                return values[docId];
            }

            @Override
            public byte getValueMissing(int docId, byte missingValue) {
                if (set.get(docId)) {
                    return values[docId];
                } else {
                    return missingValue;
                }
            }

            @Override
            public ByteArrayRef getValues(int docId) {
                if (set.get(docId)) {
                    arrayScratch.values[0] = values[docId];
                    return arrayScratch;
                } else {
                    return ByteArrayRef.EMPTY;
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

            private final byte[] values;
            private final FixedBitSet set;

            private final LongArrayRef arrayScratch = new LongArrayRef(new long[1], 1);
            private final Iter.Single iter = new Iter.Single();

            LongValues(byte[] values, FixedBitSet set) {
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

            private final byte[] values;
            private final FixedBitSet set;

            private final DoubleArrayRef arrayScratch = new DoubleArrayRef(new double[1], 1);
            private final Iter.Single iter = new Iter.Single();

            DoubleValues(byte[] values, FixedBitSet set) {
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
    public static class Single extends ByteArrayAtomicFieldData {

        /**
         * Note, here, we assume that there is no offset by 1 from docId, so position 0
         * is the value for docId 0.
         */
        public Single(byte[] values, int numDocs) {
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
                size = RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length);
            }
            return size;
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return new ScriptDocValues.NumericDouble(getDoubleValues());
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
            return new StringValues.ByteBased(getByteValues());
        }

        @Override
        public ByteValues getByteValues() {
            return new ByteValues(values);
        }

        @Override
        public ShortValues getShortValues() {
            return new ShortValues.LongBased(getLongValues());
        }

        @Override
        public IntValues getIntValues() {
            return new IntValues.LongBased(getLongValues());
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

        static class ByteValues implements org.elasticsearch.index.fielddata.ByteValues {

            private final byte[] values;

            private final ByteArrayRef arrayScratch = new ByteArrayRef(new byte[1], 1);
            private final Iter.Single iter = new Iter.Single();

            ByteValues(byte[] values) {
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
            public byte getValue(int docId) {
                return values[docId];
            }

            @Override
            public byte getValueMissing(int docId, byte missingValue) {
                return values[docId];
            }

            @Override
            public ByteArrayRef getValues(int docId) {
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

            private final byte[] values;

            private final LongArrayRef arrayScratch = new LongArrayRef(new long[1], 1);
            private final Iter.Single iter = new Iter.Single();

            LongValues(byte[] values) {
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

            private final byte[] values;

            private final DoubleArrayRef arrayScratch = new DoubleArrayRef(new double[1], 1);
            private final Iter.Single iter = new Iter.Single();

            DoubleValues(byte[] values) {
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
                return (double) values[docId];
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
