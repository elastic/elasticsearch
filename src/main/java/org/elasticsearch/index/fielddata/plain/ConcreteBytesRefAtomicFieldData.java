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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.util.BytesRefArrayRef;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.elasticsearch.index.fielddata.util.StringArrayRef;

/**
 */
public class ConcreteBytesRefAtomicFieldData implements AtomicOrdinalFieldData<ScriptDocValues.Strings> {

    // 0 ordinal in values means no value (its null)
    private final BytesRef[] values;
    private final Ordinals ordinals;

    private int[] hashes;
    private long size = -1;

    public ConcreteBytesRefAtomicFieldData(BytesRef[] values, Ordinals ordinals) {
        this.values = values;
        this.ordinals = ordinals;
    }

    @Override
    public boolean isMultiValued() {
        return ordinals.isMultiValued();
    }

    @Override
    public int getNumDocs() {
        return ordinals.getNumDocs();
    }

    @Override
    public boolean isValuesOrdered() {
        return true;
    }

    @Override
    public long getMemorySizeInBytes() {
        if (size == -1) {
            long size = RamUsage.NUM_BYTES_ARRAY_HEADER;
            for (BytesRef value : values) {
                if (value != null) {
                    size += RamUsage.NUM_BYTES_OBJECT_REF + RamUsage.NUM_BYTES_OBJECT_HEADER +
                            RamUsage.NUM_BYTES_ARRAY_HEADER + (value.length + (2 * RamUsage.NUM_BYTES_INT));
                }
            }
            size += ordinals.getMemorySizeInBytes();
            this.size = size;
        }
        return size;
    }

    @Override
    public OrdinalsBytesValues getBytesValues() {
        return ordinals.isMultiValued() ? new BytesValues.Multi(values, ordinals.ordinals()) : new BytesValues.Single(values, ordinals.ordinals());
    }

    @Override
    public OrdinalsHashedBytesValues getHashedBytesValues() {
        if (hashes == null) {
            int[] hashes = new int[values.length];
            for (int i = 0; i < values.length; i++) {
                BytesRef value = values[i];
                hashes[i] = value == null ? 0 : value.hashCode();
            }
            this.hashes = hashes;
        }
        return ordinals.isMultiValued() ? new HashedBytesValues.Multi(values, hashes, ordinals.ordinals()) : new HashedBytesValues.Single(values, hashes, ordinals.ordinals());
    }

    @Override
    public OrdinalsStringValues getStringValues() {
        return ordinals.isMultiValued() ? new StringValues.Multi(values, ordinals.ordinals()) : new StringValues.Single(values, ordinals.ordinals());
    }

    @Override
    public ScriptDocValues.Strings getScriptValues() {
        return new ScriptDocValues.Strings(getStringValues());
    }

    static abstract class BytesValues implements org.elasticsearch.index.fielddata.OrdinalsBytesValues {

        protected final BytesRef[] values;
        protected final Ordinals.Docs ordinals;

        BytesValues(BytesRef[] values, Ordinals.Docs ordinals) {
            this.values = values;
            this.ordinals = ordinals;
        }

        @Override
        public Ordinals.Docs ordinals() {
            return this.ordinals;
        }

        @Override
        public BytesRef getValueByOrd(int ord) {
            return values[ord];
        }

        @Override
        public BytesRef getValueScratchByOrd(int ord, BytesRef ret) {
            BytesRef value = values[ord];
            if (value == null) {
                ret.length = 0;
            } else {
                ret.bytes = value.bytes;
                ret.offset = value.offset;
                ret.length = value.length;
            }
            return ret;
        }

        @Override
        public BytesRef getSafeValueByOrd(int ord) {
            return values[ord];
        }

        @Override
        public boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != 0;
        }

        @Override
        public BytesRef makeSafe(BytesRef bytes) {
            // no need to do anything, its already concrete bytes...
            return bytes;
        }

        @Override
        public BytesRef getValue(int docId) {
            return values[ordinals.getOrd(docId)];
        }

        @Override
        public BytesRef getValueScratch(int docId, BytesRef ret) {
            BytesRef value = values[ordinals.getOrd(docId)];
            if (value == null) {
                ret.length = 0;
            } else {
                ret.bytes = value.bytes;
                ret.offset = value.offset;
                ret.length = value.length;
            }
            return ret;
        }

        static class Single extends BytesValues {

            private final BytesRefArrayRef arrayScratch = new BytesRefArrayRef(new BytesRef[1], 1);
            private final Iter.Single iter = new Iter.Single();

            Single(BytesRef[] values, Ordinals.Docs ordinals) {
                super(values, ordinals);
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public BytesRefArrayRef getValues(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return BytesRefArrayRef.EMPTY;
                arrayScratch.values[0] = values[ord];
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return Iter.Empty.INSTANCE;
                return iter.reset(values[ord]);
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    proc.onMissing(docId);
                } else {
                    proc.onValue(docId, values[ord]);
                }
            }
        }

        static class Multi extends BytesValues {

            private final BytesRefArrayRef arrayScratch = new BytesRefArrayRef(new BytesRef[10], 0);
            private final ValuesIter iter;

            Multi(BytesRef[] values, Ordinals.Docs ordinals) {
                super(values, ordinals);
                this.iter = new ValuesIter(values);
            }

            @Override
            public boolean isMultiValued() {
                return true;
            }

            @Override
            public BytesRefArrayRef getValues(int docId) {
                IntArrayRef ords = ordinals.getOrds(docId);
                int size = ords.size();
                if (size == 0) return BytesRefArrayRef.EMPTY;

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

                private final BytesRef[] values;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(BytesRef[] values) {
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
                public BytesRef next() {
                    BytesRef value = values[ord];
                    ord = ordsIter.next();
                    return value;
                }
            }
        }
    }

    static abstract class HashedBytesValues implements org.elasticsearch.index.fielddata.OrdinalsHashedBytesValues {

        protected final BytesRef[] values;
        protected final int[] hashes;
        protected final Ordinals.Docs ordinals;

        protected final HashedBytesRef scratch = new HashedBytesRef();

        HashedBytesValues(BytesRef[] values, int[] hashes, Ordinals.Docs ordinals) {
            this.values = values;
            this.hashes = hashes;
            this.ordinals = ordinals;
        }

        @Override
        public Ordinals.Docs ordinals() {
            return this.ordinals;
        }

        @Override
        public HashedBytesRef getValueByOrd(int ord) {
            return scratch.reset(values[ord], hashes[ord]);
        }

        @Override
        public HashedBytesRef getSafeValueByOrd(int ord) {
            return new HashedBytesRef(values[ord], hashes[ord]);
        }

        @Override
        public boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != 0;
        }

        @Override
        public HashedBytesRef makeSafe(HashedBytesRef bytes) {
            // we just need to create a copy of the bytes ref, no need
            // to create a copy of the actual BytesRef, as its concrete
            return new HashedBytesRef(bytes.bytes, bytes.hash);
        }

        @Override
        public HashedBytesRef getValue(int docId) {
            int ord = ordinals.getOrd(docId);
            if (ord == 0) return null;
            return scratch.reset(values[ord], hashes[ord]);
        }

        static class Single extends HashedBytesValues {

            private final Iter.Single iter = new Iter.Single();

            Single(BytesRef[] values, int[] hashes, Ordinals.Docs ordinals) {
                super(values, hashes, ordinals);
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public Iter getIter(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return Iter.Empty.INSTANCE;
                return iter.reset(scratch.reset(values[ord], hashes[ord]));
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    proc.onMissing(docId);
                } else {
                    proc.onValue(docId, scratch.reset(values[ord], hashes[ord]));
                }
            }
        }

        static class Multi extends HashedBytesValues {

            private final ValuesIter iter;

            Multi(BytesRef[] values, int[] hashes, Ordinals.Docs ordinals) {
                super(values, hashes, ordinals);
                this.iter = new ValuesIter(values, hashes);
            }

            @Override
            public boolean isMultiValued() {
                return true;
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
                    proc.onValue(docId, scratch.reset(values[ord], hashes[ord]));
                } while ((ord = iter.next()) != 0);
            }

            static class ValuesIter implements Iter {

                private final BytesRef[] values;
                private final int[] hashes;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                private final HashedBytesRef scratch = new HashedBytesRef();

                ValuesIter(BytesRef[] values, int[] hashes) {
                    this.values = values;
                    this.hashes = hashes;
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
                public HashedBytesRef next() {
                    HashedBytesRef value = scratch.reset(values[ord], hashes[ord]);
                    ord = ordsIter.next();
                    return value;
                }
            }
        }
    }

    static abstract class StringValues implements OrdinalsStringValues {

        protected final BytesRef[] values;
        protected final Ordinals.Docs ordinals;

        protected StringValues(BytesRef[] values, Ordinals.Docs ordinals) {
            this.values = values;
            this.ordinals = ordinals;
        }

        @Override
        public Ordinals.Docs ordinals() {
            return ordinals;
        }

        @Override
        public String getValueByOrd(int ord) {
            BytesRef value = values[ord];
            if (value == null) return null;
            return value.utf8ToString();
        }

        @Override
        public boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != 0;
        }

        @Override
        public String getValue(int docId) {
            BytesRef value = values[ordinals.getOrd(docId)];
            if (value == null) return null;
            return value.utf8ToString();
        }

        static class Single extends StringValues {

            private final StringArrayRef arrayScratch = new StringArrayRef(new String[1], 1);
            private final Iter.Single iter = new Iter.Single();

            Single(BytesRef[] values, Ordinals.Docs ordinals) {
                super(values, ordinals);
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public StringArrayRef getValues(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return StringArrayRef.EMPTY;
                BytesRef value = values[ord];
                arrayScratch.values[0] = value == null ? null : value.utf8ToString();
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return Iter.Empty.INSTANCE;
                return iter.reset(values[ord].utf8ToString());
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    proc.onMissing(docId);
                    return;
                }
                proc.onValue(docId, values[ord].utf8ToString());
            }
        }

        static class Multi extends StringValues {

            private final StringArrayRef arrayScratch = new StringArrayRef(new String[10], 0);
            private final ValuesIter iter;

            Multi(BytesRef[] values, Ordinals.Docs ordinals) {
                super(values, ordinals);
                iter = new ValuesIter(values);
            }

            @Override
            public boolean isMultiValued() {
                return true;
            }

            @Override
            public StringArrayRef getValues(int docId) {
                IntArrayRef ords = ordinals.getOrds(docId);
                int size = ords.size();
                if (size == 0) return StringArrayRef.EMPTY;

                arrayScratch.reset(size);
                for (int i = ords.start; i < ords.end; i++) {
                    BytesRef value = values[ords.values[i]];
                    arrayScratch.values[arrayScratch.end++] = value == null ? null : value.utf8ToString();
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
                    BytesRef value = values[ord];
                    proc.onValue(docId, value == null ? null : value.utf8ToString());
                } while ((ord = iter.next()) != 0);
            }

            static class ValuesIter implements StringValues.Iter {

                private final BytesRef[] values;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(BytesRef[] values) {
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
                    BytesRef value = values[ord];
                    ord = ordsIter.next();
                    return value == null ? null : value.utf8ToString();
                }
            }
        }
    }
}
