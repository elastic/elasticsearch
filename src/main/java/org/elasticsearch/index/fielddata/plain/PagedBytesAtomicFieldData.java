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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.EmptyOrdinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.util.BytesRefArrayRef;
import org.elasticsearch.index.fielddata.util.IntArrayRef;
import org.elasticsearch.index.fielddata.util.StringArrayRef;

/**
 */
public class PagedBytesAtomicFieldData implements AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> {

    public static PagedBytesAtomicFieldData empty(int numDocs) {
        return new Empty(numDocs);
    }

    // 0 ordinal in values means no value (its null)
    private final PagedBytes.Reader bytes;
    private final PackedInts.Reader termOrdToBytesOffset;
    protected final Ordinals ordinals;

    private int[] hashes;
    private long size = -1;
    private final long readerBytesSize;

    public PagedBytesAtomicFieldData(PagedBytes.Reader bytes, long readerBytesSize, PackedInts.Reader termOrdToBytesOffset, Ordinals ordinals) {
        this.bytes = bytes;
        this.termOrdToBytesOffset = termOrdToBytesOffset;
        this.ordinals = ordinals;
        this.readerBytesSize = readerBytesSize;
    }

    @Override
    public void close() {
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
            long size = ordinals.getMemorySizeInBytes();
            // PackedBytes
            size += readerBytesSize;
            // PackedInts
            size += termOrdToBytesOffset.ramBytesUsed();
            this.size = size;
        }
        return size;
    }

    @Override
    public BytesValues.WithOrdinals getBytesValues() {
        return ordinals.isMultiValued() ? new BytesValues.Multi(bytes, termOrdToBytesOffset, ordinals.ordinals()) : new BytesValues.Single(bytes, termOrdToBytesOffset, ordinals.ordinals());
    }

    @Override
    public HashedBytesValues.WithOrdinals getHashedBytesValues() {
        if (hashes == null) {
            int numberOfValues = termOrdToBytesOffset.size();
            int[] hashes = new int[numberOfValues];
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < numberOfValues; i++) {
                bytes.fill(scratch, termOrdToBytesOffset.get(i));
                hashes[i] = scratch.hashCode();
            }
            this.hashes = hashes;
        }
        return ordinals.isMultiValued() ? new HashedBytesValues.Multi(bytes, termOrdToBytesOffset, hashes, ordinals.ordinals()) : new HashedBytesValues.Single(bytes, termOrdToBytesOffset, hashes, ordinals.ordinals());
    }

    @Override
    public StringValues.WithOrdinals getStringValues() {
        return ordinals.isMultiValued() ? new StringValues.Multi(bytes, termOrdToBytesOffset, ordinals.ordinals()) : new StringValues.Single(bytes, termOrdToBytesOffset, ordinals.ordinals());
    }

    @Override
    public ScriptDocValues.Strings getScriptValues() {
        return new ScriptDocValues.Strings(getStringValues());
    }

    static abstract class BytesValues implements org.elasticsearch.index.fielddata.BytesValues.WithOrdinals {

        protected final PagedBytes.Reader bytes;
        protected final PackedInts.Reader termOrdToBytesOffset;
        protected final Ordinals.Docs ordinals;

        protected final BytesRef scratch = new BytesRef();

        BytesValues(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
            this.bytes = bytes;
            this.termOrdToBytesOffset = termOrdToBytesOffset;
            this.ordinals = ordinals;
        }

        @Override
        public Ordinals.Docs ordinals() {
            return this.ordinals;
        }

        @Override
        public BytesRef getValueByOrd(int ord) {
            bytes.fill(scratch, termOrdToBytesOffset.get(ord));
            return scratch;
        }

        @Override
        public BytesRef getValueScratchByOrd(int ord, BytesRef ret) {
            bytes.fill(ret, termOrdToBytesOffset.get(ord));
            return ret;
        }

        @Override
        public BytesRef getSafeValueByOrd(int ord) {
            final BytesRef retVal = new BytesRef();
            bytes.fill(retVal, termOrdToBytesOffset.get(ord));
            return retVal;
        }

        @Override
        public boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != 0;
        }

        @Override
        public BytesRef makeSafe(BytesRef bytes) {
            return BytesRef.deepCopyOf(bytes);
        }

        @Override
        public BytesRef getValue(int docId) {
            int ord = ordinals.getOrd(docId);
            if (ord == 0) return null;
            bytes.fill(scratch, termOrdToBytesOffset.get(ord));
            return scratch;
        }

        @Override
        public BytesRef getValueScratch(int docId, BytesRef ret) {
            bytes.fill(ret, termOrdToBytesOffset.get(ordinals.getOrd(docId)));
            return ret;
        }

        static class Single extends BytesValues {

            private final BytesRefArrayRef arrayScratch = new BytesRefArrayRef(new BytesRef[1], 1);
            private final Iter.Single iter = new Iter.Single();

            Single(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public BytesRefArrayRef getValues(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return BytesRefArrayRef.EMPTY;
                arrayScratch.values[0] = new BytesRef();
                bytes.fill(arrayScratch.values[0], termOrdToBytesOffset.get(ord));
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return Iter.Empty.INSTANCE;
                bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                return iter.reset(scratch);
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    proc.onMissing(docId);
                } else {
                    bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                    proc.onValue(docId, scratch);
                }
            }
        }

        static class Multi extends BytesValues {

            private final BytesRefArrayRef arrayScratch = new BytesRefArrayRef(new BytesRef[10], 0);
            private final ValuesIter iter;

            Multi(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                this.iter = new ValuesIter(bytes, termOrdToBytesOffset);
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
                    final BytesRef bytesRef = new BytesRef();
                    bytes.fill(bytesRef, termOrdToBytesOffset.get(ords.values[i]));
                    arrayScratch.values[arrayScratch.end++] = bytesRef;
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
                    bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                    proc.onValue(docId, scratch);
                } while ((ord = iter.next()) != 0);
            }

            static class ValuesIter implements Iter {

                private final PagedBytes.Reader bytes;
                private final PackedInts.Reader termOrdToBytesOffset;
                private final BytesRef scratch = new BytesRef();
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset) {
                    this.bytes = bytes;
                    this.termOrdToBytesOffset = termOrdToBytesOffset;
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
                    bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                    ord = ordsIter.next();
                    return scratch;
                }
            }
        }
    }

    static abstract class HashedBytesValues implements org.elasticsearch.index.fielddata.HashedBytesValues.WithOrdinals {

        protected final PagedBytes.Reader bytes;
        protected final PackedInts.Reader termOrdToBytesOffset;
        protected final int[] hashes;
        protected final Ordinals.Docs ordinals;

        protected final BytesRef scratch1 = new BytesRef();
        protected final HashedBytesRef scratch = new HashedBytesRef();

        HashedBytesValues(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, int[] hashes, Ordinals.Docs ordinals) {
            this.bytes = bytes;
            this.termOrdToBytesOffset = termOrdToBytesOffset;
            this.hashes = hashes;
            this.ordinals = ordinals;
        }

        @Override
        public Ordinals.Docs ordinals() {
            return this.ordinals;
        }

        @Override
        public HashedBytesRef getValueByOrd(int ord) {
            bytes.fill(scratch1, termOrdToBytesOffset.get(ord));
            return scratch.reset(scratch1, hashes[ord]);
        }

        @Override
        public HashedBytesRef getSafeValueByOrd(int ord) {
            final BytesRef bytesRef = new BytesRef();
            bytes.fill(bytesRef, termOrdToBytesOffset.get(ord));
            return new HashedBytesRef(bytesRef, hashes[ord]);
        }

        @Override
        public boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != 0;
        }

        @Override
        public HashedBytesRef makeSafe(HashedBytesRef bytes) {
            return new HashedBytesRef(BytesRef.deepCopyOf(bytes.bytes), bytes.hash);
        }

        @Override
        public HashedBytesRef getValue(int docId) {
            int ord = ordinals.getOrd(docId);
            if (ord == 0) return null;
            bytes.fill(scratch1, termOrdToBytesOffset.get(ord));
            return scratch.reset(scratch1, hashes[ord]);
        }

        static class Single extends HashedBytesValues {

            private final Iter.Single iter = new Iter.Single();

            Single(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, int[] hashes, Ordinals.Docs ordinals) {
                super(bytes, termOrdToBytesOffset, hashes, ordinals);
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public Iter getIter(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return Iter.Empty.INSTANCE;
                bytes.fill(scratch1, termOrdToBytesOffset.get(ord));
                return iter.reset(scratch.reset(scratch1, hashes[ord]));
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    proc.onMissing(docId);
                } else {
                    bytes.fill(scratch1, termOrdToBytesOffset.get(ord));
                    proc.onValue(docId, scratch.reset(scratch1, hashes[ord]));
                }
            }
        }

        static class Multi extends HashedBytesValues {

            private final ValuesIter iter;

            Multi(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, int[] hashes, Ordinals.Docs ordinals) {
                super(bytes, termOrdToBytesOffset, hashes, ordinals);
                this.iter = new ValuesIter(bytes, termOrdToBytesOffset, hashes);
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
                    bytes.fill(scratch1, termOrdToBytesOffset.get(ord));
                    proc.onValue(docId, scratch.reset(scratch1, hashes[ord]));
                } while ((ord = iter.next()) != 0);
            }

            static class ValuesIter implements Iter {

                private final PagedBytes.Reader bytes;
                private final PackedInts.Reader termOrdToBytesOffset;
                private final int[] hashes;
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                private final BytesRef scratch1 = new BytesRef();
                private final HashedBytesRef scratch = new HashedBytesRef();

                ValuesIter(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, int[] hashes) {
                    this.bytes = bytes;
                    this.termOrdToBytesOffset = termOrdToBytesOffset;
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
                    bytes.fill(scratch1, termOrdToBytesOffset.get(ord));
                    HashedBytesRef value = scratch.reset(scratch1, hashes[ord]);
                    ord = ordsIter.next();
                    return value;
                }
            }
        }
    }

    static abstract class StringValues implements org.elasticsearch.index.fielddata.StringValues.WithOrdinals {

        protected final PagedBytes.Reader bytes;
        protected final PackedInts.Reader termOrdToBytesOffset;
        protected final Ordinals.Docs ordinals;

        protected final BytesRef scratch = new BytesRef();

        protected StringValues(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
            this.bytes = bytes;
            this.termOrdToBytesOffset = termOrdToBytesOffset;
            this.ordinals = ordinals;
        }

        @Override
        public Ordinals.Docs ordinals() {
            return ordinals;
        }

        @Override
        public String getValueByOrd(int ord) {
            bytes.fill(scratch, termOrdToBytesOffset.get(ord));
            return scratch.utf8ToString();
        }

        @Override
        public boolean hasValue(int docId) {
            return ordinals.getOrd(docId) != 0;
        }

        @Override
        public String getValue(int docId) {
            int ord = ordinals.getOrd(docId);
            if (ord == 0) return null;
            bytes.fill(scratch, termOrdToBytesOffset.get(ord));
            return scratch.utf8ToString();
        }

        static class Single extends StringValues {

            private final StringArrayRef arrayScratch = new StringArrayRef(new String[1], 1);
            private final Iter.Single iter = new Iter.Single();

            Single(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
            }

            @Override
            public boolean isMultiValued() {
                return false;
            }

            @Override
            public StringArrayRef getValues(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return StringArrayRef.EMPTY;
                bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                arrayScratch.values[0] = scratch.utf8ToString();
                return arrayScratch;
            }

            @Override
            public Iter getIter(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return Iter.Empty.INSTANCE;
                bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                return iter.reset(scratch.utf8ToString());
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) {
                    proc.onMissing(docId);
                    return;
                }
                bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                proc.onValue(docId, scratch.utf8ToString());
            }
        }

        static class Multi extends StringValues {

            private final StringArrayRef arrayScratch = new StringArrayRef(new String[10], 0);
            private final ValuesIter iter;

            Multi(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                iter = new ValuesIter(bytes, termOrdToBytesOffset);
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
                    bytes.fill(scratch, termOrdToBytesOffset.get(ords.values[i]));
                    arrayScratch.values[arrayScratch.end++] = scratch.utf8ToString();
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
                    bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                    proc.onValue(docId, scratch.utf8ToString());
                } while ((ord = iter.next()) != 0);
            }

            static class ValuesIter implements StringValues.Iter {

                private final PagedBytes.Reader bytes;
                private final PackedInts.Reader termOrdToBytesOffset;
                private final BytesRef scratch = new BytesRef();
                private Ordinals.Docs.Iter ordsIter;
                private int ord;

                ValuesIter(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset) {
                    this.bytes = bytes;
                    this.termOrdToBytesOffset = termOrdToBytesOffset;
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
                    bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                    ord = ordsIter.next();
                    return scratch.utf8ToString();
                }
            }
        }
    }

    static class Empty extends PagedBytesAtomicFieldData {

        Empty(int numDocs) {
            super(emptyBytes(), 0, new GrowableWriter(1, 2, PackedInts.FASTEST).getMutable(), new EmptyOrdinals(numDocs));
        }

        static PagedBytes.Reader emptyBytes() {
            PagedBytes bytes = new PagedBytes(1);
            bytes.copyUsingLengthPrefix(new BytesRef());
            return bytes.freeze(true);
        }

        @Override
        public boolean isMultiValued() {
            return false;
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
        public BytesValues.WithOrdinals getBytesValues() {
            return new BytesValues.WithOrdinals.Empty((EmptyOrdinals) ordinals);
        }

        @Override
        public HashedBytesValues.WithOrdinals getHashedBytesValues() {
            return new HashedBytesValues.Empty((EmptyOrdinals) ordinals);
        }

        @Override
        public StringValues.WithOrdinals getStringValues() {
            return new StringValues.WithOrdinals.Empty((EmptyOrdinals) ordinals);
        }

        @Override
        public ScriptDocValues.Strings getScriptValues() {
            return ScriptDocValues.EMPTY_STRINGS;
        }
    }

}
