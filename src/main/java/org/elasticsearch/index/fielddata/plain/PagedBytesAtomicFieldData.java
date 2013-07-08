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
import org.apache.lucene.util.PagedBytes.Reader;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.elasticsearch.common.util.BigIntArray;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.EmptyOrdinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;

/**
 */
public class PagedBytesAtomicFieldData implements AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> {

    public static PagedBytesAtomicFieldData empty(int numDocs) {
        return new Empty(numDocs);
    }

    // 0 ordinal in values means no value (its null)
    private final PagedBytes.Reader bytes;
    private final MonotonicAppendingLongBuffer termOrdToBytesOffset;
    protected final Ordinals ordinals;

    private volatile BigIntArray hashes;
    private long size = -1;
    private final long readerBytesSize;

    public PagedBytesAtomicFieldData(PagedBytes.Reader bytes, long readerBytesSize, MonotonicAppendingLongBuffer termOrdToBytesOffset, Ordinals ordinals) {
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

    private final BigIntArray getHashes() {
        if (hashes == null) {
            long numberOfValues = termOrdToBytesOffset.size();
            BigIntArray hashes = new BigIntArray(numberOfValues);
            BytesRef scratch = new BytesRef();
            for (long i = 0; i < numberOfValues; i++) {
                bytes.fill(scratch, termOrdToBytesOffset.get(i));
                hashes.set(i, scratch.hashCode());
            }
            this.hashes = hashes;
        }
        return hashes;
    }

    @Override
    public BytesValues.WithOrdinals getBytesValues() {
        return ordinals.isMultiValued() ? new BytesValues.Multi(bytes, termOrdToBytesOffset, ordinals.ordinals()) : new BytesValues.Single(
                bytes, termOrdToBytesOffset, ordinals.ordinals());
    }

    @Override
    public org.elasticsearch.index.fielddata.BytesValues.WithOrdinals getHashedBytesValues() {
        final BigIntArray hashes = getHashes();
        return ordinals.isMultiValued() ? new BytesValues.MultiHashed(hashes, bytes, termOrdToBytesOffset, ordinals.ordinals())
                : new BytesValues.SingleHashed(hashes, bytes, termOrdToBytesOffset, ordinals.ordinals());
    }

    @Override
    public ScriptDocValues.Strings getScriptValues() {
        return new ScriptDocValues.Strings(getBytesValues());
    }

    static abstract class BytesValues extends org.elasticsearch.index.fielddata.BytesValues.WithOrdinals {

        protected final PagedBytes.Reader bytes;
        protected final MonotonicAppendingLongBuffer termOrdToBytesOffset;
        protected final Ordinals.Docs ordinals;

        protected final BytesRef scratch = new BytesRef();

        BytesValues(PagedBytes.Reader bytes, MonotonicAppendingLongBuffer termOrdToBytesOffset, Ordinals.Docs ordinals) {
            super(ordinals);
            this.bytes = bytes;
            this.termOrdToBytesOffset = termOrdToBytesOffset;
            this.ordinals = ordinals;
        }

        @Override
        public BytesRef makeSafe(BytesRef bytes) {
            // when we fill from the pages bytes, we just reference an existing buffer slice, its enough
            // to create a shallow copy of the bytes to be safe for "reads".
            return new BytesRef(bytes.bytes, bytes.offset, bytes.length);
        }

        @Override
        public Ordinals.Docs ordinals() {
            return this.ordinals;
        }

        @Override
        public BytesRef getValueScratchByOrd(long ord, BytesRef ret) {
            bytes.fill(ret, termOrdToBytesOffset.get(ord));
            return ret;
        }


        static class Single extends BytesValues {

            private final Iter.Single iter;

            Single(PagedBytes.Reader bytes, MonotonicAppendingLongBuffer termOrdToBytesOffset, Ordinals.Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                assert !ordinals.isMultiValued();
                iter = newSingleIter();
            }

            @Override
            public Iter getIter(int docId) {
                long ord = ordinals.getOrd(docId);
                if (ord == 0) return Iter.Empty.INSTANCE;
                bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                return iter.reset(scratch, ord);
            }

        }

        static final class SingleHashed extends Single {
            private final BigIntArray hashes;

            SingleHashed(BigIntArray hashes, Reader bytes, MonotonicAppendingLongBuffer termOrdToBytesOffset, Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                this.hashes = hashes;
            }

            @Override
            protected Iter.Single newSingleIter() {
                return new Iter.Single() {
                    public int hash() {
                        return hashes.get(ord);
                    }
                };
            }

            @Override
            public int getValueHashed(int docId, BytesRef ret) {
                final long ord = ordinals.getOrd(docId);
                getValueScratchByOrd(ord, ret);
                return hashes.get(ord);
            }

        }


        static class Multi extends BytesValues {

            private final Iter.Multi iter;

            Multi(PagedBytes.Reader bytes, MonotonicAppendingLongBuffer termOrdToBytesOffset, Ordinals.Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                assert ordinals.isMultiValued();
                this.iter = newMultiIter();
            }

            @Override
            public Iter getIter(int docId) {
                return iter.reset(ordinals.getIter(docId));
            }
        }

        static final class MultiHashed extends Multi {

            private final BigIntArray hashes;

            MultiHashed(BigIntArray hashes, Reader bytes, MonotonicAppendingLongBuffer termOrdToBytesOffset, Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                this.hashes = hashes;
            }

            @Override
            protected Iter.Multi newMultiIter() {
                return new Iter.Multi(this) {
                    public int hash() {
                        return hashes.get(ord);
                    }
                };
            }

            @Override
            public int getValueHashed(int docId, BytesRef ret) {
                long ord = ordinals.getOrd(docId);
                getValueScratchByOrd(ord, ret);
                return hashes.get(ord);
            }

        }
    }

    static class Empty extends PagedBytesAtomicFieldData {

        Empty(int numDocs) {
            super(emptyBytes(), 0, new MonotonicAppendingLongBuffer(), new EmptyOrdinals(numDocs));
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
            return new BytesValues.WithOrdinals.Empty(ordinals.ordinals());
        }

        @Override
        public ScriptDocValues.Strings getScriptValues() {
            return ScriptDocValues.EMPTY_STRINGS;
        }
    }

}
