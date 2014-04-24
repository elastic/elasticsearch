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

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.PagedBytes.Reader;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.EmptyOrdinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;

/**
 */
public class PagedBytesAtomicFieldData implements AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> {

    public static PagedBytesAtomicFieldData empty() {
        return new Empty();
    }

    private final PagedBytes.Reader bytes;
    private final MonotonicAppendingLongBuffer termOrdToBytesOffset;
    protected final Ordinals ordinals;

    private volatile IntArray hashes;
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
    public long getNumberUniqueValues() {
        return ordinals.getMaxOrd() - Ordinals.MIN_ORDINAL;
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

    private final IntArray getHashes() {
        if (hashes == null) {
            long numberOfValues = termOrdToBytesOffset.size();
            IntArray hashes = BigArrays.NON_RECYCLING_INSTANCE.newIntArray(numberOfValues);
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
    public BytesValues.WithOrdinals getBytesValues(boolean needsHashes) {
        if (needsHashes) {
            final IntArray hashes = getHashes();
            return new BytesValues.HashedBytesValues(hashes, bytes, termOrdToBytesOffset, ordinals.ordinals());
        } else {
            return new BytesValues(bytes, termOrdToBytesOffset, ordinals.ordinals());
        }
    }

    @Override
    public ScriptDocValues.Strings getScriptValues() {
        return new ScriptDocValues.Strings(getBytesValues(false));
    }

    @Override
    public TermsEnum getTermsEnum() {
        return new AtomicFieldDataWithOrdinalsTermsEnum(this);
    }

    static class BytesValues extends org.elasticsearch.index.fielddata.BytesValues.WithOrdinals {

        protected final PagedBytes.Reader bytes;
        protected final MonotonicAppendingLongBuffer termOrdToBytesOffset;
        protected final Ordinals.Docs ordinals;

        BytesValues(PagedBytes.Reader bytes, MonotonicAppendingLongBuffer termOrdToBytesOffset, Ordinals.Docs ordinals) {
            super(ordinals);
            this.bytes = bytes;
            this.termOrdToBytesOffset = termOrdToBytesOffset;
            this.ordinals = ordinals;
        }

        @Override
        public BytesRef copyShared() {
            // when we fill from the pages bytes, we just reference an existing buffer slice, its enough
            // to create a shallow copy of the bytes to be safe for "reads".
            return new BytesRef(scratch.bytes, scratch.offset, scratch.length);
        }

        @Override
        public final Ordinals.Docs ordinals() {
            return this.ordinals;
        }

        @Override
        public final BytesRef getValueByOrd(long ord) {
            assert ord != Ordinals.MISSING_ORDINAL;
            bytes.fill(scratch, termOrdToBytesOffset.get(ord));
            return scratch;
        }

        @Override
        public final BytesRef nextValue() {
            bytes.fill(scratch, termOrdToBytesOffset.get(ordinals.nextOrd()));
            return scratch;
        }

        static final class HashedBytesValues extends BytesValues {
            private final IntArray hashes;


            HashedBytesValues(IntArray hashes, Reader bytes, MonotonicAppendingLongBuffer termOrdToBytesOffset, Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                this.hashes = hashes;
            }

            @Override
            public int currentValueHash() {
                assert ordinals.currentOrd() >= 0;
                return hashes.get(ordinals.currentOrd());
            }
        }

    }

    private final static class Empty extends PagedBytesAtomicFieldData {

        Empty() {
            super(emptyBytes(), 0, new MonotonicAppendingLongBuffer(), EmptyOrdinals.INSTANCE);
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
        public long getNumberUniqueValues() {
            return 0;
        }

        @Override
        public BytesValues.WithOrdinals getBytesValues(boolean needsHashes) {
            return new EmptyByteValuesWithOrdinals(ordinals.ordinals());
        }

        @Override
        public ScriptDocValues.Strings getScriptValues() {
            return ScriptDocValues.EMPTY_STRINGS;
        }
    }

}
