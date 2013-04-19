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
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
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
    private final PackedInts.Reader termOrdToBytesOffset;
    protected final Ordinals ordinals;

    private volatile int[] hashes;
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
    
    private final int[] getHashes() {
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
        return hashes;
    }

    @Override
    public BytesValues.WithOrdinals getBytesValues() {
        return ordinals.isMultiValued() ? new BytesValues.Multi(bytes, termOrdToBytesOffset, ordinals.ordinals()) : new BytesValues.Single(
                bytes, termOrdToBytesOffset, ordinals.ordinals());
    }
    
    @Override
    public org.elasticsearch.index.fielddata.BytesValues.WithOrdinals getHashedBytesValues() {
        final int[] hashes = getHashes();
        return ordinals.isMultiValued() ? new BytesValues.MultiHashed(hashes, bytes, termOrdToBytesOffset, ordinals.ordinals())
                : new BytesValues.SingleHashed(hashes, bytes, termOrdToBytesOffset, ordinals.ordinals());
    }
    
    @Override
    public ScriptDocValues.Strings getScriptValues() {
        return new ScriptDocValues.Strings(getBytesValues());
    }

    static abstract class BytesValues extends org.elasticsearch.index.fielddata.BytesValues.WithOrdinals {

        protected final PagedBytes.Reader bytes;
        protected final PackedInts.Reader termOrdToBytesOffset;
        protected final Ordinals.Docs ordinals;

        protected final BytesRef scratch = new BytesRef();

        BytesValues(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
            super(ordinals);
            this.bytes = bytes;
            this.termOrdToBytesOffset = termOrdToBytesOffset;
            this.ordinals = ordinals;
        }

        @Override
        public Ordinals.Docs ordinals() {
            return this.ordinals;
        }

        @Override
        public BytesRef getValueScratchByOrd(int ord, BytesRef ret) {
            bytes.fill(ret, termOrdToBytesOffset.get(ord));
            return ret;
        }


        static class Single extends BytesValues {

            private final Iter.Single iter;

            Single(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                assert !ordinals.isMultiValued();
                iter = newSingleIter();
            }
            
            @Override
            public Iter getIter(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return Iter.Empty.INSTANCE;
                bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                return iter.reset(scratch, ord);
            }

        }
        
        static final class SingleHashed extends Single {
            private final int[] hashes;

            SingleHashed(int[] hashes, Reader bytes, org.apache.lucene.util.packed.PackedInts.Reader termOrdToBytesOffset, Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                this.hashes = hashes;
            }
            
            @Override
            protected Iter.Single newSingleIter() {
                return new Iter.Single() {
                    public int hash() {
                        return hashes[ord];
                    }
                };
            }
            
            @Override
            public int getValueHashed(int docId, BytesRef ret) {
                final int ord = ordinals.getOrd(docId);
                getValueScratchByOrd(ord, ret);
                return hashes[ord];
            }
            
        }
        

        static class Multi extends BytesValues {

            private final Iter.Multi iter;

            Multi(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
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

            private final int[] hashes;

            MultiHashed(int[] hashes, Reader bytes, org.apache.lucene.util.packed.PackedInts.Reader termOrdToBytesOffset, Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                this.hashes = hashes;
            }

            @Override
            protected Iter.Multi newMultiIter() {
                return new Iter.Multi(this) {
                    public int hash() {
                        return hashes[ord];
                    }
                };
            }

            @Override
            public int getValueHashed(int docId, BytesRef ret) {
                int ord = ordinals.getOrd(docId);
                getValueScratchByOrd(ord, ret);
                return hashes[ord];
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
            return new BytesValues.WithOrdinals.Empty(ordinals.ordinals());
        }

        @Override
        public ScriptDocValues.Strings getScriptValues() {
            return ScriptDocValues.EMPTY_STRINGS;
        }
    }

}
