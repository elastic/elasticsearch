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
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.*;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.EmptyOrdinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;

import java.io.IOException;

/**
 */
public class FSTBytesAtomicFieldData implements AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> {

    public static FSTBytesAtomicFieldData empty() {
        return new Empty();
    }

    // 0 ordinal in values means no value (its null)
    protected final Ordinals ordinals;

    private volatile IntArray hashes;
    private long size = -1;

    private final FST<Long> fst;

    public FSTBytesAtomicFieldData(FST<Long> fst, Ordinals ordinals) {
        this.ordinals = ordinals;
        this.fst = fst;
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
            // FST
            size += fst == null ? 0 : fst.sizeInBytes();
            this.size = size;
        }
        return size;
    }

    @Override
    public BytesValues.WithOrdinals getBytesValues(boolean needsHashes) {
        assert fst != null;
        if (needsHashes) {
            if (hashes == null) {
                BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<>(fst);
                IntArray hashes = BigArrays.NON_RECYCLING_INSTANCE.newIntArray(ordinals.getMaxOrd());
                try {
                    for (long i = 0, maxOrd = ordinals.getMaxOrd(); i < maxOrd; ++i) {
                        hashes.set(i, fstEnum.next().input.hashCode());
                    }
                    assert fstEnum.next() == null;
                } catch (IOException e) {
                    throw new AssertionError("Cannot happen", e);
                }
                this.hashes = hashes;
            }
            return new HashedBytesValues(fst, ordinals.ordinals(), hashes);
        } else {
            return new BytesValues(fst, ordinals.ordinals());
        }
    }


    @Override
    public ScriptDocValues.Strings getScriptValues() {
        assert fst != null;
        return new ScriptDocValues.Strings(getBytesValues(false));
    }

    @Override
    public TermsEnum getTermsEnum() {
        return new AtomicFieldDataWithOrdinalsTermsEnum(this);
    }

    static class BytesValues extends org.elasticsearch.index.fielddata.BytesValues.WithOrdinals {

        protected final FST<Long> fst;
        protected final Ordinals.Docs ordinals;

        // per-thread resources
        protected final BytesReader in;
        protected final Arc<Long> firstArc = new Arc<>();
        protected final Arc<Long> scratchArc = new Arc<>();
        protected final IntsRef scratchInts = new IntsRef();

        BytesValues(FST<Long> fst, Ordinals.Docs ordinals) {
            super(ordinals);
            this.fst = fst;
            this.ordinals = ordinals;
            in = fst.getBytesReader();
        }

        @Override
        public BytesRef getValueByOrd(long ord) {
            assert ord != Ordinals.MISSING_ORDINAL;
            in.setPosition(0);
            fst.getFirstArc(firstArc);
            try {
                IntsRef output = Util.getByOutput(fst, ord, in, firstArc, scratchArc, scratchInts);
                scratch.length = scratch.offset = 0;
                scratch.grow(output.length);
                Util.toBytesRef(output, scratch);
            } catch (IOException ex) {
                //bogus
            }
            return scratch;
        }

    }
    
    static final class HashedBytesValues extends BytesValues {
        private final IntArray hashes;

        HashedBytesValues(FST<Long> fst, Docs ordinals, IntArray hashes) {
            super(fst, ordinals);
            this.hashes = hashes;
        }

        @Override
        public int currentValueHash() {
            assert ordinals.currentOrd() >= 0;
            return hashes.get(ordinals.currentOrd());
        }
    }


    final static class Empty extends FSTBytesAtomicFieldData {

        Empty() {
            super(null, EmptyOrdinals.INSTANCE);
        }

        @Override
        public boolean isMultiValued() {
            return false;
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
