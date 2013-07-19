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
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.*;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.elasticsearch.common.util.BigIntArray;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.EmptyOrdinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;

import java.io.IOException;

/**
 */
public class FSTBytesAtomicFieldData implements AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> {

    public static FSTBytesAtomicFieldData empty(int numDocs) {
        return new Empty(numDocs);
    }

    // 0 ordinal in values means no value (its null)
    protected final Ordinals ordinals;

    private volatile BigIntArray hashes;
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
            // FST
            size += fst == null ? 0 : fst.sizeInBytes();
            this.size = size;
        }
        return size;
    }

    @Override
    public BytesValues.WithOrdinals getBytesValues() {
        assert fst != null;
        return ordinals.isMultiValued() ? new BytesValues.Multi(fst, ordinals.ordinals()) : new BytesValues.Single(fst, ordinals.ordinals());
    }


    @Override
    public ScriptDocValues.Strings getScriptValues() {
        assert fst != null;
        return new ScriptDocValues.Strings(getBytesValues());
    }

    @Override
    public org.elasticsearch.index.fielddata.BytesValues.WithOrdinals getHashedBytesValues() {
        assert fst != null;
        if (hashes == null) {
            BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<Long>(fst);
            BigIntArray hashes = new BigIntArray(ordinals.getMaxOrd());
            // we don't store an ord 0 in the FST since we could have an empty string in there and FST don't support
            // empty strings twice. ie. them merge fails for long output.
            hashes.set(0, new BytesRef().hashCode());
            try {
                for (long i = 1, maxOrd = ordinals.getMaxOrd(); i < maxOrd; ++i) {
                    hashes.set(i, fstEnum.next().input.hashCode());
                }
                assert fstEnum.next() == null;
            } catch (IOException e) {
                // Don't use new "AssertionError("Cannot happen", e)" directly as this is a Java 1.7-only API
                final AssertionError error = new AssertionError("Cannot happen");
                error.initCause(e);
                throw error;
            }
            this.hashes = hashes;
        }
        return ordinals.isMultiValued() ? new BytesValues.MultiHashed(fst, ordinals.ordinals(), hashes) : new BytesValues.SingleHashed(fst, ordinals.ordinals(), hashes);
    }

    static abstract class BytesValues extends org.elasticsearch.index.fielddata.BytesValues.WithOrdinals {

        protected final FST<Long> fst;
        protected final Ordinals.Docs ordinals;

        // per-thread resources
        protected final BytesReader in;
        protected final Arc<Long> firstArc = new Arc<Long>();
        protected final Arc<Long> scratchArc = new Arc<Long>();
        protected final IntsRef scratchInts = new IntsRef();

        BytesValues(FST<Long> fst, Ordinals.Docs ordinals) {
            super(ordinals);
            this.fst = fst;
            this.ordinals = ordinals;
            in = fst.getBytesReader();
        }

        @Override
        public BytesRef getValueScratchByOrd(long ord, BytesRef ret) {
            if (ord == 0) {
                ret.length = 0;
                return ret;
            }
            in.setPosition(0);
            fst.getFirstArc(firstArc);
            try {
                IntsRef output = Util.getByOutput(fst, ord, in, firstArc, scratchArc, scratchInts);
                ret.grow(output.length);
                ret.length = ret.offset = 0;
                Util.toBytesRef(output, ret);
            } catch (IOException ex) {
                //bogus
            }
            return ret;
        }

        static class Single extends BytesValues {
            private final Iter.Single iter;

            Single(FST<Long> fst, Ordinals.Docs ordinals) {
                super(fst, ordinals);
                assert !ordinals.isMultiValued();
                this.iter = newSingleIter();
            }

            @Override
            public Iter getIter(int docId) {
                long ord = ordinals.getOrd(docId);
                if (ord == 0) return Iter.Empty.INSTANCE;
                return iter.reset(getValueByOrd(ord), ord);
            }
        }

        static final class SingleHashed extends Single {
            private final BigIntArray hashes;

            SingleHashed(FST<Long> fst, Docs ordinals, BigIntArray hashes) {
                super(fst, ordinals);
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

            Multi(FST<Long> fst, Ordinals.Docs ordinals) {
                super(fst, ordinals);
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

            MultiHashed(FST<Long> fst, Docs ordinals, BigIntArray hashes) {
                super(fst, ordinals);
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
                final long ord = ordinals.getOrd(docId);
                getValueScratchByOrd(ord, ret);
                return hashes.get(ord);
            }

        }
    }


    static class Empty extends FSTBytesAtomicFieldData {

        Empty(int numDocs) {
            super(null, new EmptyOrdinals(numDocs));
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

        @Override
        public org.elasticsearch.index.fielddata.BytesValues.WithOrdinals getHashedBytesValues() {
            return getBytesValues();
        }
    }


}
