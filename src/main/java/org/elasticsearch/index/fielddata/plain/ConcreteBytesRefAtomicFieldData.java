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
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.EmptyOrdinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.util.BytesRefArrayRef;

/**
 */
public class ConcreteBytesRefAtomicFieldData implements AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> {

    public static ConcreteBytesRefAtomicFieldData empty(int numDocs) {
        return new Empty(numDocs);
    }

    // 0 ordinal in values means no value (its null)
    private final BytesRef[] values;
    protected final Ordinals ordinals;

    private volatile int[] hashes;
    private long size = -1;

    public ConcreteBytesRefAtomicFieldData(BytesRef[] values, Ordinals ordinals) {
        this.values = values;
        this.ordinals = ordinals;
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
    public BytesValues.WithOrdinals getBytesValues() {
        return ordinals.isMultiValued() ? new BytesValues.Multi(values, ordinals.ordinals()) : new BytesValues.Single(values, ordinals.ordinals());
    }

    @Override
    public BytesValues getHashedBytesValues() {
        if (hashes == null) {
            int[] hashes = new int[values.length];
            for (int i = 0; i < values.length; i++) {
                BytesRef value = values[i];
                hashes[i] = value == null ? 0 : value.hashCode();
            }
            this.hashes = hashes;
        }
        return ordinals.isMultiValued() ? new BytesValues.MultiHashed(values, ordinals.ordinals(), hashes) : new BytesValues.SingleHashed(values, ordinals.ordinals(), hashes);
    }


    @Override
    public ScriptDocValues.Strings getScriptValues() {
        return new ScriptDocValues.Strings(getBytesValues());
    }

    static abstract class BytesValues extends org.elasticsearch.index.fielddata.BytesValues.WithOrdinals {

        protected final BytesRef[] values;

        BytesValues(BytesRef[] values, Ordinals.Docs ordinals) {
            super(ordinals);
            this.values = values;
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
        public BytesRef makeSafe(BytesRef bytes) {
            // no need to do anything, its already concrete bytes...
            return bytes;
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

            private final Iter.Single iter;

            Single(BytesRef[] values, Ordinals.Docs ordinals) {
                super(values, ordinals);
                this.iter = newSingleIter();
            }

            @Override
            public Iter getIter(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return Iter.Empty.INSTANCE;
                return iter.reset(values[ord], ord);
            }
        }
        
        static final class SingleHashed extends Single {
            private final int[] hashes;
            SingleHashed(BytesRef[] values, Docs ordinals, int[] hashes) {
                super(values, ordinals);
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

            Multi(BytesRef[] values, Ordinals.Docs ordinals) {
                super(values, ordinals);
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

            MultiHashed(BytesRef[] values, Ordinals.Docs ordinals, int[] hashes) {
                super(values, ordinals);
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
                final int ord = ordinals.getOrd(docId);
                getValueScratchByOrd(ord, ret);
                return hashes[ord];
            }
            
        }
    }


    static class Empty extends ConcreteBytesRefAtomicFieldData {

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
        public long getMemorySizeInBytes() {
            return 0;
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
