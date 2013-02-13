/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.elasticsearch.index.fielddata.ordinals;

import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.fielddata.util.IntArrayRef;

/**
 * Ordinals implementation that stores the ordinals into sparse fixed arrays.
 * <p/>
 * This prevents large ordinal arrays that are created in for example {@link MultiFlatArrayOrdinals} when
 * only a few documents have a lot of terms per field.
 */
public final class SparseMultiArrayOrdinals implements Ordinals {

    private final int[] lookup;
    private final PositiveIntPool pool;
    private final int numOrds;
    private final int maxOrd;
    private final int numDocs;
    private long size;

    public SparseMultiArrayOrdinals(OrdinalsBuilder builder, int maxSize) {
        int blockShift = Math.min(floorPow2(builder.getTotalNumOrds() << 1), floorPow2(maxSize));
        this.pool = new PositiveIntPool(Math.max(4, blockShift));
        this.numDocs = builder.maxDoc();


        this.lookup = new int[numDocs];
        this.numOrds = builder.getNumOrds();
        this.maxOrd = numOrds + 1;
        IntArrayRef spare;
        for (int doc = 0; doc < numDocs; doc++) {
            spare = builder.docOrds(doc);
            int size = spare.size();
            if (size == 0) {
                lookup[doc] = 0;
            } else if (size == 1) {
                lookup[doc] = spare.values[spare.start];
            } else {
                int offset = pool.put(spare);
                lookup[doc] = -(offset) - 1;
            }
        }
    }

    private static int floorPow2(int number) {
        return 31 - Integer.numberOfLeadingZeros(number);
    }

    @Override
    public boolean hasSingleArrayBackingStorage() {
        return false;
    }

    @Override
    public Object getBackingStorage() {
        return null;
    }

    @Override
    public long getMemorySizeInBytes() {
        if (size == -1) {
            size = (RamUsage.NUM_BYTES_ARRAY_HEADER + (RamUsage.NUM_BYTES_INT * lookup.length)) + pool.getMemorySizeInBytes();
        }
        return size;
    }

    @Override
    public boolean isMultiValued() {
        return true;
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    @Override
    public int getNumOrds() {
        return numOrds;
    }

    @Override
    public int getMaxOrd() {
        return maxOrd;
    }

    @Override
    public Docs ordinals() {
        return new Docs(this, lookup, pool);
    }

    static class Docs implements Ordinals.Docs {

        private final SparseMultiArrayOrdinals parent;
        private final int[] lookup;

        private final IterImpl iter;
        private final PositiveIntPool pool;
        private final IntArrayRef spare = new IntArrayRef(new int[1]);

        public Docs(SparseMultiArrayOrdinals parent, int[] lookup, PositiveIntPool pool) {
            this.parent = parent;
            this.lookup = lookup;
            this.pool = pool;
            this.iter = new IterImpl(lookup, pool);
        }

        @Override
        public Ordinals ordinals() {
            return this.parent;
        }

        @Override
        public int getNumDocs() {
            return parent.getNumDocs();
        }

        @Override
        public int getNumOrds() {
            return parent.getNumOrds();
        }

        @Override
        public int getMaxOrd() {
            return parent.getMaxOrd();
        }

        @Override
        public boolean isMultiValued() {
            return true;
        }

        @Override
        public int getOrd(int docId) {
            int pointer = lookup[docId];
            if (pointer < 0) {
                return pool.getFirstFromOffset(-(pointer + 1));
            }
            return pointer;
        }

        @Override
        public IntArrayRef getOrds(int docId) {
            spare.end = 0;
            int pointer = lookup[docId];
            if (pointer == 0) {
                return IntArrayRef.EMPTY;
            } else if (pointer > 0) {
                spare.end = 1;
                spare.values[0] = pointer;
                return spare;
            } else {
                pool.fill(spare, -(pointer + 1));
                return spare;
            }
        }

        @Override
        public Iter getIter(int docId) {
            return iter.reset(docId);
        }

        @Override
        public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
            int pointer = lookup[docId];
            if (pointer >= 0) {
                proc.onOrdinal(docId, pointer);
            } else {
                pool.fill(spare, -(pointer + 1));
                for (int i = spare.start; i < spare.end; i++) {
                    proc.onOrdinal(docId, spare.values[i]);
                }
            }
        }

        class IterImpl implements Docs.Iter {
            private final int[] lookup;
            private final PositiveIntPool pool;
            private final IntArrayRef slice = new IntArrayRef(new int[1]);
            private int valuesOffset;

            public IterImpl(int[] lookup, PositiveIntPool pool) {
                this.lookup = lookup;
                this.pool = pool;
            }

            public IterImpl reset(int docId) {
                final int pointer = lookup[docId];
                if (pointer < 0) {
                    pool.fill(slice, -(pointer + 1));
                } else {
                    slice.values[0] = pointer;
                    slice.start = 0;
                    slice.end = 1;
                }
                valuesOffset = 0;
                return this;
            }

            @Override
            public int next() {
                if (valuesOffset >= slice.end) {
                    return 0;
                }
                return slice.values[slice.start + (valuesOffset++)];
            }
        }
    }
}
