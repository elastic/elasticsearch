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

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.RamUsage;

/**
 * "Flat" multi valued ordinals, the first level array size is as the maximum
 * values a docId has. Ordinals are populated in order from the first flat array
 * value to the next.
 */
public final class MultiFlatArrayOrdinals implements Ordinals {

    // ordinals with value 0 indicates no value
    private final int[][] ordinals;
    private final int numDocs;
    private final int numOrds;
    private final int maxOrd;

    private long size = -1;

    public MultiFlatArrayOrdinals(int[][] ordinals, int numOrds) {
        assert ordinals.length > 0;
        this.ordinals = ordinals;
        this.numDocs = ordinals[0].length;
        this.numOrds = numOrds;
        this.maxOrd = numOrds + 1;
    }

    @Override
    public boolean hasSingleArrayBackingStorage() {
        return false;
    }

    @Override
    public Object getBackingStorage() {
        return ordinals;
    }

    @Override
    public long getMemorySizeInBytes() {
        if (size == -1) {
            long size = 0;
            size += RamUsage.NUM_BYTES_ARRAY_HEADER; // for the top level array
            for (int[] ordinal : ordinals) {
                size += RamUsage.NUM_BYTES_INT * ordinal.length + RamUsage.NUM_BYTES_ARRAY_HEADER;
            }
            this.size = size;
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
        return this.maxOrd;
    }

    @Override
    public Docs ordinals() {
        return new Docs(this, ordinals);
    }

    public static class Docs implements Ordinals.Docs {

        private final MultiFlatArrayOrdinals parent;
        private final int[][] ordinals;
        private final IterImpl iter;

        private final IntsRef intsScratch;

        public Docs(MultiFlatArrayOrdinals parent, int[][] ordinals) {
            this.parent = parent;
            this.ordinals = ordinals;
            this.iter = new IterImpl(ordinals);
            this.intsScratch = new IntsRef(new int[16], 0 , 16);
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
            return ordinals[0][docId];
        }

        @Override
        public IntsRef getOrds(int docId) {
            intsScratch.offset = 0;
            int i;
            for (i = 0; i < ordinals.length; i++) {
                int ordinal = ordinals[i][docId];
                if (ordinal == 0) {
                    if (i == 0) {
                        intsScratch.length = 0;
                        return intsScratch;
                    }
                    break;
                }
                intsScratch.grow(i+1);
                intsScratch.ints[i] = ordinal;
            }
            intsScratch.length = i;
            return intsScratch;
        }

        @Override
        public Iter getIter(int docId) {
            return iter.reset(docId);
        }

        @Override
        public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
            for (int i = 0; i < ordinals.length; i++) {
                int ordinal = ordinals[i][docId];
                if (ordinal == 0) {
                    if (i == 0) proc.onOrdinal(docId, 0);
                    return;
                }
                proc.onOrdinal(docId, ordinal);
            }
        }

        public static class IterImpl implements Docs.Iter {

            private final int[][] ordinals;
            private int docId;
            private int i;

            public IterImpl(int[][] ordinals) {
                this.ordinals = ordinals;
            }

            public IterImpl reset(int docId) {
                this.docId = docId;
                this.i = 0;
                return this;
            }

            @Override
            public int next() {
                if (i >= ordinals.length) return 0;
                return ordinals[i++][docId];
            }
        }
    }
}
