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

import gnu.trove.list.array.TIntArrayList;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.fielddata.util.IntArrayRef;

import java.util.ArrayList;
import java.util.List;

/**
 * Ordinals implementation that stores the ordinals into sparse fixed arrays.
 * <p/>
 * This prevents large ordinal arrays that are created in for example {@link MultiFlatArrayOrdinals} when
 * only a few documents have a lot of terms per field.
 */
public class SparseMultiArrayOrdinals implements Ordinals {

    // Contains pointer in starageOrdinals or the actual ordinal if document has one ordinal
    private final int[] lookup;

    // Contains the ordinals for documents that have more than one ordinal. Each of this document has a start
    // point this array and the last ordinal is marked as negative.
    private final int[][] storageOrdinals;

    // The n-th bit to shift the index of the storage array to inside the lookup pointer
    final int storageShift;
    final int numOrds;
    final int numDocs;

    private long size;

    /**
     * @param loadedOrds The ordinals
     * @param numOrds    The total number of unique ords
     * @param maxSize    The maximum size in elements for each individual storage array
     */
    public SparseMultiArrayOrdinals(int[][] loadedOrds, int numOrds, int maxSize) {
        int maxDoc = loadedOrds[0].length;
        if (loadedOrds.length * loadedOrds[0].length < maxSize) {
            maxSize = loadedOrds.length * loadedOrds[0].length + 1;
        }

        int tempMaxSize = maxSize;
        int storageShift = 0;
        while (tempMaxSize > 0) {
            storageShift++;
            tempMaxSize = tempMaxSize >> 1;
        }
        this.storageShift = storageShift;

        this.lookup = new int[maxDoc];
        this.numDocs = loadedOrds[0].length;
        this.numOrds = numOrds;
        List<int[]> allStorageArrays = new ArrayList<int[]>();

        TIntArrayList currentStorageArray = new TIntArrayList(maxSize);
        currentStorageArray.add(Integer.MIN_VALUE);

        TIntArrayList currentDocOrs = new TIntArrayList();
        for (int doc = 0; doc < maxDoc; doc++) {
            currentDocOrs.resetQuick();
            for (int[] currentOrds : loadedOrds) {
                int currentOrd = currentOrds[doc];
                if (currentOrd == 0) {
                    break;
                }
                currentDocOrs.add(currentOrd);
            }

            int currentStorageArrayOffset = currentStorageArray.size();
            if (currentStorageArrayOffset + currentDocOrs.size() >= maxSize) {
                if (currentDocOrs.size() >= maxSize) {
                    throw new ElasticSearchException("Doc[" + doc + "] has " + currentDocOrs.size() + " ordinals, but it surpasses the limit of " + maxSize);
                }

                allStorageArrays.add(currentStorageArray.toArray());
                currentStorageArray.resetQuick();
                currentStorageArray.add(Integer.MIN_VALUE);
                currentStorageArrayOffset = 1;
            }

            int size = currentDocOrs.size();
            if (size == 0) {
                lookup[doc] = 0;
            } else if (size == 1) {
                lookup[doc] = currentDocOrs.get(0);
            } else {
                // Mark the last ordinal for this doc.
                currentDocOrs.set(currentDocOrs.size() - 1, -currentDocOrs.get(currentDocOrs.size() - 1));
                currentStorageArray.addAll(currentDocOrs);
                lookup[doc] = allStorageArrays.size() << storageShift; // The left side of storageShift is for index in main array
                lookup[doc] |= (currentStorageArrayOffset & ((1 << storageShift) - 1)); // The right side of storageShift is for index in ordinal array
                lookup[doc] = -lookup[doc]; // Mark this value as 'pointer' into ordinals array
            }
        }

        if (!currentStorageArray.isEmpty()) {
            allStorageArrays.add(currentStorageArray.toArray());
        }

        this.storageOrdinals = new int[allStorageArrays.size()][];
        for (int i = 0; i < this.storageOrdinals.length; i++) {
            this.storageOrdinals[i] = allStorageArrays.get(i);
        }
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
            long size = 0;
            size += RamUsage.NUM_BYTES_ARRAY_HEADER;
            for (int[] ordinal : storageOrdinals) {
                size += RamUsage.NUM_BYTES_INT * ordinal.length + RamUsage.NUM_BYTES_ARRAY_HEADER;
            }
            size += RamUsage.NUM_BYTES_ARRAY_HEADER + (RamUsage.NUM_BYTES_INT * lookup.length);
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
    public Docs ordinals() {
        return new Docs(this, lookup, storageOrdinals);
    }

    static class Docs implements Ordinals.Docs {

        private final SparseMultiArrayOrdinals parent;
        private final int[] lookup;
        private final int[][] ordinals;

        private final IterImpl iter;
        private final IntArrayRef intsScratch;

        public Docs(SparseMultiArrayOrdinals parent, int[] lookup, int[][] ordinals) {
            this.parent = parent;
            this.lookup = lookup;
            this.ordinals = ordinals;
            this.iter = new IterImpl(lookup, ordinals);
            this.intsScratch = new IntArrayRef(new int[parent.numOrds]);
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
        public boolean isMultiValued() {
            return true;
        }

        @Override
        public int getOrd(int docId) {
            int pointer = lookup[docId];
            if (pointer == 0) {
                return 0;
            } else if (pointer > 0) {
                return pointer;
            } else {
                pointer = -pointer;
                int allOrdsIndex = pointer >> parent.storageShift;
                int ordsIndex = (pointer & ((1 << parent.storageShift) - 1));
                return ordinals[allOrdsIndex][ordsIndex];
            }
        }

        @Override
        public IntArrayRef getOrds(int docId) {
            intsScratch.end = 0;
            int pointer = lookup[docId];
//            System.out.println("\nPointer: " + pointer);
            if (pointer == 0) {
                return IntArrayRef.EMPTY;
            } else if (pointer > 0) {
                intsScratch.end = 1;
                intsScratch.values[0] = pointer;
                return intsScratch;
            } else {
                pointer = -pointer;
                int allOrdsIndex = pointer >> parent.storageShift;
                int ordsIndex = (pointer & ((1 << parent.storageShift) - 1));
//                System.out.println("Storage index: " + allOrdsIndex);
//                System.out.println("Ordinal index: " + ordsIndex);

                int[] ords = ordinals[allOrdsIndex];

                int i = 0;
                int ord;
                while ((ord = ords[ordsIndex++]) > 0) {
                    intsScratch.values[i++] = ord;
                }
                intsScratch.values[i++] = -ord;
                intsScratch.end = i;

                return intsScratch;
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
                pointer = -pointer;
                int allOrdsIndex = pointer >> parent.storageShift;
                int ordsIndex = (pointer & ((1 << parent.storageShift) - 1));
                int[] ords = ordinals[allOrdsIndex];
                int i = ordsIndex;
                for (; ords[i] > 0; i++) {
                    proc.onOrdinal(docId, ords[i]);
                }
                proc.onOrdinal(docId, -ords[i]);
            }
        }

        class IterImpl implements Docs.Iter {

            private final int[] lookup;
            private final int[][] ordinals;

            private int pointer;

            private int allOrdsIndex;
            private int ordsIndex;

            private int ord;

            public IterImpl(int[] lookup, int[][] ordinals) {
                this.lookup = lookup;
                this.ordinals = ordinals;
            }

            public IterImpl reset(int docId) {
                pointer = lookup[docId];
                if (pointer < 0) {
                    int pointer = -this.pointer;
                    allOrdsIndex = pointer >> parent.storageShift;
                    ordsIndex = (pointer & ((1 << parent.storageShift) - 1));
                    ord = ordinals[allOrdsIndex][ordsIndex];
                } else {
                    ord = pointer;
                }
                return this;
            }

            @Override
            public int next() {
                if (ord <= 0) {
                    return 0;
                }

                if (pointer > 0) {
                    ord = 0;
                    return pointer;
                } else {
                    ord = ordinals[allOrdsIndex][ordsIndex++];
                    if (ord < 0) {
                        return -ord;
                    }
                    return ord;
                }
            }
        }
    }
}
