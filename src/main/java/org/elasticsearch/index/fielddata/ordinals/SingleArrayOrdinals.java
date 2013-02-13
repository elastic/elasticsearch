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

import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.fielddata.util.IntArrayRef;

/**
 */
public class SingleArrayOrdinals implements Ordinals {

    // ordinals with value 0 indicates no value
    private final int[] ordinals;
    private final int numOrds;
    private final int maxOrd;

    private long size = -1;

    public SingleArrayOrdinals(int[] ordinals, int numOrds) {
        this.ordinals = ordinals;
        this.numOrds = numOrds;
        this.maxOrd = numOrds + 1;
    }

    @Override
    public boolean hasSingleArrayBackingStorage() {
        return true;
    }

    @Override
    public Object getBackingStorage() {
        return ordinals;
    }

    @Override
    public long getMemorySizeInBytes() {
        if (size == -1) {
            size = RamUsage.NUM_BYTES_INT * ordinals.length + RamUsage.NUM_BYTES_ARRAY_HEADER;
        }
        return size;
    }

    @Override
    public boolean isMultiValued() {
        return false;
    }

    @Override
    public int getNumDocs() {
        return ordinals.length;
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
        return new Docs(this, ordinals);
    }

    public static class Docs implements Ordinals.Docs {

        private final SingleArrayOrdinals parent;
        private final int[] ordinals;

        private final IntArrayRef intsScratch = new IntArrayRef(new int[1]);
        private final SingleValueIter iter = new SingleValueIter();

        public Docs(SingleArrayOrdinals parent, int[] ordinals) {
            this.parent = parent;
            this.ordinals = ordinals;
        }

        @Override
        public Ordinals ordinals() {
            return parent;
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
            return false;
        }

        @Override
        public int getOrd(int docId) {
            return ordinals[docId];
        }

        @Override
        public IntArrayRef getOrds(int docId) {
            int ordinal = ordinals[docId];
            if (ordinal == 0) return IntArrayRef.EMPTY;
            intsScratch.values[0] = ordinal;
            return intsScratch;
        }

        @Override
        public Iter getIter(int docId) {
            return iter.reset(ordinals[docId]);
        }

        @Override
        public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
            proc.onOrdinal(docId, ordinals[docId]);
        }
    }
}
