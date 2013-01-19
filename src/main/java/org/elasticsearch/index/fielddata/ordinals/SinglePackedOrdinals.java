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

import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.fielddata.util.IntArrayRef;

/**
 */
public class SinglePackedOrdinals implements Ordinals {

    // ordinals with value 0 indicates no value
    private final PackedInts.Reader reader;
    private final int numOrds;

    private long size = -1;

    public SinglePackedOrdinals(PackedInts.Reader reader, int numOrds) {
        this.reader = reader;
        this.numOrds = numOrds;
    }

    @Override
    public Object getBackingStorage() {
        if (reader.hasArray()) {
            return reader.getArray();
        }
        return reader;
    }

    @Override
    public long getMemorySizeInBytes() {
        if (size == -1) {
            size = RamUsage.NUM_BYTES_OBJECT_REF + reader.ramBytesUsed();
        }
        return size;
    }

    @Override
    public boolean isMultiValued() {
        return false;
    }

    @Override
    public int getNumDocs() {
        return reader.size();
    }

    @Override
    public int getNumOrds() {
        return numOrds;
    }

    @Override
    public Docs ordinals() {
        return new Docs(this, reader);
    }

    public static class Docs implements Ordinals.Docs {

        private final SinglePackedOrdinals parent;
        private final PackedInts.Reader reader;

        private final IntArrayRef intsScratch = new IntArrayRef(new int[1]);
        private final SingleValueIter iter = new SingleValueIter();

        public Docs(SinglePackedOrdinals parent, PackedInts.Reader reader) {
            this.parent = parent;
            this.reader = reader;
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
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public int getOrd(int docId) {
            return (int) reader.get(docId);
        }

        @Override
        public IntArrayRef getOrds(int docId) {
            int ordinal = (int) reader.get(docId);
            if (ordinal == 0) return IntArrayRef.EMPTY;
            intsScratch.values[0] = ordinal;
            return intsScratch;
        }

        @Override
        public Iter getIter(int docId) {
            return iter.reset((int) reader.get(docId));
        }

        @Override
        public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
            proc.onOrdinal(docId, (int) reader.get(docId));
        }
    }
}
