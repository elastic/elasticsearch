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

import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

/**
 */
public class SinglePackedOrdinals implements Ordinals {

    // ordinals with value 0 indicates no value
    private final PackedInts.Reader reader;
    private final long numOrds;
    private final long maxOrd;

    private long size = -1;

    public SinglePackedOrdinals(OrdinalsBuilder builder, float acceptableOverheadRatio) {
        assert builder.getNumMultiValuesDocs() == 0;
        this.numOrds = builder.getNumOrds();
        this.maxOrd = builder.getNumOrds() + 1;
        // We don't reuse the builder as-is because it might have been built with a higher overhead ratio
        final PackedInts.Mutable reader = PackedInts.getMutable(builder.maxDoc(), PackedInts.bitsRequired(getNumOrds()), acceptableOverheadRatio);
        PackedInts.copy(builder.getFirstOrdinals(), 0, reader, 0, builder.maxDoc(), 8 * 1024);
        this.reader = reader;
    }

    @Override
    public long getMemorySizeInBytes() {
        if (size == -1) {
            size = RamUsageEstimator.NUM_BYTES_OBJECT_REF + reader.ramBytesUsed();
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
    public long getNumOrds() {
        return numOrds;
    }

    @Override
    public long getMaxOrd() {
        return maxOrd;
    }

    @Override
    public Docs ordinals() {
        return new Docs(this, reader);
    }

    public static class Docs implements Ordinals.Docs {

        private final SinglePackedOrdinals parent;
        private final PackedInts.Reader reader;

        private final LongsRef longsScratch = new LongsRef(1);
        private long currentOrdinal;

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
        public long getNumOrds() {
            return parent.getNumOrds();
        }

        @Override
        public long getMaxOrd() {
            return parent.getMaxOrd();
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public long getOrd(int docId) {
            return currentOrdinal = reader.get(docId);
        }

        @Override
        public LongsRef getOrds(int docId) {
            final long ordinal = reader.get(docId);
            longsScratch.offset = 0;
            longsScratch.length = (int)Math.min(currentOrdinal, 1);
            longsScratch.longs[0] = currentOrdinal = ordinal;
            return longsScratch;
        }

        @Override
        public long nextOrd() {
            assert currentOrdinal > 0;
            return currentOrdinal;
        }

        @Override
        public int setDocument(int docId) {
            currentOrdinal = reader.get(docId);
            // either this is > 1 or 0 - in any case it prevents a branch!
            return (int)Math.min(currentOrdinal, 1);
        }

        @Override
        public long currentOrd() {
            return currentOrdinal;
        }
    }
}
