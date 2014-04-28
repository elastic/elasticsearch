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

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

/**
 */
public class SinglePackedOrdinals implements Ordinals {

    // ordinals with value 0 indicates no value
    private final PackedInts.Reader reader;
    private final long maxOrd;

    private long size = -1;

    public SinglePackedOrdinals(OrdinalsBuilder builder, float acceptableOverheadRatio) {
        assert builder.getNumMultiValuesDocs() == 0;
        this.maxOrd = builder.getMaxOrd();
        // We don't reuse the builder as-is because it might have been built with a higher overhead ratio
        final PackedInts.Mutable reader = PackedInts.getMutable(builder.maxDoc(), PackedInts.bitsRequired(getMaxOrd()), acceptableOverheadRatio);
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
    public long getMaxOrd() {
        return maxOrd;
    }

    @Override
    public Docs ordinals() {
        return new Docs(this, reader);
    }

    public static class Docs extends Ordinals.AbstractDocs {

        private final PackedInts.Reader reader;

        private long currentOrdinal;

        public Docs(SinglePackedOrdinals parent, PackedInts.Reader reader) {
            super(parent);
            this.reader = reader;
        }

        @Override
        public long getOrd(int docId) {
            return currentOrdinal = reader.get(docId) - 1;
        }

        @Override
        public long nextOrd() {
            assert currentOrdinal >= Ordinals.MIN_ORDINAL;
            return currentOrdinal;
        }

        @Override
        public int setDocument(int docId) {
            currentOrdinal = reader.get(docId) - 1;
            // either this is > 1 or 0 - in any case it prevents a branch!
            return 1 + (int) Math.min(currentOrdinal, 0);
        }

        @Override
        public long currentOrd() {
            return currentOrdinal;
        }
    }
}
