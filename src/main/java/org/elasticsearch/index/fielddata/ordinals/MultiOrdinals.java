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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.packed.AppendingPackedLongBuffer;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.BytesValues.WithOrdinals;

/**
 * {@link Ordinals} implementation which is efficient at storing field data ordinals for multi-valued or sparse fields.
 */
public class MultiOrdinals extends Ordinals {

    private static final int OFFSETS_PAGE_SIZE = 1024;
    private static final int OFFSET_INIT_PAGE_COUNT = 16;

    /**
     * Return true if this impl is going to be smaller than {@link SinglePackedOrdinals} by at least 20%.
     */
    public static boolean significantlySmallerThanSinglePackedOrdinals(int maxDoc, int numDocsWithValue, long numOrds, float acceptableOverheadRatio) {
        int bitsPerOrd = PackedInts.bitsRequired(numOrds);
        bitsPerOrd = PackedInts.fastestFormatAndBits(numDocsWithValue, bitsPerOrd, acceptableOverheadRatio).bitsPerValue;
        // Compute the worst-case number of bits per value for offsets in the worst case, eg. if no docs have a value at the
        // beginning of the block and all docs have one at the end of the block
        final float avgValuesPerDoc = (float) numDocsWithValue / maxDoc;
        final int maxDelta = (int) Math.ceil(OFFSETS_PAGE_SIZE * (1 - avgValuesPerDoc) * avgValuesPerDoc);
        int bitsPerOffset = PackedInts.bitsRequired(maxDelta) + 1; // +1 because of the sign
        bitsPerOffset = PackedInts.fastestFormatAndBits(maxDoc, bitsPerOffset, acceptableOverheadRatio).bitsPerValue;

        final long expectedMultiSizeInBytes = (long) numDocsWithValue * bitsPerOrd + (long) maxDoc * bitsPerOffset;
        final long expectedSingleSizeInBytes = (long) maxDoc * bitsPerOrd;
        return expectedMultiSizeInBytes < 0.8f * expectedSingleSizeInBytes;
    }

    private final boolean multiValued;
    private final long maxOrd;
    private final MonotonicAppendingLongBuffer endOffsets;
    private final AppendingPackedLongBuffer ords;

    public MultiOrdinals(OrdinalsBuilder builder, float acceptableOverheadRatio) {
        multiValued = builder.getNumMultiValuesDocs() > 0;
        maxOrd = builder.getMaxOrd();
        endOffsets = new MonotonicAppendingLongBuffer(OFFSET_INIT_PAGE_COUNT, OFFSETS_PAGE_SIZE, acceptableOverheadRatio);
        ords = new AppendingPackedLongBuffer(OFFSET_INIT_PAGE_COUNT, OFFSETS_PAGE_SIZE, acceptableOverheadRatio);
        long lastEndOffset = 0;
        for (int i = 0; i < builder.maxDoc(); ++i) {
            final LongsRef docOrds = builder.docOrds(i);
            final long endOffset = lastEndOffset + docOrds.length;
            endOffsets.add(endOffset);
            for (int j = 0; j < docOrds.length; ++j) {
                ords.add(docOrds.longs[docOrds.offset + j]);
            }
            lastEndOffset = endOffset;
        }
        assert endOffsets.size() == builder.maxDoc();
        assert ords.size() == builder.getTotalNumOrds() : ords.size() + " != " + builder.getTotalNumOrds();
    }

    @Override
    public long ramBytesUsed() {
        return endOffsets.ramBytesUsed() + ords.ramBytesUsed();
    }

    @Override
    public WithOrdinals ordinals(ValuesHolder values) {
        return new MultiDocs(this, values);
    }

    public static class MultiDocs extends BytesValues.WithOrdinals {

        private final long maxOrd;
        private final MonotonicAppendingLongBuffer endOffsets;
        private final AppendingPackedLongBuffer ords;
        private long offset;
        private long limit;
        private final ValuesHolder values;

        MultiDocs(MultiOrdinals ordinals, ValuesHolder values) {
            super(ordinals.multiValued);
            this.maxOrd = ordinals.maxOrd;
            this.endOffsets = ordinals.endOffsets;
            this.ords = ordinals.ords;
            this.values = values;
        }

        @Override
        public long getMaxOrd() {
            return maxOrd;
        }

        @Override
        public long getOrd(int docId) {
            final long startOffset = docId > 0 ? endOffsets.get(docId - 1) : 0;
            final long endOffset = endOffsets.get(docId);
            if (startOffset == endOffset) {
                return MISSING_ORDINAL; // ord for missing values
            } else {
                return ords.get(startOffset);
            }
        }

        @Override
        public long nextOrd() {
            assert offset < limit;
            return ords.get(offset++);
        }

        @Override
        public int setDocument(int docId) {
            final long startOffset = docId > 0 ? endOffsets.get(docId - 1) : 0;
            final long endOffset = endOffsets.get(docId);
            offset = startOffset;
            limit = endOffset;
            return (int) (endOffset - startOffset);
        }

        @Override
        public BytesRef getValueByOrd(long ord) {
            return values.getValueByOrd(ord);
        }
    }
}
