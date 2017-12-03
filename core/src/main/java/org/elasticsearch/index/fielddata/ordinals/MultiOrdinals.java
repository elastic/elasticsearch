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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.index.fielddata.AbstractSortedDocValues;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link Ordinals} implementation which is efficient at storing field data ordinals for multi-valued or sparse fields.
 */
public class MultiOrdinals extends Ordinals {

    private static final int OFFSETS_PAGE_SIZE = 1024;

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
    private final long valueCount;
    private final PackedLongValues endOffsets;
    private final PackedLongValues ords;

    public MultiOrdinals(OrdinalsBuilder builder, float acceptableOverheadRatio) {
        multiValued = builder.getNumMultiValuesDocs() > 0;
        valueCount = builder.getValueCount();
        PackedLongValues.Builder endOffsetsBuilder = PackedLongValues.monotonicBuilder(OFFSETS_PAGE_SIZE, acceptableOverheadRatio);
        PackedLongValues.Builder ordsBuilder = PackedLongValues.packedBuilder(OFFSETS_PAGE_SIZE, acceptableOverheadRatio);
        long lastEndOffset = 0;
        for (int i = 0; i < builder.maxDoc(); ++i) {
            final LongsRef docOrds = builder.docOrds(i);
            final long endOffset = lastEndOffset + docOrds.length;
            endOffsetsBuilder.add(endOffset);
            for (int j = 0; j < docOrds.length; ++j) {
                ordsBuilder.add(docOrds.longs[docOrds.offset + j]);
            }
            lastEndOffset = endOffset;
        }
        endOffsets = endOffsetsBuilder.build();
        ords = ordsBuilder.build();
        assert endOffsets.size() == builder.maxDoc();
        assert ords.size() == builder.getTotalNumOrds() : ords.size() + " != " + builder.getTotalNumOrds();
    }

    @Override
    public long ramBytesUsed() {
        return endOffsets.ramBytesUsed() + ords.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        List<Accountable> resources = new ArrayList<>();
        resources.add(Accountables.namedAccountable("offsets", endOffsets));
        resources.add(Accountables.namedAccountable("ordinals", ords));
        return Collections.unmodifiableCollection(resources);
    }

    @Override
    public SortedSetDocValues ordinals(ValuesHolder values) {
        if (multiValued) {
            return new MultiDocs(this, values);
        } else {
            return (SortedSetDocValues) DocValues.singleton(new SingleDocs(this, values));
        }
    }

    private static class SingleDocs extends AbstractSortedDocValues {

        private final int valueCount;
        private final PackedLongValues endOffsets;
        private final PackedLongValues ords;
        private final ValuesHolder values;

        private int currentDoc = -1;
        private long currentStartOffset;
        private long currentEndOffset;

        SingleDocs(MultiOrdinals ordinals, ValuesHolder values) {
            this.valueCount = (int) ordinals.valueCount;
            this.endOffsets = ordinals.endOffsets;
            this.ords = ordinals.ords;
            this.values = values;
        }

        @Override
        public int ordValue() {
            return (int) ords.get(currentStartOffset);
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            currentDoc = docId;
            currentStartOffset = docId != 0 ? endOffsets.get(docId - 1) : 0;
            currentEndOffset = endOffsets.get(docId);
            return currentStartOffset != currentEndOffset;
        }

        @Override
        public int docID() {
            return currentDoc;
        }

        @Override
        public BytesRef lookupOrd(int ord) {
            return values.lookupOrd(ord);
        }

        @Override
        public int getValueCount() {
            return valueCount;
        }

    }

    private static class MultiDocs extends AbstractSortedSetDocValues {

        private final long valueCount;
        private final PackedLongValues endOffsets;
        private final PackedLongValues ords;
        private final ValuesHolder values;

        private long currentOffset;
        private long currentEndOffset;

        MultiDocs(MultiOrdinals ordinals, ValuesHolder values) {
            this.valueCount = ordinals.valueCount;
            this.endOffsets = ordinals.endOffsets;
            this.ords = ordinals.ords;
            this.values = values;
        }

        @Override
        public long getValueCount() {
            return valueCount;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            currentOffset = docId != 0 ? endOffsets.get(docId - 1) : 0;
            currentEndOffset = endOffsets.get(docId);
            return currentOffset != currentEndOffset;
        }

        @Override
        public long nextOrd() throws IOException {
            if (currentOffset == currentEndOffset) {
                return SortedSetDocValues.NO_MORE_ORDS;
            } else {
                return ords.get(currentOffset++);
            }
        }

        @Override
        public BytesRef lookupOrd(long ord) {
            return values.lookupOrd(ord);
        }
    }
}
