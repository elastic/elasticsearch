/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.search.AbstractDocIdSetIterator;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.MathUtil;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * An extension of {@link AcceptDocs} that provides additional methods to get an approximate cost
 * and a BitSet representation of the accepted documents.
 */
public abstract sealed class ESAcceptDocs extends AcceptDocs {

    private final int sliceOrd;
    private final IOSupplier<SliceAcceptDocs> sliceAcceptDocsSupplier;
    private SliceAcceptDocs sliceAcceptDocsCache;

    protected ESAcceptDocs(int sliceOrd, IOSupplier<SliceAcceptDocs> sliceAcceptDocsSupplier) {
        this.sliceOrd = sliceOrd;
        this.sliceAcceptDocsSupplier = sliceAcceptDocsSupplier;
    }

    /** Returns an approximate cost of the accepted documents.
     * This is generally much cheaper than {@link #cost()}, as implementations may
     * not fully evaluate filters to provide this estimate and may ignore deletions
     * @return the approximate cost
     * @throws IOException if an I/O error occurs
     */
    public abstract int approximateCost() throws IOException;

    public final int sliceOrd() {
        return sliceOrd;
    }

    public final SliceAcceptDocs sliceAcceptDocs() throws IOException {
        return Objects.requireNonNull(sliceAcceptDocsOrNull(), "sliceAcceptDocs is not available");
    }

    protected final SliceAcceptDocs sliceAcceptDocsOrNull() throws IOException {
        if (sliceOrd < 0 || sliceAcceptDocsSupplier == null) {
            return null;
        }
        if (sliceAcceptDocsCache == null) {
            sliceAcceptDocsCache = sliceAcceptDocsSupplier.get();
        }
        return sliceAcceptDocsCache;
    }

    static BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc) throws IOException {
        int threshold = maxDoc >> 7; // same as BitSet#of
        if (iterator.cost() >= threshold) {
            FixedBitSet bitSet = new FixedBitSet(maxDoc);
            bitSet.or(iterator);
            if (liveDocs != null) {
                liveDocs.applyMask(bitSet, 0);
            }
            return bitSet;
        } else {
            return BitSet.of(liveDocs == null ? iterator : new FilteredDocIdSetIterator(iterator) {
                @Override
                protected boolean match(int doc) {
                    return liveDocs.get(doc);
                }
            }, maxDoc); // create a sparse bitset
        }
    }

    static BitSet createSliceBitSet(DocIdSetIterator iterator, Bits liveDocs, SliceAcceptDocs acceptDocs) throws IOException {
        FixedBitSet bitSet = new FixedBitSet(acceptDocs.length());
        if (iterator.docID() < acceptDocs.startDoc()) {
            iterator.advance(acceptDocs.startDoc());
        }
        iterator.intoBitSet(acceptDocs.endDoc(), bitSet, acceptDocs.startDoc());
        if (liveDocs != null) {
            liveDocs.applyMask(bitSet, acceptDocs.startDoc());
        }
        return bitSet;
    }

    /** A simple record to represent the range of documents accepted by a slice, from {@code starDoc}
     * inclusive to {@code endDoc} exclusive */
    public record SliceAcceptDocs(int startDoc, int endDoc) {
        public SliceAcceptDocs {
            if (startDoc < 0) {
                throw new IllegalArgumentException("startDoc must be non-negative");
            }
            if (endDoc <= startDoc) {
                throw new IllegalArgumentException("endDoc must be greater than startDoc");
            }
        }

        public int length() {
            return endDoc - startDoc;
        }
    }

    private static DocIdSetIterator sliceIterator(DocIdSetIterator iterator, SliceAcceptDocs slice) throws IOException {
        if (slice == null) {
            return iterator;
        }
        return new SliceIterator(iterator, slice);
    }

    private static class SliceIterator extends DocIdSetIterator {
        private final DocIdSetIterator iterator;
        private final SliceAcceptDocs slice;
        private int docID = -1;

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            if (docID < slice.startDoc()) {
                advance(slice.startDoc());
            }
            if (docID != NO_MORE_DOCS) {
                upTo = Math.min(upTo, slice.endDoc());
                iterator.intoBitSet(upTo, bitSet, offset);
                docID = iterator.docID() >= slice.endDoc() ? NO_MORE_DOCS : iterator.docID();
            }
        }

        private SliceIterator(DocIdSetIterator iterator, SliceAcceptDocs slice) {
            this.iterator = iterator;
            this.slice = slice;
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(docID + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            assert target >= docID;
            if (target >= slice.endDoc()) {
                docID = NO_MORE_DOCS;
                return docID;
            }
            target = Math.max(target, slice.startDoc());
            if (target > iterator.docID()) {
                docID = iterator.advance(target);
                if (docID >= slice.endDoc()) {
                    docID = NO_MORE_DOCS;
                }
            } else {
                docID = iterator.docID();
            }
            return docID;
        }

        @Override
        public long cost() {
            return Math.min(iterator.cost(), slice.length());
        }
    }

    private static int countBitsInRange(Bits bits, SliceAcceptDocs slice) {
        int startDoc = Math.max(0, slice.startDoc());
        int endDoc = Math.min(bits.length(), slice.endDoc());
        if (bits instanceof BitSet bitSet) {
            return countBitsInRange(bitSet, startDoc, endDoc);
        }
        int count = 0;
        for (int doc = startDoc; doc < endDoc; doc++) {
            if (bits.get(doc)) {
                count++;
            }
        }
        return count;
    }

    private static int countBitsInRange(BitSet bitSet, int startDoc, int endDoc) {
        if (bitSet instanceof FixedBitSet fixedBitSet) {
            return fixedBitSet.cardinality(startDoc, endDoc);
        }
        int count = 0;
        for (int doc = bitSet.nextSetBit(startDoc); doc != NO_MORE_DOCS && doc <= endDoc; doc = bitSet.nextSetBit(doc + 1)) {
            count++;
        }
        return count;
    }

    /** An AcceptDocs that accepts all documents. */
    public static final class ESAcceptDocsAll extends ESAcceptDocs {

        public ESAcceptDocsAll() {
            this(-1, null);
        }

        public ESAcceptDocsAll(int sliceOrd, IOSupplier<SliceAcceptDocs> sliceAcceptDocsSupplier) {
            super(sliceOrd, sliceAcceptDocsSupplier);
        }

        @Override
        public int approximateCost() {
            return 0;
        }

        @Override
        public Bits bits() throws IOException {
            return null;
        }

        @Override
        public DocIdSetIterator iterator() throws IOException {
            return null;
        }

        @Override
        public int cost() throws IOException {
            return 0;
        }
    }

    /** An AcceptDocs that wraps a Bits instance. Generally indicates that no filter was provided, but there are deleted docs */
    public static final class BitsAcceptDocs extends ESAcceptDocs {
        private final Bits bits;
        private final BitSet bitSetRef;
        private final int maxDoc;
        private final int approximateCost;
        private int sliceCost = -1;

        public BitsAcceptDocs(Bits bits, int maxDoc) {
            this(bits, maxDoc, -1, null);
        }

        public BitsAcceptDocs(Bits bits, int maxDoc, int sliceOrd, IOSupplier<SliceAcceptDocs> sliceAcceptDocsSupplier) {
            super(sliceOrd, sliceAcceptDocsSupplier);
            if (bits != null && bits.length() != maxDoc) {
                throw new IllegalArgumentException("Bits length = " + bits.length() + " != maxDoc = " + maxDoc);
            }
            this.bits = Objects.requireNonNull(bits);
            if (bits instanceof BitSet bitSet) {
                this.maxDoc = Objects.requireNonNull(bitSet).cardinality();
                this.approximateCost = Objects.requireNonNull(bitSet).approximateCardinality();
                this.bitSetRef = bitSet;
            } else {
                this.maxDoc = maxDoc;
                this.approximateCost = maxDoc;
                this.bitSetRef = null;
            }
        }

        @Override
        public Bits bits() throws IOException {
            // no special handling for slices
            return bits;
        }

        @Override
        public DocIdSetIterator iterator() throws IOException {
            DocIdSetIterator iterator;
            if (bitSetRef != null) {
                iterator = new BitSetIterator(bitSetRef, maxDoc);
            } else {
                iterator = new FilteredDocIdSetIterator(DocIdSetIterator.all(maxDoc)) {
                    @Override
                    protected boolean match(int doc) {
                        return bits.get(doc);
                    }
                };
            }
            return sliceIterator(iterator, sliceAcceptDocsOrNull());
        }

        private int sliceCost() throws IOException {
            if (sliceCost != -1) {
                return sliceCost;
            }
            SliceAcceptDocs slice = sliceAcceptDocsOrNull();
            if (slice == null) {
                return maxDoc;
            }
            sliceCost = countBitsInRange(bits, slice);
            return sliceCost;
        }

        @Override
        public int cost() throws IOException {
            SliceAcceptDocs slice = sliceAcceptDocsOrNull();
            if (slice == null) {
                // We have no better estimate. This should be ok in practice since background merges should
                // keep the number of deletes under control (< 20% by default).
                return maxDoc;
            }
            return sliceCost();
        }

        @Override
        public int approximateCost() throws IOException {
            SliceAcceptDocs slice = sliceAcceptDocsOrNull();
            if (slice == null) {
                return approximateCost;
            }
            return sliceCost();
        }
    }

    /** An AcceptDocs that wraps a ScorerSupplier. Indicates that a filter was provided. */
    public static final class ScorerSupplierAcceptDocs extends ESAcceptDocs {
        private final IOSupplier<DocIdSetIterator> docIdIteratorSupplier;
        private final LongSupplier costSupplier;
        private BitSet acceptBitSet;
        private final Bits liveDocs;
        private final int maxDoc;
        private int cardinality = -1;

        public ScorerSupplierAcceptDocs(
            IOSupplier<DocIdSetIterator> docIdIteratorSupplier,
            LongSupplier costSupplier,
            Bits liveDocs,
            int maxDoc
        ) {
            this(docIdIteratorSupplier, costSupplier, liveDocs, maxDoc, -1, null);
        }

        public ScorerSupplierAcceptDocs(
            IOSupplier<DocIdSetIterator> docIdIteratorSupplier,
            LongSupplier costSupplier,
            Bits liveDocs,
            int maxDoc,
            int sliceOrd,
            IOSupplier<SliceAcceptDocs> sliceAcceptDocsSupplier
        ) {
            super(sliceOrd, sliceAcceptDocsSupplier);
            this.docIdIteratorSupplier = docIdIteratorSupplier;
            this.costSupplier = costSupplier;
            this.liveDocs = liveDocs;
            this.maxDoc = maxDoc;
        }

        private void createBitSetIfNecessary(SliceAcceptDocs slice) throws IOException {
            if (acceptBitSet == null) {
                DocIdSetIterator iterator = docIdIteratorSupplier.get();
                if (liveDocs == null && iterator instanceof BitSetIterator bitSetIterator) {
                    acceptBitSet = bitSetIterator.getBitSet();
                    assert acceptBitSet.length() == maxDoc;
                } else if (slice == null) {
                    acceptBitSet = createBitSet(iterator, liveDocs, maxDoc);
                } else {
                    acceptBitSet = createSliceBitSet(iterator, liveDocs, slice);
                }
            }
        }

        @Override
        public Bits bits() throws IOException {
            SliceAcceptDocs slice = sliceAcceptDocsOrNull();
            createBitSetIfNecessary(slice);
            if (slice != null && acceptBitSet.length() == slice.length()) {
                return new Bits() {
                    @Override
                    public boolean get(int index) {
                        return index >= slice.startDoc() && index < slice.endDoc() && acceptBitSet.get(index - slice.startDoc());
                    }

                    @Override
                    public int length() {
                        return maxDoc;
                    }
                };
            }
            return acceptBitSet;
        }

        @Override
        public DocIdSetIterator iterator() throws IOException {
            if (acceptBitSet != null) {
                SliceAcceptDocs acceptDocs = sliceAcceptDocsOrNull();
                if (acceptDocs != null && acceptDocs.length() == acceptBitSet.length()) {
                    return new SliceBitSetIterator(acceptBitSet, cost(), acceptDocs);
                } else {
                    return sliceIterator(new BitSetIterator(acceptBitSet, cost()), acceptDocs);
                }
            }
            DocIdSetIterator iterator = liveDocs == null
                ? docIdIteratorSupplier.get()
                : new FilteredDocIdSetIterator(docIdIteratorSupplier.get()) {
                    @Override
                    protected boolean match(int doc) {
                        return liveDocs.get(doc);
                    }
                };
            return sliceIterator(iterator, sliceAcceptDocsOrNull());
        }

        @Override
        public int cost() throws IOException {
            if (cardinality == -1) {
                SliceAcceptDocs slice = sliceAcceptDocsOrNull();
                createBitSetIfNecessary(slice);
                if (slice != null && acceptBitSet.length() == maxDoc) {
                    assert acceptBitSet instanceof FixedBitSet;
                    cardinality = ((FixedBitSet) acceptBitSet).cardinality(slice.startDoc(), slice.endDoc());
                } else {
                    cardinality = acceptBitSet.cardinality();
                }
            }
            return cardinality;
        }

        @Override
        public int approximateCost() throws IOException {
            if (cardinality != -1) {
                return cardinality;
            }
            SliceAcceptDocs slice = sliceAcceptDocsOrNull();
            int maxCost = slice == null ? maxDoc : slice.length();
            int approxCost = acceptBitSet == null ? (int) costSupplier.getAsLong() : acceptBitSet.approximateCardinality();
            return Math.min(approxCost, maxCost);
        }
    }

    static class SliceBitSetIterator extends AbstractDocIdSetIterator {

        private final BitSet bits;
        private final SliceAcceptDocs acceptDocs;
        private final long cost;

        SliceBitSetIterator(BitSet bits, long cost, SliceAcceptDocs acceptDocs) {
            assert cost >= 0;
            assert acceptDocs.length() == bits.length();
            this.bits = bits;
            this.cost = cost;
            this.acceptDocs = acceptDocs;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            if (target >= acceptDocs.endDoc) {
                return doc = NO_MORE_DOCS;
            }
            target = Math.max(target - acceptDocs.startDoc, 0);
            doc = bits.nextSetBit(target);
            if (doc != NO_MORE_DOCS) {
                doc += acceptDocs.startDoc();
                assert doc < acceptDocs.endDoc();
            }
            return doc;
        }

        @Override
        public long cost() {
            return cost;
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            if (upTo > doc && bits instanceof FixedBitSet fixedBits) {
                int actualUpto = Math.min(upTo, acceptDocs.endDoc());
                // The destination bit set may be shorter than this bit set. This is only legal if all bits
                // beyond offset + bitSet.length() are clear. If not, the below call to `super.intoBitSet`
                // will throw an exception.
                actualUpto = MathUtil.unsignedMin(actualUpto, offset + acceptDocs.endDoc());
                FixedBitSet.orRange(fixedBits, doc - acceptDocs.startDoc(), bitSet, doc - offset, actualUpto - doc);
                advance(actualUpto); // set the current doc
            }
            super.intoBitSet(upTo, bitSet, offset);

        }
    }
}
