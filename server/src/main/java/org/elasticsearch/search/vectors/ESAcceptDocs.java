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

import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

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

    protected static BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc) throws IOException {
        if (liveDocs == null && iterator instanceof BitSetIterator bitSetIterator) {
            // If we already have a BitSet and no deletions, reuse the BitSet
            return bitSetIterator.getBitSet();
        } else {
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
    }

    private static BitSet sliceBitSet(BitSet bitSet, SliceAcceptDocs slice) {
        if (slice != null) {
            int startDoc = Math.max(0, slice.startDoc());
            int endDoc = Math.min(bitSet.length() - 1, slice.endDoc());
            bitSet.clear(0, startDoc);
            bitSet.clear(endDoc + 1, bitSet.length());
        }
        return bitSet;
    }

    public record SliceAcceptDocs(int startDoc, int endDoc) {
        public SliceAcceptDocs {
            if (startDoc < 0) {
                throw new IllegalArgumentException("startDoc must be non-negative");
            }
            if (endDoc < startDoc) {
                throw new IllegalArgumentException("endDoc must be greater than or equal to startDoc");
            }
        }
    }

    private static Bits sliceBits(Bits bits, SliceAcceptDocs slice) {
        if (slice == null) {
            return bits;
        }
        return new SliceBits(bits, slice);
    }

    private static DocIdSetIterator sliceIterator(DocIdSetIterator iterator, SliceAcceptDocs slice) {
        if (slice == null) {
            return iterator;
        }
        int startDoc = slice.startDoc();
        int endDoc = slice.endDoc();
        DocIdSetIterator sliceIterator = DocIdSetIterator.range(startDoc, endDoc + 1);
        return ConjunctionUtils.intersectIterators(List.of(iterator, sliceIterator));
    }

    private static int countBitsInRange(Bits bits, SliceAcceptDocs slice) {
        int maxDoc = bits.length() - 1;
        int startDoc = Math.max(0, slice.startDoc());
        int endDoc = Math.min(maxDoc, slice.endDoc());
        if (bits instanceof BitSet bitSet) {
            return countBitsInRange(bitSet, startDoc, endDoc);
        }
        int count = 0;
        for (int doc = startDoc; doc <= endDoc; doc++) {
            if (bits.get(doc)) {
                count++;
            }
        }
        return count;
    }

    private static int countBitsInRange(BitSet bitSet, int startDoc, int endDoc) {
        if (bitSet instanceof FixedBitSet fixedBitSet) {
            return fixedBitSet.cardinality(startDoc, endDoc + 1);
        }
        int count = 0;
        for (int doc = bitSet.nextSetBit(startDoc); doc != NO_MORE_DOCS && doc <= endDoc; doc = bitSet.nextSetBit(doc + 1)) {
            count++;
        }
        return count;
    }

    private static final class SliceBits implements Bits {
        private final Bits bits;
        private final int startDoc;
        private final int endDoc;

        private SliceBits(Bits bits, SliceAcceptDocs slice) {
            this.bits = bits;
            int maxDoc = bits.length() - 1;
            this.startDoc = Math.max(0, slice.startDoc());
            this.endDoc = Math.min(maxDoc, slice.endDoc());
        }

        @Override
        public boolean get(int index) {
            return index >= startDoc && index <= endDoc && bits.get(index);
        }

        @Override
        public int length() {
            return bits.length();
        }
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
        private Bits slicedBits;
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
            SliceAcceptDocs slice = sliceAcceptDocsOrNull();
            if (slice == null) {
                return bits;
            }
            if (slicedBits == null) {
                slicedBits = sliceBits(bits, slice);
            }
            return slicedBits;
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
        private final ScorerSupplier scorerSupplier;
        private BitSet acceptBitSet;
        private final Bits liveDocs;
        private final int maxDoc;
        private int cardinality = -1;

        public ScorerSupplierAcceptDocs(ScorerSupplier scorerSupplier, Bits liveDocs, int maxDoc) {
            this(scorerSupplier, liveDocs, maxDoc, -1, null);
        }

        public ScorerSupplierAcceptDocs(
            ScorerSupplier scorerSupplier,
            Bits liveDocs,
            int maxDoc,
            int sliceOrd,
            IOSupplier<SliceAcceptDocs> sliceAcceptDocsSupplier
        ) {
            super(sliceOrd, sliceAcceptDocsSupplier);
            this.scorerSupplier = scorerSupplier;
            this.liveDocs = liveDocs;
            this.maxDoc = maxDoc;
        }

        private void createBitSetIfNecessary(SliceAcceptDocs slice) throws IOException {
            if (acceptBitSet == null) {
                DocIdSetIterator iterator = scorerSupplier.get(NO_MORE_DOCS).iterator();
                if (liveDocs == null && iterator instanceof BitSetIterator) {
                    acceptBitSet = sliceBitSet(createBitSet(iterator, liveDocs, maxDoc), slice);
                } else {
                    iterator = sliceIterator(iterator, slice);
                    acceptBitSet = createBitSet(iterator, liveDocs, maxDoc);
                }
            }
        }

        @Override
        public Bits bits() throws IOException {
            SliceAcceptDocs slice = sliceAcceptDocsOrNull();
            createBitSetIfNecessary(slice);
            return acceptBitSet;
        }

        @Override
        public DocIdSetIterator iterator() throws IOException {
            if (acceptBitSet != null) {
                return new BitSetIterator(acceptBitSet, cardinality);
            }
            DocIdSetIterator iterator = liveDocs == null
                ? scorerSupplier.get(NO_MORE_DOCS).iterator()
                : new FilteredDocIdSetIterator(scorerSupplier.get(NO_MORE_DOCS).iterator()) {
                    @Override
                    protected boolean match(int doc) {
                        return liveDocs.get(doc);
                    }
                };
            return sliceIterator(iterator, sliceAcceptDocsOrNull());
        }

        @Override
        public int cost() throws IOException {
            SliceAcceptDocs slice = sliceAcceptDocsOrNull();
            createBitSetIfNecessary(slice);
            if (cardinality == -1) {
                cardinality = acceptBitSet.cardinality();
            }
            return cardinality;
        }

        @Override
        public int approximateCost() throws IOException {
            if (cardinality != -1) {
                return cardinality;
            }
            if (acceptBitSet != null) {
                return acceptBitSet.approximateCardinality();
            }
            SliceAcceptDocs slice = sliceAcceptDocsOrNull();
            if (slice != null) {
                long sliceCost = slice.endDoc() - slice.startDoc() + 1;
                return (int) Math.min(scorerSupplier.cost(), sliceCost);
            }
            return Math.toIntExact(scorerSupplier.cost());
        }
    }
}
