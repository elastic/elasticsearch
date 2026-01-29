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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * An extension of {@link AcceptDocs} that provides additional methods to get an approximate cost
 * and a BitSet representation of the accepted documents.
 */
public abstract sealed class ESAcceptDocs extends AcceptDocs {

    /** Returns an approximate cost of the accepted documents.
     * This is generally much cheaper than {@link #cost()}, as implementations may
     * not fully evaluate filters to provide this estimate and may ignore deletions
     * @return the approximate cost
     * @throws IOException if an I/O error occurs
     */
    public abstract int approximateCost() throws IOException;

    /**
     * Returns an optional BitSet representing the accepted documents.
     * If a BitSet representation is not available, returns an empty Optional. An empty optional indicates that
     * there are some accepted documents, but they cannot be represented as a BitSet efficiently.
     * Null implies that all documents are accepted.
     * @return an Optional containing the BitSet of accepted documents, or empty if not available, or null if all documents are accepted
     * @throws IOException if an I/O error occurs
     */
    public abstract Optional<BitSet> getBitSet() throws IOException;

    private static BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc) throws IOException {
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

    /** An AcceptDocs that accepts all documents. */
    public static final class ESAcceptDocsAll extends ESAcceptDocs {
        public static final ESAcceptDocsAll INSTANCE = new ESAcceptDocsAll();

        private ESAcceptDocsAll() {}

        @Override
        public int approximateCost() throws IOException {
            return 0;
        }

        @Override
        public Optional<BitSet> getBitSet() throws IOException {
            return null;
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

        BitsAcceptDocs(Bits bits, int maxDoc) {
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
        public Bits bits() {
            return bits;
        }

        @Override
        public DocIdSetIterator iterator() {
            if (bitSetRef != null) {
                return new BitSetIterator(bitSetRef, maxDoc);
            }
            return new FilteredDocIdSetIterator(DocIdSetIterator.all(maxDoc)) {
                @Override
                protected boolean match(int doc) {
                    return bits.get(doc);
                }
            };
        }

        @Override
        public int cost() {
            // We have no better estimate. This should be ok in practice since background merges should
            // keep the number of deletes under control (< 20% by default).
            return maxDoc;
        }

        @Override
        public int approximateCost() {
            return approximateCost;
        }

        @Override
        public Optional<BitSet> getBitSet() {
            if (bits == null) {
                return null;
            }
            return Optional.ofNullable(bitSetRef);
        }
    }

    /** An AcceptDocs that wraps a ScorerSupplier. Indicates that a filter was provided. */
    public static final class ScorerSupplierAcceptDocs extends ESAcceptDocs {
        private final ScorerSupplier scorerSupplier;
        private BitSet acceptBitSet;
        private final Bits liveDocs;
        private final int maxDoc;
        private int cardinality = -1;

        ScorerSupplierAcceptDocs(ScorerSupplier scorerSupplier, Bits liveDocs, int maxDoc) {
            this.scorerSupplier = scorerSupplier;
            this.liveDocs = liveDocs;
            this.maxDoc = maxDoc;
        }

        private void createBitSetIfNecessary() throws IOException {
            if (acceptBitSet == null) {
                acceptBitSet = createBitSet(scorerSupplier.get(NO_MORE_DOCS).iterator(), liveDocs, maxDoc);
            }
        }

        @Override
        public Bits bits() throws IOException {
            createBitSetIfNecessary();
            return acceptBitSet;
        }

        @Override
        public DocIdSetIterator iterator() throws IOException {
            if (acceptBitSet != null) {
                return new BitSetIterator(acceptBitSet, cardinality);
            }
            return liveDocs == null
                ? scorerSupplier.get(NO_MORE_DOCS).iterator()
                : new FilteredDocIdSetIterator(scorerSupplier.get(NO_MORE_DOCS).iterator()) {
                    @Override
                    protected boolean match(int doc) {
                        return liveDocs.get(doc);
                    }
                };
        }

        @Override
        public int cost() throws IOException {
            createBitSetIfNecessary();
            if (cardinality == -1) {
                cardinality = acceptBitSet.cardinality();
            }
            return cardinality;
        }

        @Override
        public int approximateCost() throws IOException {
            if (acceptBitSet != null) {
                return cardinality != -1 ? cardinality : acceptBitSet.approximateCardinality();
            }
            return Math.toIntExact(scorerSupplier.cost());
        }

        @Override
        public Optional<BitSet> getBitSet() throws IOException {
            createBitSetIfNecessary();
            return Optional.of(acceptBitSet);
        }
    }
}
