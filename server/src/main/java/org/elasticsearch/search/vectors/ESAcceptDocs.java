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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public abstract class ESAcceptDocs extends AcceptDocs {

    public abstract int approximateCost() throws IOException;

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

    public static class AllDocs extends ESAcceptDocs {

        public static final AllDocs INSTANCE = new AllDocs();

        private AllDocs() {}

        @Override
        public Bits bits() {
            return null;
        }

        @Override
        public DocIdSetIterator iterator() {
            return null;
        }

        @Override
        public int cost() {
            return -1;
        }

        @Override
        public int approximateCost() {
            return -1;
        }
    }

    public static class BitsAcceptDocs extends ESAcceptDocs {
        private final Bits bits;
        private final int maxDoc;
        private final int approximateCost;

        BitsAcceptDocs(Bits bits, int maxDoc) {
            if (bits != null && bits.length() != maxDoc) {
                throw new IllegalArgumentException("Bits length = " + bits.length() + " != maxDoc = " + maxDoc);
            }
            this.bits = bits;
            if (bits instanceof BitSet bitSet) {
                this.maxDoc = Objects.requireNonNull(bitSet).cardinality();
                this.approximateCost = Objects.requireNonNull(bitSet).approximateCardinality();
            } else {
                this.maxDoc = maxDoc;
                this.approximateCost = maxDoc;
            }
        }

        @Override
        public Bits bits() {
            return bits;
        }

        @Override
        public DocIdSetIterator iterator() {
            if (bits instanceof BitSet bitSet) {
                return new BitSetIterator(bitSet, maxDoc);
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
    }

    public static class ScorerSupplierAcceptDocs extends ESAcceptDocs {
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
    }

}
