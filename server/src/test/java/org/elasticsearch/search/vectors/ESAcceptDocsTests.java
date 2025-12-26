/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ESAcceptDocsTests extends ESTestCase {

    public void testAcceptAllDocs() throws IOException {
        ESAcceptDocs acceptDocs = ESAcceptDocs.ESAcceptDocsAll.INSTANCE;
        assertEquals(0L, acceptDocs.approximateCost());
        assertEquals(0L, acceptDocs.cost());
        assertNull(acceptDocs.iterator());
        assertNull(acceptDocs.bits());
        assertNull(acceptDocs.getBitSet());
    }

    public void testFromScorerSupplier() throws IOException {
        int[] docIds = new int[] { 1, 3, 5, 7, 9 };
        BitSet bitSet = new FixedBitSet(10);
        for (int docId : docIds) {
            bitSet.set(docId);
        }
        {
            DocIdSetIterator iterator = new BitSetIterator(bitSet, bitSet.cardinality());
            ESAcceptDocs acceptDocs = new ESAcceptDocs.ScorerSupplierAcceptDocs(new TestScorerSupplier(iterator), null, 10);
            assertEquals(iterator.cost(), acceptDocs.approximateCost());
            assertEquals(iterator.cost(), acceptDocs.cost());
            // iterate the docs ensuring they match
            DocIdSetIterator acceptDocsIterator = acceptDocs.iterator();
            for (int docId : docIds) {
                assertEquals(docId, acceptDocsIterator.nextDoc());
            }
        }
        {
            DocIdSetIterator iterator = new BitSetIterator(bitSet, bitSet.cardinality());
            ESAcceptDocs acceptDocs = new ESAcceptDocs.ScorerSupplierAcceptDocs(new TestScorerSupplier(iterator), null, 10);
            Bits acceptDocsBits = acceptDocs.bits();
            for (int i = 0; i < 10; i++) {
                assertEquals(bitSet.get(i), acceptDocsBits.get(i));
            }
        }
        {
            DocIdSetIterator iterator = new BitSetIterator(bitSet, bitSet.cardinality());
            FixedBitSet liveDocs = new FixedBitSet(10);
            liveDocs.set(0, 10);
            // lets delete docs 1, 3, 9
            liveDocs.clear(1);
            liveDocs.clear(3);
            liveDocs.clear(9);
            ESAcceptDocs acceptDocs = new ESAcceptDocs.ScorerSupplierAcceptDocs(new TestScorerSupplier(iterator), liveDocs, 10);
            // verify approximate cost doesn't count deleted docs
            assertEquals(5L, acceptDocs.approximateCost());
            // actual cost should count only live docs
            assertEquals(2L, acceptDocs.cost());
            // iterate the docs ensuring they match
            DocIdSetIterator acceptDocsIterator = acceptDocs.iterator();
            assertEquals(5, acceptDocsIterator.nextDoc());
            assertEquals(7, acceptDocsIterator.nextDoc());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, acceptDocsIterator.nextDoc());
        }
    }

    public void testFromBits() throws IOException {
        FixedBitSet acceptedDocs = new FixedBitSet(10);
        acceptedDocs.set(1);
        acceptedDocs.set(3);
        acceptedDocs.set(5);
        ESAcceptDocs acceptDocs = new ESAcceptDocs.BitsAcceptDocs(acceptedDocs, 10);
        assertEquals(3L, acceptDocs.approximateCost());
        assertEquals(3L, acceptDocs.cost());
        // iterate the docs ensuring they match
        DocIdSetIterator acceptDocsIterator = acceptDocs.iterator();
        assertEquals(1, acceptDocsIterator.nextDoc());
        assertEquals(3, acceptDocsIterator.nextDoc());
        assertEquals(5, acceptDocsIterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, acceptDocsIterator.nextDoc());
        // verify bits
        Bits acceptDocsBits = acceptDocs.bits();
        for (int i = 0; i < 10; i++) {
            assertEquals(acceptedDocs.get(i), acceptDocsBits.get(i));
        }
    }

    private static class TestScorerSupplier extends ScorerSupplier {
        private final DocIdSetIterator iterator;

        TestScorerSupplier(DocIdSetIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public Scorer get(long leadCost) throws IOException {
            return new Scorer() {
                @Override
                public int docID() {
                    return iterator.docID();
                }

                @Override
                public DocIdSetIterator iterator() {
                    return iterator;
                }

                @Override
                public float getMaxScore(int upTo) throws IOException {
                    return Float.MAX_VALUE;
                }

                @Override
                public float score() throws IOException {
                    return 0;
                }
            };
        }

        @Override
        public long cost() {
            return iterator.cost();
        }
    }

}
