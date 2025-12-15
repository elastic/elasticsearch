/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;
import java.util.List;

/**
 * Utility methods for vector scoring and collection.
 */
public final class VectorScoringUtils {

    private static final int BULK_SCORE_BLOCKS = 64;

    private VectorScoringUtils() {}

    /**
     * Scores and collects all vectors using the provided scorer and collector.
     *
     * @param knnCollector the collector to collect scored vectors
     * @param acceptDocs   the accept docs to filter vectors
     * @param scorer       the vector scorer
     * @throws IOException if an I/O error occurs
     */
    public static void scoreAndCollectAll(KnnCollector knnCollector, AcceptDocs acceptDocs, RandomVectorScorer scorer) throws IOException {
        if (knnCollector.k() == 0 || scorer == null) {
            return;
        }

        DocIdSetIterator acceptDocsIterator = acceptDocs.iterator();

        if (acceptDocsIterator == null) {
            for (int i = 0; i < scorer.maxOrd(); i++) {
                knnCollector.collect(scorer.ordToDoc(i), scorer.score(i));
                knnCollector.incVisitedCount(1);
            }
        } else {
            DocIdSetIterator vectorIterator = new OrdinalToDocIterator(scorer);
            var conjunction = ConjunctionUtils.intersectIterators(List.of(vectorIterator, acceptDocsIterator));

            int doc;
            while ((doc = conjunction.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                int ord = ((OrdinalToDocIterator) vectorIterator).currentOrd();
                knnCollector.collect(doc, scorer.score(ord));
                knnCollector.incVisitedCount(1);
            }
        }
        assert knnCollector.earlyTerminated() == false;
    }

    /**
     * Iterator that converts ordinals to document IDs for use with ConjunctionUtils
     */
    private static class OrdinalToDocIterator extends DocIdSetIterator {
        private final RandomVectorScorer scorer;
        private int currentOrd = -1;
        private int currentDoc = -1;

        OrdinalToDocIterator(RandomVectorScorer scorer) {
            this.scorer = scorer;
        }

        @Override
        public int docID() {
            return currentDoc;
        }

        @Override
        public int nextDoc() throws IOException {
            currentOrd++;
            if (currentOrd >= scorer.maxOrd()) {
                currentDoc = NO_MORE_DOCS;
            } else {
                currentDoc = scorer.ordToDoc(currentOrd);
            }
            return currentDoc;
        }

        @Override
        public int advance(int target) throws IOException {
            while (currentDoc < target && currentDoc != NO_MORE_DOCS) {
                nextDoc();
            }
            return currentDoc;
        }

        @Override
        public long cost() {
            return scorer.maxOrd();
        }

        int currentOrd() {
            return currentOrd;
        }
    }
}
