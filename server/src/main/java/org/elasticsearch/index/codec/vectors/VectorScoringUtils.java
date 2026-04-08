/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;

/**
 * Utility methods for vector scoring and collection.
 */
public final class VectorScoringUtils {

    private VectorScoringUtils() {}

    /**
     * {@link VectorScorer} for dense vectors; bulk path uses {@link VectorScorer.Bulk#fromRandomScorerDense}.
     *
     * @param scorer   scorer for the values behind {@code iterator}
     * @param iterator iterator over the same values instance as {@code scorer}
     */
    public static VectorScorer denseVectorScorer(RandomVectorScorer scorer, KnnVectorValues.DocIndexIterator iterator) {
        return new VectorScorer() {
            @Override
            public float score() throws IOException {
                return scorer.score(iterator.index());
            }

            @Override
            public DocIdSetIterator iterator() {
                return iterator;
            }

            @Override
            public Bulk bulk(DocIdSetIterator matchingDocs) {
                return Bulk.fromRandomScorerDense(scorer, iterator, matchingDocs);
            }
        };
    }

    /**
     * {@link VectorScorer} for sparse vectors; bulk path uses {@link VectorScorer.Bulk#fromRandomScorerSparse}.
     *
     * @param scorer   scorer for the values behind {@code iterator}
     * @param iterator iterator over the same values instance as {@code scorer}
     */
    public static VectorScorer sparseVectorScorer(RandomVectorScorer scorer, KnnVectorValues.DocIndexIterator iterator) {
        return new VectorScorer() {
            @Override
            public float score() throws IOException {
                return scorer.score(iterator.index());
            }

            @Override
            public DocIdSetIterator iterator() {
                return iterator;
            }

            @Override
            public Bulk bulk(DocIdSetIterator matchingDocs) {
                return Bulk.fromRandomScorerSparse(scorer, iterator, matchingDocs);
            }
        };
    }

    /**
     * Scores all vectors using the provided scorer and collects batches of documents when the batch's maxScore beats the collector's
     * minCompetitiveSimilarity.
     *
     * @param knnCollector the collector to collect scored vectors
     * @param acceptDocs   the accept docs to filter vectors
     * @param vectorScorer the vector scorer
     * @throws IOException if an I/O error occurs
     */
    public static void scoreAndCollectAll(KnnCollector knnCollector, AcceptDocs acceptDocs, VectorScorer vectorScorer) throws IOException {
        if (knnCollector.k() == 0 || vectorScorer == null) {
            return;
        }

        VectorScorer.Bulk bulkScorer = vectorScorer.bulk(acceptDocs.iterator());
        DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
        for (float maxScore = bulkScorer.nextDocsAndScores(DocIdSetIterator.NO_MORE_DOCS, null, buffer); buffer.size > 0; maxScore =
            bulkScorer.nextDocsAndScores(DocIdSetIterator.NO_MORE_DOCS, null, buffer)) {

            // Tracks the number of vectors scored. for loop condition guarantees buffer.size is greater than 0.
            knnCollector.incVisitedCount(buffer.size);

            if (knnCollector.earlyTerminated()) {
                break;
            }

            if (maxScore < knnCollector.minCompetitiveSimilarity()) {
                // all the scores in this batch are too low, skip
                continue;
            }

            for (int i = 0; i < buffer.size; i++) {
                float score = buffer.features[i];
                int doc = buffer.docs[i];
                knnCollector.collect(doc, score);
            }
        }
        assert knnCollector.earlyTerminated() == false;
    }
}
