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
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;

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
        // TODO we need to switch from scorer to VectorScorer and values so the filter can be lazily applied
        //  building the bitset eagerly is silly for scoring everything
        if (knnCollector.k() == 0 || scorer == null) {
            return;
        }
        Bits acceptedOrds = scorer.getAcceptOrds(acceptDocs.bits());
        int[] ords = new int[BULK_SCORE_BLOCKS];
        float[] scores = new float[BULK_SCORE_BLOCKS];
        int numOrds = 0;
        int numVectors = scorer.maxOrd();
        for (int i = 0; i < numVectors; i++) {
            if (acceptedOrds == null || acceptedOrds.get(i)) {
                if (knnCollector.earlyTerminated()) {
                    break;
                }
                ords[numOrds++] = i;
                if (numOrds == ords.length) {
                    knnCollector.incVisitedCount(numOrds);
                    scorer.bulkScore(ords, scores, numOrds);
                    for (int j = 0; j < numOrds; j++) {
                        knnCollector.collect(scorer.ordToDoc(ords[j]), scores[j]);
                    }
                    numOrds = 0;
                }
            }
        }

        if (numOrds > 0) {
            knnCollector.incVisitedCount(numOrds);
            scorer.bulkScore(ords, scores, numOrds);
            for (int j = 0; j < numOrds; j++) {
                knnCollector.collect(scorer.ordToDoc(ords[j]), scores[j]);
            }
        }
        assert knnCollector.earlyTerminated() == false;
    }
}
