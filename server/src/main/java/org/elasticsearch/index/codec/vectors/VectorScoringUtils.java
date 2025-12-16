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
     * @param vectorValues the vector values for iteration
     * @param scorer       the vector scorer
     * @throws IOException if an I/O error occurs
     */
    public static void scoreAndCollectAll(KnnCollector knnCollector, AcceptDocs acceptDocs, KnnVectorValues vectorValues, RandomVectorScorer scorer) throws IOException {
        if (knnCollector.k() == 0 || scorer == null || vectorValues == null) {
            return;
        }

        DocIdSetIterator acceptDocsIterator = acceptDocs.iterator();

        if (acceptDocsIterator == null) {
            bulkScoreAllOrdinals(knnCollector, scorer);
        } else {
            bulkScoreWithFilter(knnCollector, acceptDocsIterator, vectorValues, scorer);
        }
    }

    /**
     * Bulk score all vector ordinals without doc filtering.
     * Assumes all ordinals map to live docs.
     */
    private static void bulkScoreAllOrdinals(KnnCollector knnCollector, RandomVectorScorer scorer) throws IOException {
        int[] ords = new int[BULK_SCORE_BLOCKS];
        float[] scores = new float[BULK_SCORE_BLOCKS];
        int count = 0;
        int maxOrd = scorer.maxOrd();
        
        for (int i = 0; i < maxOrd; i++) {
            if (knnCollector.earlyTerminated()) {
                break;
            }
            
            ords[count] = i;
            count++;
            
            if (count == BULK_SCORE_BLOCKS) {
                scorer.bulkScore(ords, scores, count);
                for (int j = 0; j < count; j++) {
                    knnCollector.collect(scorer.ordToDoc(ords[j]), scores[j]);
                }
                knnCollector.incVisitedCount(count);
                count = 0;
            }
        }
        
        if (count > 0) {
            scorer.bulkScore(ords, scores, count);
            for (int j = 0; j < count; j++) {
                knnCollector.collect(scorer.ordToDoc(ords[j]), scores[j]);
            }
            knnCollector.incVisitedCount(count);
        }
    }

    private static void bulkScoreWithFilter(KnnCollector knnCollector, DocIdSetIterator acceptDocsIterator, KnnVectorValues vectorValues, RandomVectorScorer scorer) throws IOException {
        int[] ords = new int[BULK_SCORE_BLOCKS];
        int[] docs = new int[BULK_SCORE_BLOCKS];
        float[] scores = new float[BULK_SCORE_BLOCKS];
        int count = 0;
        
        var conjunction = ConjunctionUtils.intersectIterators(List.of(vectorValues, acceptDocsIterator));
        
        int doc;
        while ((doc = conjunction.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (knnCollector.earlyTerminated()) {
                break;
            }
            
            int ord = vectorValues.ordValue();
            ords[count] = ord;
            docs[count] = doc;
            count++;
            
            if (count == BULK_SCORE_BLOCKS) {
                scorer.bulkScore(ords, scores, count);
                for (int j = 0; j < count; j++) {
                    knnCollector.collect(docs[j], scores[j]);
                }
                knnCollector.incVisitedCount(count);
                count = 0;
            }
        }
        
        if (count > 0) {
            scorer.bulkScore(ords, scores, count);
            for (int j = 0; j < count; j++) {
                knnCollector.collect(docs[j], scores[j]);
            }
            knnCollector.incVisitedCount(count);
        }
}
