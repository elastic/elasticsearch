/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.features.NodeFeature;

public class MinMaxScoreNormalizer extends ScoreNormalizer {
    public static final MinMaxScoreNormalizer INSTANCE = new MinMaxScoreNormalizer();

    public static final NodeFeature LINEAR_RETRIEVER_MINMAX_SINGLE_DOC_FIX = new NodeFeature("linear_retriever.minmax_single_doc_fix");

    public static final String NAME = "minmax";

    private static final float EPSILON = 1e-6f;

    public MinMaxScoreNormalizer() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ScoreDoc[] normalizeScores(ScoreDoc[] docs) {
        if (docs.length == 0) {
            return docs;
        }
        // create a new array to avoid changing ScoreDocs in place
        ScoreDoc[] scoreDocs = new ScoreDoc[docs.length];
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        boolean atLeastOneValidScore = false;
        for (ScoreDoc rd : docs) {
            if (false == atLeastOneValidScore && false == Float.isNaN(rd.score)) {
                atLeastOneValidScore = true;
            }
            if (rd.score > max) {
                max = rd.score;
            }
            if (rd.score < min) {
                min = rd.score;
            }
        }
        if (false == atLeastOneValidScore) {
            // we do not have any scores to normalize, so we just return the original array
            return docs;
        }

        boolean minEqualsMax = Math.abs(min - max) < EPSILON;
        for (int i = 0; i < docs.length; i++) {
            float score;
            if (minEqualsMax) {
                // This can happen if there is only one doc in the result set or if all docs have nearly equivalent scores
                // (i.e. within epsilon). In this case, assign every doc the max normalized score.
                score = 1.0f;
            } else {
                score = (docs[i].score - min) / (max - min);
            }
            scoreDocs[i] = new ScoreDoc(docs[i].doc, score, docs[i].shardIndex);
        }
        return scoreDocs;
    }
}
