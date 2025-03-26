/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;

public class MinMaxScoreNormalizer extends ScoreNormalizer {

    public static final MinMaxScoreNormalizer INSTANCE = new MinMaxScoreNormalizer();

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
            if (Float.isNaN(rd.score) == false) {
                atLeastOneValidScore = true;
                if (rd.score > max) {
                    max = rd.score;
                }
                if (rd.score < min) {
                    min = rd.score;
                }
            }
        }

        boolean minEqualsMax = atLeastOneValidScore && Math.abs(min - max) < EPSILON;

        for (int i = 0; i < docs.length; i++) {
            float score;
            if (Float.isNaN(docs[i].score) || (atLeastOneValidScore == false)) {
                score = 0.0f;
            } else if (minEqualsMax) {
                score = docs[i].score;
            } else {
                score = (docs[i].score - min) / (max - min);
            }
            scoreDocs[i] = new ScoreDoc(docs[i].doc, score, docs[i].shardIndex);
        }
        return scoreDocs;
    }
}
