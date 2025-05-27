
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;

public class L2ScoreNormalizer extends ScoreNormalizer {

    public static final L2ScoreNormalizer INSTANCE = new L2ScoreNormalizer();

    public static final String NAME = "l2_norm";

    private static final float EPSILON = 1e-6f;

    public L2ScoreNormalizer() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ScoreDoc[] normalizeScores(ScoreDoc[] docs) {
        if (docs.length == 0) {
            return docs;
        }
        double sumOfSquares = 0.0;
        boolean atLeastOneValidScore = false;
        for (ScoreDoc rd : docs) {
            if (Float.isNaN(rd.score) == false) {
                atLeastOneValidScore = true;
                sumOfSquares += rd.score * rd.score;
            }
        }
        if (atLeastOneValidScore == false) {
            // No valid scores to normalize
            return docs;
        }
        double norm = Math.sqrt(sumOfSquares);
        if (norm < EPSILON) {
            // Avoid division by zero, return original scores (or set all to zero if you prefer)
            return docs;
        }
        ScoreDoc[] scoreDocs = new ScoreDoc[docs.length];
        for (int i = 0; i < docs.length; i++) {
            float score = (float) (docs[i].score / norm);
            scoreDocs[i] = new ScoreDoc(docs[i].doc, score, docs[i].shardIndex);
        }
        return scoreDocs;
    }
}
