
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;

/**
 * A score normalizer that applies L2 normalization to a set of scores.
 * <p>
 * Each score is divided by the L2 norm of the scores if the norm is greater than a small EPSILON.
 * If all scores are zero or NaN, normalization is skipped and the original scores are returned.
 * </p>
 */
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
        for (ScoreDoc doc : docs) {
            if (Float.isNaN(doc.score) == false) {
                atLeastOneValidScore = true;
                sumOfSquares += doc.score * doc.score;
            }
        }
        if (atLeastOneValidScore == false) {
            // No valid scores to normalize
            return docs;
        }
        double norm = Math.sqrt(sumOfSquares);
        if (norm < EPSILON) {
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
