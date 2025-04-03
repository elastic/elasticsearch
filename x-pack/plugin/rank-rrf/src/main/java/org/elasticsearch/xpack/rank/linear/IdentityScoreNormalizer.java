/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;

public class IdentityScoreNormalizer extends ScoreNormalizer {

    public static final IdentityScoreNormalizer INSTANCE = new IdentityScoreNormalizer();

    public static final String NAME = "none";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ScoreDoc[] normalizeScores(ScoreDoc[] docs) {
        if (docs == null || docs.length == 0) {
            return docs;
        }

        // Create a new array to avoid modifying input
        ScoreDoc[] normalizedDocs = new ScoreDoc[docs.length];
        for (int i = 0; i < docs.length; i++) {
            ScoreDoc doc = docs[i];
            if (doc == null) {
                normalizedDocs[i] = new ScoreDoc(0, 0.0f, 0);
            } else {
                float score = Float.isNaN(doc.score) ? 0.0f : doc.score;
                normalizedDocs[i] = new ScoreDoc(doc.doc, score, doc.shardIndex);
            }
        }
        return normalizedDocs;
    }
}
