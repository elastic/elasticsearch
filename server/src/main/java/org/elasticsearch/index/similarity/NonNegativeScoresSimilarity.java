/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.similarity;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;

/**
 * A {@link Similarity} that rejects negative scores. This class exists so that users get
 * an error instead of silently corrupt top hits. It should be applied to any custom or
 * scripted similarity.
 */
// public for testing
public final class NonNegativeScoresSimilarity extends Similarity {

    // Escape hatch
    private static final String ES_ENFORCE_POSITIVE_SCORES = "es.enforce.positive.scores";
    private static final boolean ENFORCE_POSITIVE_SCORES;
    static {
        String enforcePositiveScores = System.getProperty(ES_ENFORCE_POSITIVE_SCORES);
        if (enforcePositiveScores == null) {
            ENFORCE_POSITIVE_SCORES = true;
        } else if ("false".equals(enforcePositiveScores)) {
            ENFORCE_POSITIVE_SCORES = false;
        } else {
            throw new IllegalArgumentException(
                ES_ENFORCE_POSITIVE_SCORES + " may only be unset or set to [false], but got [" + enforcePositiveScores + "]"
            );
        }
    }

    private final Similarity in;

    public NonNegativeScoresSimilarity(Similarity in) {
        this.in = in;
    }

    public Similarity getDelegate() {
        return in;
    }

    @Override
    public long computeNorm(FieldInvertState state) {
        return in.computeNorm(state);
    }

    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        final SimScorer inScorer = in.scorer(boost, collectionStats, termStats);
        return new SimScorer() {

            @Override
            public float score(float freq, long norm) {
                float score = inScorer.score(freq, norm);
                if (score < 0f) {
                    if (ENFORCE_POSITIVE_SCORES) {
                        throw new IllegalArgumentException(
                            "Similarities must not produce negative scores, but got:\n"
                                + inScorer.explain(Explanation.match(freq, "term frequency"), norm)
                        );
                    } else {
                        return 0f;
                    }
                }
                return score;
            }

            @Override
            public Explanation explain(Explanation freq, long norm) {
                Explanation expl = inScorer.explain(freq, norm);
                if (expl.isMatch() && expl.getValue().floatValue() < 0) {
                    expl = Explanation.match(0f, "max of:", expl, Explanation.match(0f, "Minimum allowed score"));
                }
                return expl;
            }
        };
    }
}
