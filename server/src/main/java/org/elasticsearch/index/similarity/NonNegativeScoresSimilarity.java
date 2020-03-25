/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
            throw new IllegalArgumentException(ES_ENFORCE_POSITIVE_SCORES + " may only be unset or set to [false], but got [" +
                    enforcePositiveScores + "]");
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
                        throw new IllegalArgumentException("Similarities must not produce negative scores, but got:\n" +
                                inScorer.explain(Explanation.match(freq, "term frequency"), norm));
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
                    expl = Explanation.match(0f, "max of:",
                            expl, Explanation.match(0f, "Minimum allowed score"));
                }
                return expl;
            }
        };
    }
}
