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
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

public class NonNegativeScoresSimilarityTests extends ESTestCase {

    public void testBasics() {
        Similarity negativeScoresSim = new Similarity() {

            @Override
            public long computeNorm(FieldInvertState state) {
                return state.getLength();
            }

            @Override
            public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
                return new SimScorer() {
                    @Override
                    public float score(float freq, long norm) {
                        return freq - 5;
                    }
                };
            }
        };
        Similarity assertingSimilarity = new NonNegativeScoresSimilarity(negativeScoresSim);
        SimScorer scorer = assertingSimilarity.scorer(1f, null);
        assertEquals(2f, scorer.score(7f, 1L), 0f);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> scorer.score(2f, 1L));
        assertThat(e.getMessage(), Matchers.containsString("Similarities must not produce negative scores"));
    }

}
