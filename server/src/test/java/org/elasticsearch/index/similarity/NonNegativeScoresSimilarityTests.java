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
