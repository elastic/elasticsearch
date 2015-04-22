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

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.io.IOException;

/**
 *
 */
public class WeightFactorFunction extends ScoreFunction {

    private static final ScoreFunction SCORE_ONE = new ScoreOne(CombineFunction.MULT);
    private final ScoreFunction scoreFunction;
    private float weight = 1.0f;

    public WeightFactorFunction(float weight, ScoreFunction scoreFunction) {
        super(CombineFunction.MULT);
        if (scoreFunction instanceof BoostScoreFunction) {
            throw new ElasticsearchIllegalArgumentException(BoostScoreFunction.BOOST_WEIGHT_ERROR_MESSAGE);
        }
        if (scoreFunction == null) {
            this.scoreFunction = SCORE_ONE;
        } else {
            this.scoreFunction = scoreFunction;
        }
        this.weight = weight;
    }

    public WeightFactorFunction(float weight) {
        super(CombineFunction.MULT);
        this.scoreFunction = SCORE_ONE;
        this.weight = weight;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
        final LeafScoreFunction leafFunction = scoreFunction.getLeafScoreFunction(ctx);
        return new LeafScoreFunction() {
            @Override
            public double score(int docId, float subQueryScore) {
                return leafFunction.score(docId, subQueryScore) * getWeight();
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                Explanation functionExplanation = leafFunction.explainScore(docId, subQueryScore);
                return Explanation.match(
                        functionExplanation.getValue() * (float) getWeight(), "product of:",
                        functionExplanation, explainWeight());
            }
        };
    }

    public Explanation explainWeight() {
        return Explanation.match(getWeight(), "weight");
    }

    public float getWeight() {
        return weight;
    }

    private static class ScoreOne extends ScoreFunction {

        protected ScoreOne(CombineFunction scoreCombiner) {
            super(scoreCombiner);
        }

        @Override
        public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) {
            return new LeafScoreFunction() {
                @Override
                public double score(int docId, float subQueryScore) {
                    return 1.0;
                }

                @Override
                public Explanation explainScore(int docId, Explanation subQueryScore) {
                    return Explanation.match(1.0f, "constant score 1.0 - no function provided");
                }
            };
        }
    }
}
