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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchIllegalArgumentException;

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
    public void setNextReader(AtomicReaderContext context) {
        scoreFunction.setNextReader(context);
    }

    @Override
    public double score(int docId, float subQueryScore) {
        return scoreFunction.score(docId, subQueryScore) * getWeight();
    }

    @Override
    public Explanation explainScore(int docId, float score) {
        Explanation functionScoreExplanation;
        Explanation functionExplanation = scoreFunction.explainScore(docId, score);
        functionScoreExplanation = new ComplexExplanation(true, functionExplanation.getValue() * (float) getWeight(), "product of:");
        functionScoreExplanation.addDetail(functionExplanation);
        functionScoreExplanation.addDetail(explainWeight());
        return functionScoreExplanation;
    }

    public Explanation explainWeight() {
        return new Explanation(getWeight(), "weight");
    }

    public float getWeight() {
        return weight;
    }

    private static class ScoreOne extends ScoreFunction {

        protected ScoreOne(CombineFunction scoreCombiner) {
            super(scoreCombiner);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {

        }

        @Override
        public double score(int docId, float subQueryScore) {
            return 1.0;
        }

        @Override
        public Explanation explainScore(int docId, float subQueryScore) {
            return new Explanation(1.0f, "constant score 1.0 - no function provided");
        }
    }
}
