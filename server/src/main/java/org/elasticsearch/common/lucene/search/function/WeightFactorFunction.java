/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;

import java.io.IOException;
import java.util.Objects;

public class WeightFactorFunction extends ScoreFunction {

    private static final ScoreFunction SCORE_ONE = new ScoreOne(CombineFunction.MULTIPLY);
    private final ScoreFunction scoreFunction;
    private float weight = 1.0f;

    public WeightFactorFunction(float weight, ScoreFunction scoreFunction) {
        super(CombineFunction.MULTIPLY);
        if (scoreFunction == null) {
            this.scoreFunction = SCORE_ONE;
        } else {
            this.scoreFunction = scoreFunction;
        }
        this.weight = weight;
    }

    public WeightFactorFunction(float weight) {
        super(CombineFunction.MULTIPLY);
        this.scoreFunction = SCORE_ONE;
        this.weight = weight;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
        final LeafScoreFunction leafFunction = scoreFunction.getLeafScoreFunction(ctx);
        return new LeafScoreFunction() {
            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                return leafFunction.score(docId, subQueryScore) * getWeight();
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                Explanation functionExplanation = leafFunction.explainScore(docId, subQueryScore);
                return Explanation.match(
                        functionExplanation.getValue().floatValue() * getWeight(), "product of:",
                        functionExplanation, explainWeight());
            }
        };
    }

    @Override
    public boolean needsScores() {
        return scoreFunction.needsScores();
    }

    public Explanation explainWeight() {
        return Explanation.match(getWeight(), "weight");
    }

    @Override
    public float getWeight() {
        return weight;
    }

    public ScoreFunction getScoreFunction() {
        return scoreFunction;
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        WeightFactorFunction weightFactorFunction = (WeightFactorFunction) other;
        return this.weight == weightFactorFunction.weight &&
                Objects.equals(this.scoreFunction, weightFactorFunction.scoreFunction);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(weight, scoreFunction);
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

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        protected boolean doEquals(ScoreFunction other) {
            return true;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }
    }
}
