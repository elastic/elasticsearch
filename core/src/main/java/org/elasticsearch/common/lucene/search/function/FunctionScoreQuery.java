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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * A query that allows for a pluggable boost function to be applied to it.
 */
public class FunctionScoreQuery extends Query {

    public static final float DEFAULT_MAX_BOOST = Float.MAX_VALUE;

    Query subQuery;
    final ScoreFunction function;
    final float maxBoost;
    final CombineFunction combineFunction;
    private Float minScore;

    public FunctionScoreQuery(Query subQuery, ScoreFunction function, Float minScore, CombineFunction combineFunction, float maxBoost) {
        this.subQuery = subQuery;
        this.function = function;
        this.combineFunction = combineFunction;
        this.minScore = minScore;
        this.maxBoost = maxBoost;
    }

    public FunctionScoreQuery(Query subQuery, ScoreFunction function) {
        this.subQuery = subQuery;
        this.function = function;
        this.combineFunction = function.getDefaultScoreCombiner();
        this.maxBoost = DEFAULT_MAX_BOOST;
    }

    public float getMaxBoost() {
        return this.maxBoost;
    }

    public Query getSubQuery() {
        return subQuery;
    }

    public ScoreFunction getFunction() {
        return function;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = super.rewrite(reader);
        if (rewritten != this) {
            return rewritten;
        }
        Query newQ = subQuery.rewrite(reader);
        if (newQ == subQuery) {
            return this;
        }
        FunctionScoreQuery bq = (FunctionScoreQuery) this.clone();
        bq.subQuery = newQ;
        return bq;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        if (needsScores == false && minScore == null) {
            return subQuery.createWeight(searcher, needsScores);
        }

        boolean subQueryNeedsScores =
                combineFunction != CombineFunction.REPLACE // if we don't replace we need the original score
                || function == null // when the function is null, we just multiply the score, so we need it
                || function.needsScores(); // some scripts can replace with a script that returns eg. 1/_score
        Weight subQueryWeight = subQuery.createWeight(searcher, subQueryNeedsScores);
        return new CustomBoostFactorWeight(this, subQueryWeight, subQueryNeedsScores);
    }

    class CustomBoostFactorWeight extends Weight {

        final Weight subQueryWeight;
        final boolean needsScores;

        public CustomBoostFactorWeight(Query parent, Weight subQueryWeight, boolean needsScores) throws IOException {
            super(parent);
            this.subQueryWeight = subQueryWeight;
            this.needsScores = needsScores;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            subQueryWeight.extractTerms(terms);
        }

        @Override
        public float getValueForNormalization() throws IOException {
            return subQueryWeight.getValueForNormalization();
        }

        @Override
        public void normalize(float norm, float boost) {
            subQueryWeight.normalize(norm, boost);
        }

        private FunctionFactorScorer functionScorer(LeafReaderContext context) throws IOException {
            Scorer subQueryScorer = subQueryWeight.scorer(context);
            if (subQueryScorer == null) {
                return null;
            }
            LeafScoreFunction leafFunction = null;
            if (function != null) {
                leafFunction = function.getLeafScoreFunction(context);
            }
            return new FunctionFactorScorer(this, subQueryScorer, leafFunction, maxBoost, combineFunction, needsScores);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Scorer scorer = functionScorer(context);
            if (scorer != null && minScore != null) {
                scorer = new MinScoreScorer(this, scorer, minScore);
            }
            return scorer;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            Explanation subQueryExpl = subQueryWeight.explain(context, doc);
            if (!subQueryExpl.isMatch()) {
                return subQueryExpl;
            }
            Explanation expl;
            if (function != null) {
                Explanation functionExplanation = function.getLeafScoreFunction(context).explainScore(doc, subQueryExpl);
                expl = combineFunction.explain(subQueryExpl, functionExplanation, maxBoost);
            } else {
                expl = subQueryExpl;
            }
            if (minScore != null && minScore > expl.getValue()) {
                expl = Explanation.noMatch("Score value is too low, expected at least " + minScore + " but got " + expl.getValue(), expl);
            }
            return expl;
        }
    }

    static class FunctionFactorScorer extends FilterScorer {

        private final LeafScoreFunction function;
        private final boolean needsScores;
        private final CombineFunction scoreCombiner;
        private final float maxBoost;

        private FunctionFactorScorer(CustomBoostFactorWeight w, Scorer scorer, LeafScoreFunction function, float maxBoost, CombineFunction scoreCombiner, boolean needsScores)
                throws IOException {
            super(scorer, w);
            this.function = function;
            this.scoreCombiner = scoreCombiner;
            this.maxBoost = maxBoost;
            this.needsScores = needsScores;
        }

        @Override
        public float score() throws IOException {
            // Even if the weight is created with needsScores=false, it might
            // be costly to call score(), so we explicitly check if scores
            // are needed
            float score = needsScores ? super.score() : 0f;
            if (function == null) {
                return score;
            } else {
                return scoreCombiner.combine(score,
                        function.score(docID(), score), maxBoost);
            }
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("function score (").append(subQuery.toString(field)).append(",function=").append(function).append(')');
        sb.append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (super.equals(o) == false) {
            return false;
        }
        FunctionScoreQuery other = (FunctionScoreQuery) o;
        return Objects.equals(this.subQuery, other.subQuery) && Objects.equals(this.function, other.function)
                && Objects.equals(this.combineFunction, other.combineFunction)
                && Objects.equals(this.minScore, other.minScore) && this.maxBoost == other.maxBoost;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), subQuery.hashCode(), function, combineFunction, minScore, maxBoost);
    }
}
