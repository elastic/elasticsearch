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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * A query that allows for a pluggable boost function to be applied to it.
 */
public class FunctionScoreQuery extends Query {

    Query subQuery;
    final ScoreFunction function;
    float maxBoost = Float.MAX_VALUE;
    CombineFunction combineFunction;
    private Float minScore = null;

    public FunctionScoreQuery(Query subQuery, ScoreFunction function, Float minScore) {
        this.subQuery = subQuery;
        this.function = function;
        this.combineFunction = function == null? CombineFunction.MULT : function.getDefaultScoreCombiner();
        this.minScore = minScore;
    }

    public FunctionScoreQuery(Query subQuery, ScoreFunction function) {
        this.subQuery = subQuery;
        this.function = function;
        this.combineFunction = function.getDefaultScoreCombiner();
    }

    public void setCombineFunction(CombineFunction combineFunction) {
        this.combineFunction = combineFunction;
    }
    
    public void setMaxBoost(float maxBoost) {
        this.maxBoost = maxBoost;
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
        if (getBoost() != 1.0F) {
            return super.rewrite(reader);
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
        if (needsScores == false) {
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

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Scorer subQueryScorer = subQueryWeight.scorer(context);
            if (subQueryScorer == null) {
                return null;
            }
            LeafScoreFunction leafFunction = null;
            if (function != null) {
                leafFunction = function.getLeafScoreFunction(context);
            }
            return new FunctionFactorScorer(this, subQueryScorer, leafFunction, maxBoost, combineFunction, minScore, needsScores);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            Explanation subQueryExpl = subQueryWeight.explain(context, doc);
            if (!subQueryExpl.isMatch()) {
                return subQueryExpl;
            }
            if (function != null) {
                Explanation functionExplanation = function.getLeafScoreFunction(context).explainScore(doc, subQueryExpl);
                return combineFunction.explain(subQueryExpl, functionExplanation, maxBoost);
            } else {
                return subQueryExpl;
            }
        }
    }

    static class FunctionFactorScorer extends CustomBoostFactorScorer {

        private final LeafScoreFunction function;
        private final boolean needsScores;

        private FunctionFactorScorer(CustomBoostFactorWeight w, Scorer scorer, LeafScoreFunction function, float maxBoost, CombineFunction scoreCombiner, Float minScore, boolean needsScores)
                throws IOException {
            super(w, scorer, maxBoost, scoreCombiner, minScore);
            this.function = function;
            this.needsScores = needsScores;
        }

        @Override
        public float innerScore() throws IOException {
            // Even if the weight is created with needsScores=false, it might
            // be costly to call score(), so we explicitly check if scores
            // are needed
            float score = needsScores ? scorer.score() : 0f;
            if (function == null) {
                return score;
            } else {
                return scoreCombiner.combine(score,
                        function.score(scorer.docID(), score), maxBoost);
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
        if (o == null || getClass() != o.getClass())
            return false;
        FunctionScoreQuery other = (FunctionScoreQuery) o;
        return this.getBoost() == other.getBoost() && this.subQuery.equals(other.subQuery) && (this.function != null ? this.function.equals(other.function) : other.function == null)
                && this.maxBoost == other.maxBoost;
    }

    @Override
    public int hashCode() {
        return subQuery.hashCode() + 31 * Objects.hashCode(function) ^ Float.floatToIntBits(getBoost());
    }
}
