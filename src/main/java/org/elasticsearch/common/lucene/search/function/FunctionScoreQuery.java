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
        this.combineFunction = function == null? combineFunction.MULT : function.getDefaultScoreCombiner();
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
        Query newQ = subQuery.rewrite(reader);
        if (newQ == subQuery) {
            return this;
        }
        FunctionScoreQuery bq = (FunctionScoreQuery) this.clone();
        bq.subQuery = newQ;
        return bq;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        subQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        Weight subQueryWeight = subQuery.createWeight(searcher);
        return new CustomBoostFactorWeight(subQueryWeight);
    }

    class CustomBoostFactorWeight extends Weight {

        final Weight subQueryWeight;

        public CustomBoostFactorWeight(Weight subQueryWeight) throws IOException {
            this.subQueryWeight = subQueryWeight;
        }

        public Query getQuery() {
            return FunctionScoreQuery.this;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            float sum = subQueryWeight.getValueForNormalization();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            subQueryWeight.normalize(norm, topLevelBoost * getBoost());
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            // we ignore scoreDocsInOrder parameter, because we need to score in
            // order if documents are scored with a script. The
            // ShardLookup depends on in order scoring.
            Scorer subQueryScorer = subQueryWeight.scorer(context, acceptDocs);
            if (subQueryScorer == null) {
                return null;
            }
            if (function != null) {
                function.setNextReader(context);
            }
            return new FunctionFactorScorer(this, subQueryScorer, function, maxBoost, combineFunction, minScore);
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            Explanation subQueryExpl = subQueryWeight.explain(context, doc);
            if (!subQueryExpl.isMatch()) {
                return subQueryExpl;
            }
            if (function != null) {
                function.setNextReader(context);
                Explanation functionExplanation = function.explainScore(doc, subQueryExpl);
                return combineFunction.explain(getBoost(), subQueryExpl, functionExplanation, maxBoost);
            } else {
                return subQueryExpl;
            }
        }
    }

    static class FunctionFactorScorer extends CustomBoostFactorScorer {

        private final ScoreFunction function;

        private FunctionFactorScorer(CustomBoostFactorWeight w, Scorer scorer, ScoreFunction function, float maxBoost, CombineFunction scoreCombiner, Float minScore)
                throws IOException {
            super(w, scorer, maxBoost, scoreCombiner, minScore);
            this.function = function;
        }

        @Override
        public float innerScore() throws IOException {
            float score = scorer.score();
            if (function == null) {
                return subQueryBoost * score;
            } else {
                return scoreCombiner.combine(subQueryBoost, score,
                        function.score(scorer.docID(), score), maxBoost);
            }
        }
    }

    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("function score (").append(subQuery.toString(field)).append(",function=").append(function).append(')');
        sb.append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        FunctionScoreQuery other = (FunctionScoreQuery) o;
        return this.getBoost() == other.getBoost() && this.subQuery.equals(other.subQuery) && (this.function != null ? this.function.equals(other.function) : other.function == null)
                && this.maxBoost == other.maxBoost;
    }

    public int hashCode() {
        return subQuery.hashCode() + 31 * Objects.hashCode(function) ^ Float.floatToIntBits(getBoost());
    }
}
