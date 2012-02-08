/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Set;

/**
 * A query that allows for a pluggable boost function to be applied to it.
 */
public class FunctionScoreQuery extends Query {

    Query subQuery;
    final ScoreFunction function;

    public FunctionScoreQuery(Query subQuery, ScoreFunction function) {
        this.subQuery = subQuery;
        this.function = function;
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
        if (newQ == subQuery) return this;
        FunctionScoreQuery bq = (FunctionScoreQuery) this.clone();
        bq.subQuery = newQ;
        return bq;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        subQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(Searcher searcher) throws IOException {
        return new CustomBoostFactorWeight(searcher);
    }

    class CustomBoostFactorWeight extends Weight {
        Searcher searcher;
        Weight subQueryWeight;

        public CustomBoostFactorWeight(Searcher searcher) throws IOException {
            this.searcher = searcher;
            this.subQueryWeight = subQuery.weight(searcher);
        }

        public Query getQuery() {
            return FunctionScoreQuery.this;
        }

        public float getValue() {
            return getBoost();
        }

        @Override
        public float sumOfSquaredWeights() throws IOException {
            float sum = subQueryWeight.sumOfSquaredWeights();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm) {
            norm *= getBoost();
            subQueryWeight.normalize(norm);
        }

        @Override
        public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
            Scorer subQueryScorer = subQueryWeight.scorer(reader, scoreDocsInOrder, false);
            if (subQueryScorer == null) {
                return null;
            }
            function.setNextReader(reader);
            return new CustomBoostFactorScorer(getSimilarity(searcher), this, subQueryScorer, function);
        }

        @Override
        public Explanation explain(IndexReader reader, int doc) throws IOException {
            Explanation subQueryExpl = subQueryWeight.explain(reader, doc);
            if (!subQueryExpl.isMatch()) {
                return subQueryExpl;
            }

            function.setNextReader(reader);
            Explanation functionExplanation = function.explainScore(doc, subQueryExpl);
            float sc = getValue() * functionExplanation.getValue();
            Explanation res = new ComplexExplanation(true, sc, "custom score, product of:");
            res.addDetail(functionExplanation);
            res.addDetail(new Explanation(getValue(), "queryBoost"));
            return res;
        }
    }


    static class CustomBoostFactorScorer extends Scorer {
        private final float subQueryWeight;
        private final Scorer scorer;
        private final ScoreFunction function;

        private CustomBoostFactorScorer(Similarity similarity, CustomBoostFactorWeight w, Scorer scorer, ScoreFunction function) throws IOException {
            super(similarity);
            this.subQueryWeight = w.getValue();
            this.scorer = scorer;
            this.function = function;
        }

        @Override
        public int docID() {
            return scorer.docID();
        }

        @Override
        public int advance(int target) throws IOException {
            return scorer.advance(target);
        }

        @Override
        public int nextDoc() throws IOException {
            return scorer.nextDoc();
        }

        @Override
        public float score() throws IOException {
            return subQueryWeight * function.score(scorer.docID(), scorer.score());
        }
    }


    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("custom score (").append(subQuery.toString(field)).append(",function=").append(function).append(')');
        sb.append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    public boolean equals(Object o) {
        if (getClass() != o.getClass()) return false;
        FunctionScoreQuery other = (FunctionScoreQuery) o;
        return this.getBoost() == other.getBoost()
                && this.subQuery.equals(other.subQuery)
                && this.function.equals(other.function);
    }

    public int hashCode() {
        return subQuery.hashCode() + 31 * function.hashCode() ^ Float.floatToIntBits(getBoost());
    }
}

