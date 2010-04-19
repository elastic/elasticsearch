/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.util.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Set;

/**
 * A query that wraps another query and applies the provided boost values to it. Simply
 * applied the boost factor to the score of the wrapped query.
 *
 * @author kimchy (shay.banon)
 */
public class CustomBoostFactorQuery extends Query {

    private Query q;
    private float boostFactor;

    public CustomBoostFactorQuery(Query subQuery, float boostFactor) {
        this.q = subQuery;
        this.boostFactor = boostFactor;
    }

    public Query getQuery() {
        return q;
    }

    public float getBoostFactor() {
        return boostFactor;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query newQ = q.rewrite(reader);
        if (newQ == q) return this;
        CustomBoostFactorQuery bq = (CustomBoostFactorQuery) this.clone();
        bq.q = newQ;
        return bq;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        q.extractTerms(terms);
    }

    @Override
    public Weight createWeight(Searcher searcher) throws IOException {
        return new CustomBoostFactorQuery.BoostedWeight(searcher);
    }

    private class BoostedWeight extends Weight {
        Searcher searcher;
        Weight qWeight;

        public BoostedWeight(Searcher searcher) throws IOException {
            this.searcher = searcher;
            this.qWeight = q.weight(searcher);
        }

        public Query getQuery() {
            return CustomBoostFactorQuery.this;
        }

        public float getValue() {
            return getBoost();
        }

        @Override
        public float sumOfSquaredWeights() throws IOException {
            float sum = qWeight.sumOfSquaredWeights();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm) {
            norm *= getBoost();
            qWeight.normalize(norm);
        }

        @Override
        public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
            Scorer subQueryScorer = qWeight.scorer(reader, true, false);
            if (subQueryScorer == null) {
                return null;
            }
            return new CustomBoostFactorQuery.CustomScorer(getSimilarity(searcher), reader, this, subQueryScorer);
        }

        @Override
        public Explanation explain(IndexReader reader, int doc) throws IOException {
            Explanation subQueryExpl = qWeight.explain(reader, doc);
            if (!subQueryExpl.isMatch()) {
                return subQueryExpl;
            }

            float sc = subQueryExpl.getValue() * boostFactor;
            Explanation res = new ComplexExplanation(
                    true, sc, CustomBoostFactorQuery.this.toString() + ", product of:");
            res.addDetail(subQueryExpl);
            res.addDetail(new Explanation(boostFactor, "boostFactor"));
            return res;
        }
    }


    private class CustomScorer extends Scorer {
        private final CustomBoostFactorQuery.BoostedWeight weight;
        private final float qWeight;
        private final Scorer scorer;
        private final IndexReader reader;

        private CustomScorer(Similarity similarity, IndexReader reader, CustomBoostFactorQuery.BoostedWeight w,
                             Scorer scorer) throws IOException {
            super(similarity);
            this.weight = w;
            this.qWeight = w.getValue();
            this.scorer = scorer;
            this.reader = reader;
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
            float score = qWeight * scorer.score() * boostFactor;

            // Current Lucene priority queues can't handle NaN and -Infinity, so
            // map to -Float.MAX_VALUE. This conditional handles both -infinity
            // and NaN since comparisons with NaN are always false.
            return score > Float.NEGATIVE_INFINITY ? score : -Float.MAX_VALUE;
        }

        public Explanation explain(int doc) throws IOException {
            Explanation subQueryExpl = weight.qWeight.explain(reader, doc);
            if (!subQueryExpl.isMatch()) {
                return subQueryExpl;
            }
            float sc = subQueryExpl.getValue() * boostFactor;
            Explanation res = new ComplexExplanation(
                    true, sc, CustomBoostFactorQuery.this.toString() + ", product of:");
            res.addDetail(subQueryExpl);
            res.addDetail(new Explanation(boostFactor, "boostFactor"));
            return res;
        }
    }


    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("CustomBoostFactor(").append(q.toString(field)).append(',').append(boostFactor).append(')');
        sb.append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    public boolean equals(Object o) {
        if (getClass() != o.getClass()) return false;
        CustomBoostFactorQuery other = (CustomBoostFactorQuery) o;
        return this.getBoost() == other.getBoost()
                && this.q.equals(other.q)
                && this.boostFactor == other.boostFactor;
    }

    public int hashCode() {
        int h = q.hashCode();
        h ^= (h << 17) | (h >>> 16);
        h += Float.floatToIntBits(boostFactor);
        h ^= (h << 8) | (h >>> 25);
        h += Float.floatToIntBits(getBoost());
        return h;
    }

}

