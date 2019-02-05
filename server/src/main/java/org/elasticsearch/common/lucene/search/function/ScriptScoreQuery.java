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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticsearchException;


import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * A query that uses a script to compute documents' scores.
 */
public class ScriptScoreQuery extends Query {
    final Query subQuery;
    final ScriptScoreFunction function;
    private final Float minScore;

    public ScriptScoreQuery(Query subQuery, ScriptScoreFunction function, Float minScore) {
        this.subQuery = subQuery;
        this.function = function;
        this.minScore = minScore;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query newQ = subQuery.rewrite(reader);
        ScriptScoreFunction newFunction = (ScriptScoreFunction) function.rewrite(reader);
        if ((newQ != subQuery) || (newFunction != function)) {
            return new ScriptScoreQuery(newQ, newFunction, minScore);
        }
        return super.rewrite(reader);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (scoreMode == ScoreMode.COMPLETE_NO_SCORES && minScore == null) {
            return subQuery.createWeight(searcher, scoreMode, boost);
        }
        ScoreMode subQueryScoreMode = function.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
        Weight subQueryWeight = subQuery.createWeight(searcher, subQueryScoreMode, boost);

        return new Weight(this){
            @Override
            public void extractTerms(Set<Term> terms) {
                subQueryWeight.extractTerms(terms);
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                Scorer subQueryScorer = subQueryWeight.scorer(context);
                if (subQueryScorer == null) {
                    return null;
                }
                final LeafScoreFunction leafFunction = function.getLeafScoreFunction(context);
                Scorer scriptScorer = new Scorer(this) {
                    @Override
                    public float score() throws IOException {
                        int docId = docID();
                        float subQueryScore = subQueryScoreMode == ScoreMode.COMPLETE ? subQueryScorer.score() : 0f;
                        float score = (float) leafFunction.score(docId, subQueryScore);
                        if (score == Float.NEGATIVE_INFINITY || Float.isNaN(score)) {
                            throw new ElasticsearchException(
                                "script score query returned an invalid score: " + score + " for doc: " + docId);
                        }
                        return score;
                    }
                    @Override
                    public int docID() {
                        return subQueryScorer.docID();
                    }

                    @Override
                    public DocIdSetIterator iterator() {
                        return subQueryScorer.iterator();
                    }

                    @Override
                    public float getMaxScore(int upTo) {
                        return Float.MAX_VALUE; // TODO: what would be a good upper bound?
                    }
                };

                if (minScore != null) {
                    scriptScorer = new MinScoreScorer(this, scriptScorer, minScore);
                }
                return scriptScorer;
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                Explanation queryExplanation = subQueryWeight.explain(context, doc);
                if (queryExplanation.isMatch() == false) {
                    return queryExplanation;
                }
                Explanation explanation = function.getLeafScoreFunction(context).explainScore(doc, queryExplanation);
                if (minScore != null && minScore > explanation.getValue().floatValue()) {
                    explanation = Explanation.noMatch("Score value is too low, expected at least " + minScore +
                        " but got " + explanation.getValue(), explanation);
                }
                return explanation;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // If minScore is not null, then matches depend on statistics of the top-level reader.
                return minScore == null;
            }
        };
    }


    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("script score (").append(subQuery.toString(field)).append(", function: ");
        sb.append("{" + (function == null ? "" : function.toString()) + "}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        ScriptScoreQuery other = (ScriptScoreQuery) o;
        return Objects.equals(this.subQuery, other.subQuery)  &&
            Objects.equals(this.minScore, other.minScore) &&
            Objects.equals(this.function, other.function);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), subQuery, minScore, function);
    }
}
