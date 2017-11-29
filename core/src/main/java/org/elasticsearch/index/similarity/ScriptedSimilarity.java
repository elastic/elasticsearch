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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;
import org.elasticsearch.script.SimilarityScript;
import org.elasticsearch.script.SimilarityWeightScript;

import java.io.IOException;

/**
 * A {@link Similarity} implementation that allows scores to be scripted.
 */
public final class ScriptedSimilarity extends Similarity {

    final String weightScriptSource;
    final String scriptSource;
    final SimilarityWeightScript.Factory weightScriptFactory;
    final SimilarityScript.Factory scriptFactory;
    final boolean discountOverlaps;

    /** Sole constructor. */
    public ScriptedSimilarity(String weightScriptString, SimilarityWeightScript.Factory weightScriptFactory,
            String scriptString, SimilarityScript.Factory scriptFactory, boolean discountOverlaps) {
        this.weightScriptSource = weightScriptString;
        this.weightScriptFactory = weightScriptFactory;
        this.scriptSource = scriptString;
        this.scriptFactory = scriptFactory;
        this.discountOverlaps = discountOverlaps;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(weightScript=[" + weightScriptSource + "], script=[" + scriptSource + "])";
    }

    @Override
    public long computeNorm(FieldInvertState state) {
        final int numTerms = discountOverlaps ? state.getLength() - state.getNumOverlap() : state.getLength();
        return SmallFloat.intToByte4(numTerms);
    }

    @Override
    public SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        Query query = new Query(boost);
        long docCount = collectionStats.docCount();
        if (docCount == -1) {
            docCount = collectionStats.maxDoc();
        }
        Field field = new Field(docCount, collectionStats.sumDocFreq(), collectionStats.sumTotalTermFreq());
        Term[] terms = new Term[termStats.length];
        for (int i = 0; i < termStats.length; ++i) {
            terms[i] = new Term(termStats[i].docFreq(), termStats[i].totalTermFreq());
        }
        return new Weight(collectionStats.field(), query, field, terms);
    }

    /** Compute the part of the score that does not depend on the current document using the init_script. */
    private double computeWeight(Query query, Field field, Term term) throws IOException {
        if (weightScriptFactory == null) {
            return 1d;
        }
        SimilarityWeightScript weightScript = weightScriptFactory.newInstance();
        return weightScript.execute(query, field, term);
    }

    @Override
    public SimScorer simScorer(SimWeight w, LeafReaderContext context) throws IOException {
        Weight weight = (Weight) w;
        SimScorer[] scorers = new SimScorer[weight.terms.length];
        for (int i = 0; i < weight.terms.length; ++i) {
            final Term term = weight.terms[i];
            final SimilarityScript script = scriptFactory.newInstance();
            final NumericDocValues norms = context.reader().getNormValues(weight.fieldName);
            final Doc doc = new Doc(norms);
            final double scoreWeight = computeWeight(weight.query, weight.field, term);
            scorers[i] = new SimScorer() {

                @Override
                public float score(int docID, float freq) throws IOException {
                    doc.docID = docID;
                    doc.freq = freq;
                    return (float) script.execute(scoreWeight, weight.query, weight.field, term, doc);
                }

                @Override
                public float computeSlopFactor(int distance) {
                    return 1.0f / (distance + 1);
                }

                @Override
                public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
                    return 1f;
                }

                @Override
                public Explanation explain(int docID, Explanation freq) throws IOException {
                    doc.docID = docID;
                    float score = score(docID, freq.getValue());
                    return Explanation.match(score, "score from " + ScriptedSimilarity.this.toString() +
                            " computed from:",
                            Explanation.match((float) scoreWeight, "weight"),
                            Explanation.match(weight.query.boost, "query.boost"),
                            Explanation.match(weight.field.docCount, "field.docCount"),
                            Explanation.match(weight.field.sumDocFreq, "field.sumDocFreq"),
                            Explanation.match(weight.field.sumTotalTermFreq, "field.sumTotalTermFreq"),
                            Explanation.match(term.docFreq, "term.docFreq"),
                            Explanation.match(term.totalTermFreq, "term.totalTermFreq"),
                            Explanation.match(freq.getValue(), "doc.freq", freq.getDetails()),
                            Explanation.match(doc.getLength(), "doc.length"));
                }
            };
        }
        if (scorers.length == 1) {
            return scorers[0];
        } else {
            // Sum scores across terms like a BooleanQuery would do
            return new SimScorer() {

                @Override
                public float score(int doc, float freq) throws IOException {
                    double sum = 0;
                    for (SimScorer scorer : scorers) {
                        sum += scorer.score(doc, freq);
                    }
                    return (float) sum;
                }

                @Override
                public float computeSlopFactor(int distance) {
                    return 1.0f / (distance + 1);
                }

                @Override
                public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
                    return 1f;
                }

                @Override
                public Explanation explain(int doc, Explanation freq) throws IOException {
                    Explanation[] subs = new Explanation[scorers.length];
                    for (int i = 0; i < subs.length; ++i) {
                        subs[i] = scorers[i].explain(doc, freq);
                    }
                    return Explanation.match(score(doc, freq.getValue()), "Sum of:", subs);
                }
            };
        }
    }

    private static class Weight extends SimWeight {
        private final String fieldName;
        private final Query query;
        private final Field field;
        private final Term[] terms;

        Weight(String fieldName, Query query, Field field, Term[] terms) {
            this.fieldName = fieldName;
            this.query = query;
            this.field = field;
            this.terms = terms;
        }
    }

    /** Scoring factors that come from the query. */
    public static class Query {
        private final float boost;

        private Query(float boost) {
            this.boost = boost;
        }

        /** The boost of the query. It should typically be incorporated into the score as a multiplicative factor. */
        public float getBoost() {
            return boost;
        }
    }

    /** Statistics that are specific to a given field. */
    public static class Field {
        private final long docCount;
        private final long sumDocFreq;
        private final long sumTotalTermFreq;

        private Field(long docCount, long sumDocFreq, long sumTotalTermFreq) {
            this.docCount = docCount;
            this.sumDocFreq = sumDocFreq;
            this.sumTotalTermFreq = sumTotalTermFreq;
        }

        /** Return the number of documents that have a value for this field. */
        public long getDocCount() {
            return docCount;
        }

        /** Return the sum of {@link Term#getDocFreq()} for all terms that exist in this field,
         *  or {@code -1} if this statistic is not available. */
        public long getSumDocFreq() {
            return sumDocFreq;
        }

        /** Return the sum of {@link Term#getTotalTermFreq()} for all terms that exist in this field,
         *  or {@code -1} if this statistic is not available. */
        public long getSumTotalTermFreq() {
            return sumTotalTermFreq;
        }
    }

    /** Statistics that are specific to a given term. */
    public static class Term {
        private final long docFreq;
        private final long totalTermFreq;

        private Term(long docFreq, long totalTermFreq) {
            this.docFreq = docFreq;
            this.totalTermFreq = totalTermFreq;
        }

        /** Return the number of documents that contain this term in the index. */
        public long getDocFreq() {
            return docFreq;
        }

        /** Return the total number of occurrences of this term in the index, or {@code -1} if this statistic is not available. */
        public long getTotalTermFreq() {
            return totalTermFreq;
        }
    }

    /** Statistics that are specific to a document. */
    public static class Doc {
        private final NumericDocValues norms;
        private int docID;
        private float freq;

        private Doc(NumericDocValues norms) {
            this.norms = norms;
        }

        /** Return the number of tokens that the current document has in the considered field. */
        public int getLength() throws IOException {
            // the length is computed lazily so that similarities that do not use the length are
            // not penalized
            if (norms == null) {
                return 1;
            } else if (norms.advanceExact(docID)) {
                return SmallFloat.byte4ToInt((byte) norms.longValue());
            } else {
                return 0;
            }
        }

        /** Return the number of occurrences of the term in the current document for the considered field. */
        public float getFreq() {
            return freq;
        }
    }

}
