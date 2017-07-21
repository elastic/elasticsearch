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
import org.elasticsearch.script.ExecutableScript;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * A {@link Similarity} implementation that allows scores to be scripted.
 */
public final class ScriptedSimilarity extends Similarity {

    private final String scriptString;
    private final Supplier<ExecutableScript> scriptSupplier;
    private final boolean discountOverlaps;

    ScriptedSimilarity(String scriptString, Supplier<ExecutableScript> scriptSupplier, boolean discountOverlaps) {
        this.scriptString = scriptString;
        this.scriptSupplier = scriptSupplier;
        this.discountOverlaps = discountOverlaps;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + scriptString + ")";
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

    @Override
    public SimScorer simScorer(SimWeight w, LeafReaderContext context) throws IOException {
        Weight weight = (Weight) w;
        SimScorer[] scorers = new SimScorer[weight.terms.length];
        for (int i = 0; i < weight.terms.length; ++i) {
            final Term term = weight.terms[i];
            final Doc doc = new Doc();
            final Stats stats = new Stats(weight.query, weight.field, term, doc);
            ExecutableScript script = scriptSupplier.get();
            script.setNextVar("stats", stats);
            final NumericDocValues norms = context.reader().getNormValues(weight.fieldName);
            scorers[i] = new SimScorer() {

                private int getLength(int docID) throws IOException {
                    if (norms == null) {
                        return 1;
                    } else if (norms.advanceExact(docID)) {
                        return SmallFloat.byte4ToInt((byte) norms.longValue());
                    } else {
                        return 0;
                    }
                }

                @Override
                public float score(int docID, float freq) throws IOException {
                    doc.freq = freq;
                    doc.length = getLength(docID);
                    Number score = (Number) script.run();
                    return score.floatValue();
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
                    float score = score(doc, freq.getValue());
                    return Explanation.match(score, "score from " + ScriptedSimilarity.this.toString() +
                            " on field [" + weight.fieldName + "], computed from:",
                            Explanation.match(weight.query.boost, "stats.query.boost"),
                            Explanation.match(weight.field.doc_count, "stats.field.docCount"),
                            Explanation.match(weight.field.sum_doc_freq, "stats.field.sumDocFreq"),
                            Explanation.match(weight.field.sum_total_term_freq, "stats.field.sumTotalTermFreq"),
                            Explanation.match(term.doc_freq, "stats.term.docFreq"),
                            Explanation.match(term.total_term_freq, "stats.term.totalTermFreq"),
                            Explanation.match(freq.getValue(), "stats.doc.freq", freq.getDetails()),
                            Explanation.match(getLength(doc), "stats.doc.length"));
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
        final String fieldName;
        final Query query;
        final Field field;
        final Term[] terms;

        Weight(String fieldName, Query query, Field field, Term[] terms) {
            this.fieldName = fieldName;
            this.query = query;
            this.field = field;
            this.terms = terms;
        }
    }

    public static class Stats {
        public final Query query;
        public final Field field;
        public final Term term;
        public final Doc doc;

        public Stats(Query query, Field field, Term term, Doc doc) {
            this.query = query;
            this.field = field;
            this.term = term;
            this.doc = doc;
        }

        public Query getQuery() {
            return query;
        }

        public Field getField() {
            return field;
        }

        public Term getTerm() {
            return term;
        }

        public Doc getDoc() {
            return doc;
        }
    }

    public static class Query {
        public final float boost;

        private Query(float boost) {
            this.boost = boost;
        }

        public float getBoost() {
            return boost;
        }
    }

    public static class Field {
        public final long doc_count;
        public final long sum_doc_freq;
        public final long sum_total_term_freq;

        private Field(long docCount, long sumDocFreq, long sumTotalTermFreq) {
            this.doc_count = docCount;
            this.sum_doc_freq = sumDocFreq;
            this.sum_total_term_freq = sumTotalTermFreq;
        }

        public long getDocCount() {
            return doc_count;
        }

        public long getSumDocFreq() {
            return sum_doc_freq;
        }

        public long getSumTotalTermFreq() {
            return sum_total_term_freq;
        }
    }

    public static class Term {
        public final long doc_freq;
        public final long total_term_freq;

        private Term(long docFreq, long totalTermFreq) {
            this.doc_freq = docFreq;
            this.total_term_freq = totalTermFreq;
        }

        public long getDocFreq() {
            return doc_freq;
        }

        public long getTotalTermFreq() {
            return total_term_freq;
        }
    }

    public static class Doc {
        public int length;
        public float freq;

        public int getLength() {
            return length;
        }

        public float getFreq() {
            return freq;
        }
    }

}
