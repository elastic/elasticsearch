/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.similarity;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.SmallFloat;
import org.elasticsearch.script.SimilarityScript;
import org.elasticsearch.script.SimilarityWeightScript;

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
    public ScriptedSimilarity(
        String weightScriptString,
        SimilarityWeightScript.Factory weightScriptFactory,
        String scriptString,
        SimilarityScript.Factory scriptFactory,
        boolean discountOverlaps
    ) {
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

    /** Compute the part of the score that does not depend on the current document using the init_script. */
    private double computeWeight(Query query, Field field, Term term) {
        if (weightScriptFactory == null) {
            return 1d;
        }
        SimilarityWeightScript weightScript = weightScriptFactory.newInstance();
        return weightScript.execute(query, field, term);
    }

    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
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

        SimScorer[] scorers = new SimScorer[terms.length];
        for (int i = 0; i < terms.length; ++i) {
            final Term term = terms[i];
            final SimilarityScript script = scriptFactory.newInstance();
            final Doc doc = new Doc();
            final double scoreWeight = computeWeight(query, field, term);
            scorers[i] = new SimScorer() {

                @Override
                public float score(float freq, long norm) {
                    doc.freq = freq;
                    doc.norm = norm;
                    return (float) script.execute(scoreWeight, query, field, term, doc);
                }

                @Override
                public Explanation explain(Explanation freq, long norm) {
                    float score = score(freq.getValue().floatValue(), norm);
                    return Explanation.match(
                        score,
                        "score from " + ScriptedSimilarity.this.toString() + " computed from:",
                        Explanation.match((float) scoreWeight, "weight"),
                        Explanation.match(query.boost, "query.boost"),
                        Explanation.match(field.docCount, "field.docCount"),
                        Explanation.match(field.sumDocFreq, "field.sumDocFreq"),
                        Explanation.match(field.sumTotalTermFreq, "field.sumTotalTermFreq"),
                        Explanation.match(term.docFreq, "term.docFreq"),
                        Explanation.match(term.totalTermFreq, "term.totalTermFreq"),
                        Explanation.match(freq.getValue(), "doc.freq", freq.getDetails()),
                        Explanation.match(doc.getLength(), "doc.length")
                    );
                }
            };
        }
        if (scorers.length == 1) {
            return scorers[0];
        } else {
            // Sum scores across terms like a BooleanQuery would do
            return new SimScorer() {

                @Override
                public float score(float freq, long norm) {
                    double sum = 0;
                    for (SimScorer scorer : scorers) {
                        sum += scorer.score(freq, norm);
                    }
                    return (float) sum;
                }

                @Override
                public Explanation explain(Explanation freq, long norm) {
                    Explanation[] subs = new Explanation[scorers.length];
                    for (int i = 0; i < subs.length; ++i) {
                        subs[i] = scorers[i].explain(freq, norm);
                    }
                    return Explanation.match(score(freq.getValue().floatValue(), norm), "Sum of:", subs);
                }
            };
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
        private float freq;
        private long norm;

        private Doc() {}

        /** Return the number of tokens that the current document has in the considered field. */
        public int getLength() {
            // the length is computed lazily so that similarities that do not use the length are
            // not penalized
            return SmallFloat.byte4ToInt((byte) norm);
        }

        /** Return the number of occurrences of the term in the current document for the considered field. */
        public float getFreq() {
            return freq;
        }
    }

}
