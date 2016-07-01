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

package org.elasticsearch.common.lucene.all;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.similarities.Similarity.SimWeight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * A term query that takes all payload boost values into account.
 * <p>
 * It is like PayloadTermQuery with AveragePayloadFunction, except
 * unlike PayloadTermQuery, it doesn't plug into the similarity to
 * determine how the payload should be factored in, it just parses
 * the float and multiplies the average with the regular score.
 */
public final class AllTermQuery extends Query {

    private final Term term;

    public AllTermQuery(Term term) {
        this.term = term;
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        return Objects.equals(term, ((AllTermQuery) obj).term);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + term.hashCode();
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = super.rewrite(reader);
        if (rewritten != this) {
            return rewritten;
        }
        boolean fieldExists = false;
        boolean hasPayloads = false;
        for (LeafReaderContext context : reader.leaves()) {
            final Terms terms = context.reader().terms(term.field());
            if (terms != null) {
                fieldExists = true;
                if (terms.hasPayloads()) {
                    hasPayloads = true;
                    break;
                }
            }
        }
        if (fieldExists == false) {
            return new MatchNoDocsQuery();
        }
        if (hasPayloads == false) {
            return new TermQuery(term);
        }
        return this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        if (needsScores == false) {
            return new TermQuery(term).createWeight(searcher, needsScores);
        }
        final TermContext termStates = TermContext.build(searcher.getTopReaderContext(), term);
        final CollectionStatistics collectionStats = searcher.collectionStatistics(term.field());
        final TermStatistics termStats = searcher.termStatistics(term, termStates);
        final Similarity similarity = searcher.getSimilarity(needsScores);
        final SimWeight stats = similarity.computeWeight(collectionStats, termStats);
        return new Weight(this) {

            @Override
            public final float getValueForNormalization() throws IOException {
                return stats.getValueForNormalization();
            }

            @Override
            public final void normalize(float norm, float topLevelBoost) {
                stats.normalize(norm, topLevelBoost);
            }

            @Override
            public void extractTerms(Set<Term> terms) {
                terms.add(term);
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                AllTermScorer scorer = scorer(context);
                if (scorer != null) {
                    int newDoc = scorer.iterator().advance(doc);
                    if (newDoc == doc) {
                        float score = scorer.score();
                        float freq = scorer.freq();
                        SimScorer docScorer = similarity.simScorer(stats, context);
                        Explanation freqExplanation = Explanation.match(freq, "termFreq=" + freq);
                        Explanation termScoreExplanation = docScorer.explain(doc, freqExplanation);
                        Explanation payloadBoostExplanation =
                            Explanation.match(scorer.payloadBoost(), "payloadBoost=" + scorer.payloadBoost());
                        return Explanation.match(
                                score,
                                "weight(" + getQuery() + " in " + doc + ") ["
                                        + similarity.getClass().getSimpleName() + "], product of:",
                                        termScoreExplanation, payloadBoostExplanation);
                    }
                }
                return Explanation.noMatch("no matching term");
            }

            @Override
            public AllTermScorer scorer(LeafReaderContext context) throws IOException {
                final Terms terms = context.reader().terms(term.field());
                if (terms == null) {
                    return null;
                }
                final TermsEnum termsEnum = terms.iterator();
                if (termsEnum == null) {
                    return null;
                }
                final TermState state = termStates.get(context.ord);
                if (state == null) {
                    // Term does not exist in this segment
                    return null;
                }
                termsEnum.seekExact(term.bytes(), state);
                PostingsEnum docs = termsEnum.postings(null, PostingsEnum.PAYLOADS);
                assert docs != null;
                return new AllTermScorer(this, docs, similarity.simScorer(stats, context));
            }

        };
    }

    private static class AllTermScorer extends Scorer {

        final PostingsEnum postings;
        final Similarity.SimScorer docScorer;
        int doc = -1;
        float payloadBoost;

        AllTermScorer(Weight weight, PostingsEnum postings, Similarity.SimScorer docScorer) {
            super(weight);
            this.postings = postings;
            this.docScorer = docScorer;
        }

        float payloadBoost() throws IOException {
            if (doc != docID()) {
                final int freq = postings.freq();
                payloadBoost = 0;
                for (int i = 0; i < freq; ++i) {
                    postings.nextPosition();
                    final BytesRef payload = postings.getPayload();
                    float boost;
                    if (payload == null) {
                        boost = 1;
                    } else if (payload.length == 1) {
                        boost = SmallFloat.byte315ToFloat(payload.bytes[payload.offset]);
                    } else if (payload.length == 4) {
                        // TODO: for bw compat only, remove this in 6.0
                        boost = PayloadHelper.decodeFloat(payload.bytes, payload.offset);
                    } else {
                        throw new IllegalStateException("Payloads are expected to have a length of 1 or 4 but got: "
                            + payload);
                    }
                    payloadBoost += boost;
                }
                payloadBoost /= freq;
                doc = docID();
            }
            return payloadBoost;
        }

        @Override
        public float score() throws IOException {
            return payloadBoost() * docScorer.score(postings.docID(), postings.freq());
        }

        @Override
        public int freq() throws IOException {
            return postings.freq();
        }

        @Override
        public int docID() {
            return postings.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
            return postings;
        }
    }

    @Override
    public String toString(String field) {
        return new TermQuery(term).toString(field);
    }

}
