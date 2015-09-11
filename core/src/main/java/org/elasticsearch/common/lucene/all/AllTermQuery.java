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
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
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
    public Query rewrite(IndexReader reader) throws IOException {
        if (getBoost() != 1f) {
            return super.rewrite(reader);
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
            Query rewritten = new MatchNoDocsQuery();
            rewritten.setBoost(getBoost());
            return rewritten;
        }
        if (hasPayloads == false) {
            TermQuery rewritten = new TermQuery(term);
            rewritten.setBoost(getBoost());
            return rewritten;
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
                    int newDoc = scorer.advance(doc);
                    if (newDoc == doc) {
                        float score = scorer.score();
                        float freq = scorer.freq();
                        SimScorer docScorer = similarity.simScorer(stats, context);
                        Explanation freqExplanation = Explanation.match(freq, "termFreq=" + freq);
                        Explanation termScoreExplanation = docScorer.explain(doc, freqExplanation);
                        Explanation payloadBoostExplanation = Explanation.match(scorer.payloadBoost(), "payloadBoost=" + scorer.payloadBoost());
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
                    } else {
                        assert payload.length == 4;
                        boost = PayloadHelper.decodeFloat(payload.bytes, payload.offset);
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
        public int nextDoc() throws IOException {
            return postings.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return postings.advance(target);
        }

        @Override
        public long cost() {
            return postings.cost();
        }
    }

    @Override
    public String toString(String field) {
        return new TermQuery(term).toString(field) + ToStringUtils.boost(getBoost());
    }

}
