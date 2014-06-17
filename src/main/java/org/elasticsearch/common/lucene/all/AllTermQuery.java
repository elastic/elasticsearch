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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.TermSpans;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

import static org.apache.lucene.analysis.payloads.PayloadHelper.decodeFloat;

/**
 * A term query that takes all payload boost values into account.
 *
 *
 */
public class AllTermQuery extends SpanTermQuery {

    public AllTermQuery(Term term) {
        super(term);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        return new AllTermWeight(this, searcher);
    }

    protected class AllTermWeight extends SpanWeight {

        public AllTermWeight(AllTermQuery query, IndexSearcher searcher) throws IOException {
            super(query, searcher);
        }

        @Override
        public AllTermSpanScorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            if (this.stats == null) {
                return null;
            }
            SimScorer sloppySimScorer = similarity.simScorer(stats, context);
            return new AllTermSpanScorer((TermSpans) query.getSpans(context, acceptDocs, termContexts), this, sloppySimScorer);
        }

        protected class AllTermSpanScorer extends SpanScorer {
            protected DocsAndPositionsEnum positions;
            protected float payloadScore;
            protected int payloadsSeen;

            public AllTermSpanScorer(TermSpans spans, Weight weight, Similarity.SimScorer docScorer) throws IOException {
                super(spans, weight, docScorer);
                positions = spans.getPostings();
            }

            @Override
            protected boolean setFreqCurrentDoc() throws IOException {
                if (!more) {
                    return false;
                }
                doc = spans.doc();
                freq = 0.0f;
                numMatches = 0;
                payloadScore = 0;
                payloadsSeen = 0;
                do {
                    int matchLength = spans.end() - spans.start();

                    freq += docScorer.computeSlopFactor(matchLength);
                    numMatches++;
                    processPayload();

                    more = spans.next();// this moves positions to the next match
                } while (more && (doc == spans.doc()));
                return true;
            }

            protected void processPayload() throws IOException {
                final BytesRef payload;
                if ((payload = positions.getPayload()) != null) {
                    payloadScore += decodeFloat(payload.bytes, payload.offset);
                    payloadsSeen++;

                } else {
                    // zero out the payload?
                }
            }

            /**
             * @return {@link #getSpanScore()} * {@link #getPayloadScore()}
             * @throws IOException
             */
            @Override
            public float score() throws IOException {
                return getSpanScore() * getPayloadScore();
            }

            /**
             * Returns the SpanScorer score only.
             * <p/>
             * Should not be overridden without good cause!
             *
             * @return the score for just the Span part w/o the payload
             * @throws IOException
             * @see #score()
             */
            protected float getSpanScore() throws IOException {
                return super.score();
            }

            /**
             * The score for the payload
             */
            protected float getPayloadScore() {
                return payloadsSeen > 0 ? (payloadScore / payloadsSeen) : 1;
            }

        }
        
        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException{
            AllTermSpanScorer scorer = scorer(context, context.reader().getLiveDocs());
            if (scorer != null) {
              int newDoc = scorer.advance(doc);
              if (newDoc == doc) {
                float freq = scorer.sloppyFreq();
                SimScorer docScorer = similarity.simScorer(stats, context);
                ComplexExplanation inner = new ComplexExplanation();
                inner.setDescription("weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:");
                Explanation scoreExplanation = docScorer.explain(doc, new Explanation(freq, "phraseFreq=" + freq));
                inner.addDetail(scoreExplanation);
                inner.setValue(scoreExplanation.getValue());
                inner.setMatch(true);
                ComplexExplanation result = new ComplexExplanation();
                result.addDetail(inner);
                Explanation payloadBoost = new Explanation();
                result.addDetail(payloadBoost);
                final float payloadScore = scorer.getPayloadScore();
                payloadBoost.setValue(payloadScore);
                // GSI: I suppose we could toString the payload, but I don't think that
                // would be a good idea
                payloadBoost.setDescription("allPayload(...)");
                result.setValue(inner.getValue() * payloadScore);
                result.setDescription("btq, product of:");
                return result;
              }
            }
            
            return new ComplexExplanation(false, 0.0f, "no matching term");
            
            
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() + 1;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        return true;
    }

}
