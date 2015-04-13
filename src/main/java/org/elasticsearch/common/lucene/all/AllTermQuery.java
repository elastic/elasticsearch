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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
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
    public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        // TODO: needsScores
        // we should be able to just return a regular SpanTermWeight, at most here if needsScores == false?
        return new AllTermWeight(this, searcher);
    }

    protected class AllTermWeight extends SpanWeight {

        public AllTermWeight(AllTermQuery query, IndexSearcher searcher) throws IOException {
            super(query, searcher);
        }

        @Override
        public AllTermSpanScorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            if (this.stats == null) {
                return null;
            }
            TermSpans spans = (TermSpans) query.getSpans(context, acceptDocs, termContexts);
            if (spans == null) {
                return null;
            }
            SimScorer sloppySimScorer = similarity.simScorer(stats, context);
            return new AllTermSpanScorer(spans, this, sloppySimScorer);
        }

        protected class AllTermSpanScorer extends SpanScorer {
            protected PostingsEnum positions;
            protected float payloadScore;
            protected int payloadsSeen;

            public AllTermSpanScorer(TermSpans spans, SpanWeight weight, Similarity.SimScorer docScorer) throws IOException {
                super(spans, weight, docScorer);
                positions = spans.getPostings();
            }

            @Override
            protected void setFreqCurrentDoc() throws IOException {
                freq = 0.0f;
                numMatches = 0;
                payloadScore = 0;
                payloadsSeen = 0;

                assert spans.startPosition() == -1 : "incorrect initial start position, spans="+spans;
                assert spans.endPosition() == -1 : "incorrect initial end position, spans="+spans;
                int prevStartPos = -1;
                int prevEndPos = -1;

                int startPos = spans.nextStartPosition();
                assert startPos != Spans.NO_MORE_POSITIONS : "initial startPos NO_MORE_POSITIONS, spans="+spans;
                do {
                    assert startPos >= prevStartPos;
                    int endPos = spans.endPosition();
                    assert endPos != Spans.NO_MORE_POSITIONS;
                    // This assertion can fail for Or spans on the same term:
                    // assert (startPos != prevStartPos) || (endPos > prevEndPos) : "non increased endPos="+endPos;
                    assert (startPos != prevStartPos) || (endPos >= prevEndPos) : "decreased endPos="+endPos;
                    numMatches++;
                    int matchLength = endPos - startPos;
                    freq += docScorer.computeSlopFactor(matchLength);
                    processPayload();
                    prevStartPos = startPos;
                    prevEndPos = endPos;
                    startPos = spans.nextStartPosition();
                } while (startPos != Spans.NO_MORE_POSITIONS);

                assert spans.startPosition() == Spans.NO_MORE_POSITIONS : "incorrect final start position, spans="+spans;
                assert spans.endPosition() == Spans.NO_MORE_POSITIONS : "incorrect final end position, spans="+spans;
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
            public float scoreCurrentDoc() throws IOException {
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
                return super.scoreCurrentDoc();
            }

            /**
             * The score for the payload
             */
            protected float getPayloadScore() {
                return payloadsSeen > 0 ? (payloadScore / payloadsSeen) : 1;
            }

        }
        
        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException{
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
