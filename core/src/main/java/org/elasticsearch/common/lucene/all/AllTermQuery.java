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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.payloads.AveragePayloadFunction;
import org.apache.lucene.search.payloads.PayloadTermQuery;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.TermSpans;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

import static org.apache.lucene.analysis.payloads.PayloadHelper.decodeFloat;

/**
 * A term query that takes all payload boost values into account.
 * <p>
 * It is like PayloadTermQuery with AveragePayloadFunction, except
 * unlike PayloadTermQuery, it doesn't plug into the similarity to
 * determine how the payload should be factored in, it just parses
 * the float and multiplies the average with the regular score.
 */
public final class AllTermQuery extends PayloadTermQuery {

    public AllTermQuery(Term term) {
        super(term, new AveragePayloadFunction());
    }

    @Override
    public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        // TODO: needsScores
        // we should be able to just return a regular SpanTermWeight, at most here if needsScores == false?
        return new AllTermWeight(this, searcher);
    }

    class AllTermWeight extends PayloadTermWeight {

        AllTermWeight(AllTermQuery query, IndexSearcher searcher) throws IOException {
            super(query, searcher);
        }

        @Override
        public AllTermSpanScorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            if (this.stats == null) {
                return null;
            }
            // we have a custom weight class, we must check in case something is wrong with _all
            Terms terms = context.reader().terms(query.getField());
            if (terms != null && terms.hasPositions() == false) {
                throw new IllegalStateException("field \"" + term.field() + "\" was indexed without position data; cannot run AllTermQuery (term=" + term.text() + ")");
            }
            TermSpans spans = (TermSpans) query.getSpans(context, acceptDocs, termContexts);
            if (spans == null) {
                return null;
            }
            SimScorer sloppySimScorer = similarity.simScorer(stats, context);
            return new AllTermSpanScorer(spans, this, sloppySimScorer);
        }

        class AllTermSpanScorer extends PayloadTermSpanScorer {
            final PostingsEnum postings;

            AllTermSpanScorer(TermSpans spans, SpanWeight weight, Similarity.SimScorer docScorer) throws IOException {
                super(spans, weight, docScorer);
                postings = spans.getPostings();
            }

            @Override
            protected void processPayload(Similarity similarity) throws IOException {
                // note: similarity is ignored here (we just use decodeFloat always).
                // this is the only difference between this class and PayloadTermQuery.
                if (spans.isPayloadAvailable()) {
                    BytesRef payload = postings.getPayload();
                    payloadScore += decodeFloat(payload.bytes, payload.offset);
                    payloadsSeen++;
                }
            }
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

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        boolean hasPayloads = false;
        for (LeafReaderContext context : reader.leaves()) {
            final Terms terms = context.reader().terms(term.field());
            if (terms.hasPayloads()) {
                hasPayloads = true;
                break;
            }
        }
        if (hasPayloads == false) {
            TermQuery rewritten = new TermQuery(term);
            rewritten.setBoost(getBoost());
            return rewritten;
        }
        return this;
    }

}
