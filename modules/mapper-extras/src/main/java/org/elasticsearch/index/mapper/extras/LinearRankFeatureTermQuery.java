/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.util.Objects;

/**
 * Lucene query to be used by {@link RankFeaturesFieldMapper}  when creating term queries
 *
 * This is adapted from Lucene's FeatureQuery query and extends TermQuery
 */
class LinearRankFeatureTermQuery extends TermQuery {
    private static final int MAX_FREQ = Float.floatToIntBits(Float.MAX_VALUE) >>> 15;

    static float decodeFeatureValue(float freq) {
        if (freq > MAX_FREQ) {
            return Float.MAX_VALUE;
        }
        int tf = (int) freq;
        int featureBits = tf << 15;
        return Float.intBitsToFloat(featureBits);
    }

    private final Term term;

    LinearRankFeatureTermQuery(Term term) {
        super(term);
        this.term = Objects.requireNonNull(term);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final IndexReaderContext context = searcher.getTopReaderContext();
        final TermStates termState = TermStates.build(context, term, scoreMode.needsScores());
        return new LinearRankFeatureTermWeight(scoreMode, boost, termState);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LinearRankFeatureTermQuery that = (LinearRankFeatureTermQuery) obj;
        return Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
        int h = getClass().hashCode();
        h = 31 * h + term.hashCode();
        return h;
    }

    @Override
    public String toString(String field) {
        return "LinearRankFeatureTermQuery(field=" + term.field() + ", feature=" + term.text() + ")";
    }

    private static class LinearScorer extends Similarity.SimScorer {
        private final float boost;

        LinearScorer(float boost) {
            this.boost = boost;
        }

        @Override
        public float score(float freq, long norm) {
            return decodeFeatureValue(freq) * boost;
        }

        @Override
        public Explanation explain(Explanation freq, long norm) {
            float score = score(freq.getValue().floatValue(), 1L);
            return Explanation.match(
                score,
                "LinearScorer(featureValue=" + decodeFeatureValue(freq.getValue().floatValue()) + ", weight=" + boost + ")"
            );
        }

        @Override
        public String toString() {
            return "LinearScorer";
        }
    }

    // Based on Lucene package private and final class TermWeight, but with specific scorer & optimizations
    class LinearRankFeatureTermWeight extends Weight {
        private final ScoreMode scoreMode;
        private final Similarity.SimScorer simScorer;
        private final TermStates termStates;

        LinearRankFeatureTermWeight(ScoreMode scoreMode, float boost, TermStates termStates) {
            super(LinearRankFeatureTermQuery.this);
            this.scoreMode = scoreMode;
            this.simScorer = new LinearScorer(boost);
            this.termStates = termStates;
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            TermsEnum te = getTermsEnum(context);
            if (te == null) {
                return null;
            }
            return MatchesUtils.forField(term.field(), () -> {
                PostingsEnum pe = te.postings(null, PostingsEnum.OFFSETS);
                if (pe.advance(doc) != doc) {
                    return null;
                }
                return new TermMatchesIterator(getQuery(), pe);
            });
        }

        @Override
        public String toString() {
            return "weight(" + LinearRankFeatureTermQuery.this + ")";
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            final TermsEnum termsEnum = getTermsEnum(context);
            if (termsEnum == null) {
                return null;
            }
            LeafSimScorer scorer = new LeafSimScorer(simScorer, context.reader(), term.field(), scoreMode.needsScores());
            if (scoreMode == ScoreMode.TOP_SCORES) {
                return new TermScorer(this, termsEnum.impacts(PostingsEnum.FREQS), scorer);
            } else {
                return new TermScorer(
                    this,
                    termsEnum.postings(null, scoreMode.needsScores() ? PostingsEnum.FREQS : PostingsEnum.NONE),
                    scorer
                );
            }
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }

        /**
         * Returns a {@link TermsEnum} positioned at this weights Term or null if the term does not
         * exist in the given context
         */
        private TermsEnum getTermsEnum(LeafReaderContext context) throws IOException {
            assert termStates != null;
            assert termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context))
                : "The top-reader used to create Weight is not the same as the current reader's top-reader ("
                    + ReaderUtil.getTopLevelContext(context);
            final TermState state = termStates.get(context);
            if (state == null) { // term is not present in that reader
                assert termNotInReader(context.reader(), term) : "no termstate found but term exists in reader term=" + term;
                return null;
            }
            final TermsEnum termsEnum = context.reader().terms(term.field()).iterator();
            termsEnum.seekExact(term.bytes(), state);
            return termsEnum;
        }

        private boolean termNotInReader(LeafReader reader, Term term) throws IOException {
            return reader.docFreq(term) == 0;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            String desc = "weight(" + getQuery() + " in " + doc + ") [" + simScorer + "]";

            Terms terms = context.reader().terms(term.field());
            if (terms == null) {
                return Explanation.noMatch(desc + ". Field " + term.field() + " doesn't exist.");
            }
            final TermsEnum termsEnum = getTermsEnum(context);
            if (termsEnum == null) {
                return Explanation.noMatch(desc + ". Feature " + term.text() + " doesn't exist.");
            }
            LeafSimScorer scorer = new LeafSimScorer(simScorer, context.reader(), term.field(), scoreMode.needsScores());
            final TermScorer termScorer;
            if (scoreMode == ScoreMode.TOP_SCORES) {
                termScorer = new TermScorer(this, termsEnum.impacts(PostingsEnum.FREQS), scorer);
            } else {
                termScorer = new TermScorer(
                    this,
                    termsEnum.postings(null, scoreMode.needsScores() ? PostingsEnum.FREQS : PostingsEnum.NONE),
                    scorer
                );
            }
            int newDoc = termScorer.iterator().advance(doc);
            if (newDoc == doc) {
                float freq = termScorer.freq();
                LeafSimScorer docScorer = new LeafSimScorer(simScorer, context.reader(), term.field(), true);
                Explanation freqExplanation = Explanation.match(freq, "frequency used to store featureValue");
                Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
                return Explanation.match(
                    scoreExplanation.getValue(),
                    "weight(" + getQuery() + " in " + doc + ") [SimpleLinearScorer], result of:",
                    scoreExplanation
                );
            }
            return Explanation.noMatch("no matching term");
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            if (context.reader().hasDeletions() == false) {
                TermsEnum termsEnum = getTermsEnum(context);
                // termsEnum is not null if term state is available
                if (termsEnum != null) {
                    return termsEnum.docFreq();
                } else {
                    // the term cannot be found in the dictionary so the count is 0
                    return 0;
                }
            } else {
                return super.count(context);
            }
        }
    }

    // Copied from private package Lucene TermMatchesIterator
    private static class TermMatchesIterator implements MatchesIterator {

        private int upto;
        private int pos;
        private final PostingsEnum pe;
        private final Query query;

        TermMatchesIterator(Query query, PostingsEnum pe) throws IOException {
            this.pe = pe;
            this.query = query;
            this.upto = pe.freq();
        }

        @Override
        public boolean next() throws IOException {
            if (upto-- > 0) {
                pos = pe.nextPosition();
                return true;
            }
            return false;
        }

        @Override
        public int startPosition() {
            return pos;
        }

        @Override
        public int endPosition() {
            return pos;
        }

        @Override
        public int startOffset() throws IOException {
            return pe.startOffset();
        }

        @Override
        public int endOffset() throws IOException {
            return pe.endOffset();
        }

        @Override
        public MatchesIterator getSubMatches() {
            return null;
        }

        @Override
        public Query getQuery() {
            return query;
        }
    }
}
