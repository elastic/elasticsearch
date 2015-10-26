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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link Query} wrapper that only emits as hits documents whose score is
 * above a given threshold. This query only really makes sense for queries
 * whose score is computed manually, like eg. function score queries.
 */
public final class MinScoreQuery extends Query {

    private final Query query;
    private final float minScore;
    private final IndexSearcher searcher;

    /** Sole constructor. */
    public MinScoreQuery(Query query, float minScore) {
        this(query, minScore, null);
    }

    MinScoreQuery(Query query, float minScore, IndexSearcher searcher) {
        this.query = query;
        this.minScore = minScore;
        this.searcher = searcher;
    }

    /** Return the wrapped query. */
    public Query getQuery() {
        return query;
    }

    /** Return the minimum score. */
    public float getMinScore() {
        return minScore;
    }

    @Override
    public String toString(String field) {
        return getClass().getSimpleName() + "(" + query.toString(field) + ", minScore=" + minScore + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        MinScoreQuery that = (MinScoreQuery) obj;
        return minScore == that.minScore
                && searcher == that.searcher
                && query.equals(that.query);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(query, minScore, searcher);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (getBoost() != 1f) {
            return super.rewrite(reader);
        }
        Query rewritten = query.rewrite(reader);
        if (rewritten != query) {
            return new MinScoreQuery(rewritten, minScore);
        }
        return super.rewrite(reader);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        final Weight weight = searcher.createWeight(query, true);
        // We specialize the query for the provided index searcher because it
        // can't really be cached as the documents that match depend on the
        // Similarity implementation and the top-level reader.
        final Query key = new MinScoreQuery(query, minScore, searcher);
        return new Weight(key) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                Scorer scorer = weight.scorer(context);
                if (scorer == null) {
                    return null;
                }
                return new MinScoreScorer(this, scorer, minScore);
            }

            @Override
            public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
                BulkScorer bulkScorer = weight.bulkScorer(context);
                if (bulkScorer == null) {
                    return null;
                }
                return new MinScoreBulkScorer(bulkScorer, minScore);
            }

            @Override
            public void normalize(float norm, float boost) {
                weight.normalize(norm, boost);
            }

            @Override
            public float getValueForNormalization() throws IOException {
                return weight.getValueForNormalization();
            }

            @Override
            public void extractTerms(Set<Term> terms) {
                weight.extractTerms(terms);
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                Explanation expl = weight.explain(context, doc);
                if (expl.isMatch() == false || expl.getValue() >= minScore) {
                    return expl;
                } else {
                    return Explanation.noMatch("Min score is less than the configured min score=" + minScore, expl);
                }
            }
        };
    }

    private static class MinScoreScorer extends Scorer {

        private final Scorer scorer;
        private final float minScore;
        private float score;

        protected MinScoreScorer(Weight weight, Scorer scorer, float minScore) {
            super(weight);
            this.scorer = scorer;
            this.minScore = minScore;
        }

        @Override
        public float score() throws IOException {
            return score;
        }

        @Override
        public int freq() throws IOException {
            return scorer.freq();
        }

        @Override
        public int docID() {
            return scorer.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return doNext(scorer.nextDoc());
        }

        @Override
        public int advance(int target) throws IOException {
            return doNext(scorer.advance(target));
        }

        private int doNext(int doc) throws IOException {
            for (; doc != NO_MORE_DOCS; doc = scorer.nextDoc()) {
                final float score = scorer.score();
                if (score >= minScore) {
                    this.score = score;
                    return doc;
                }
            }
            return NO_MORE_DOCS;
        }

        @Override
        public TwoPhaseIterator asTwoPhaseIterator() {
            final TwoPhaseIterator twoPhase = scorer.asTwoPhaseIterator();
            final DocIdSetIterator approximation = twoPhase == null
                    ? scorer
                    : twoPhase.approximation();
            return new TwoPhaseIterator(approximation) {
                @Override
                public boolean matches() throws IOException {
                    if (twoPhase != null && twoPhase.matches() == false) {
                        return false;
                    }
                    score = scorer.score();
                    return score >= minScore;
                }
            };
        }

        @Override
        public long cost() {
            return scorer.cost();
        }

    }

    private static class MinScoreBulkScorer extends BulkScorer {

        private final BulkScorer bulkScorer;
        private final float minScore;

        public MinScoreBulkScorer(BulkScorer bulkScorer, float minScore) {
            this.bulkScorer = bulkScorer;
            this.minScore = minScore;
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            return bulkScorer.score(new MinScoreLeafCollector(collector, minScore), acceptDocs, min, max);
        }

        @Override
        public long cost() {
            return bulkScorer.cost();
        }

    }

    private static class MinScoreLeafCollector implements LeafCollector {

        private final LeafCollector collector;
        private final float minScore;
        private Scorer scorer;

        public MinScoreLeafCollector(LeafCollector collector, float minScore) {
            this.collector = collector;
            this.minScore = minScore;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            // we will need scores at least once, maybe more due to the wrapped
            // collector, so we wrap with a ScoreCachingWrappingScorer
            if (scorer instanceof ScoreCachingWrappingScorer == false) {
                scorer = new ScoreCachingWrappingScorer(scorer);
            }
            this.scorer = scorer;
            collector.setScorer(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
            if (scorer.score() >= minScore) {
                collector.collect(doc);
            }
        }

    }
}
