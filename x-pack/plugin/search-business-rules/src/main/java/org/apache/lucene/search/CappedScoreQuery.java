/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;

/**
 * A query that wraps another query and ensures scores do not exceed a maximum value
 */
public final class CappedScoreQuery extends Query {
    private final Query query;
    private final float maxScore;

    /** Caps scores from the passed in Query to the supplied maxScore parameter */
    public CappedScoreQuery(Query query, float maxScore) {
        this.query = Objects.requireNonNull(query, "Query must not be null");
        if (maxScore > 0 == false) {
            throw new IllegalArgumentException(this.getClass().getName() + " maxScore must be >0, " + maxScore + " supplied.");
        }
        this.maxScore = maxScore;
    }

    /** Returns the encapsulated query. */
    public Query getQuery() {
        return query;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = query.rewrite(reader);

        if (rewritten != query) {
            return new CappedScoreQuery(rewritten, maxScore);
        }

        if (rewritten.getClass() == CappedScoreQuery.class) {
            return rewritten;
        }

        if (rewritten.getClass() == BoostQuery.class) {
            return new CappedScoreQuery(((BoostQuery) rewritten).getQuery(), maxScore);
        }

        return super.rewrite(reader);
    }

    /**
     * We return this as our {@link BulkScorer} so that if the CSQ wraps a query with its own optimized top-level scorer (e.g.
     * BooleanScorer) we can use that top-level scorer.
     */
    protected static class CappedBulkScorer extends BulkScorer {
        final BulkScorer bulkScorer;
        final Weight weight;
        final float maxScore;

        public CappedBulkScorer(BulkScorer bulkScorer, Weight weight, float maxScore) {
            this.bulkScorer = bulkScorer;
            this.weight = weight;
            this.maxScore = maxScore;
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            return bulkScorer.score(wrapCollector(collector), acceptDocs, min, max);
        }

        private LeafCollector wrapCollector(LeafCollector collector) {
            return new FilterLeafCollector(collector) {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    // we must wrap again here, but using the scorer passed in as parameter:
                    in.setScorer(new FilterScorable(scorer) {
                        @Override
                        public float score() throws IOException {
                            return Math.min(maxScore, in.score());
                        }

                        @Override
                        public void setMinCompetitiveScore(float minScore) throws IOException {
                            scorer.setMinCompetitiveScore(minScore);
                        }
                        
                    });
                }
            };
        }

        @Override
        public long cost() {
            return bulkScorer.cost();
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final Weight innerWeight = searcher.createWeight(query, scoreMode, boost);
        if (scoreMode.needsScores()) {
            return new CappedScoreWeight(this, innerWeight, maxScore) {
                @Override
                public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
                    final BulkScorer innerScorer = innerWeight.bulkScorer(context);
                    if (innerScorer == null) {
                        return null;
                    }
                    return new CappedBulkScorer(innerScorer, this, maxScore);
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    ScorerSupplier innerScorerSupplier = innerWeight.scorerSupplier(context);
                    if (innerScorerSupplier == null) {
                        return null;
                    }
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            final Scorer innerScorer = innerScorerSupplier.get(leadCost);
                            // short-circuit if scores will not need capping
                            innerScorer.advanceShallow(0);
                            if (innerScorer.getMaxScore(DocIdSetIterator.NO_MORE_DOCS) <= maxScore) {
                              return innerScorer;
                            }                            
                            return new CappedScorer(innerWeight, innerScorer, maxScore);
                        }

                        @Override
                        public long cost() {
                            return innerScorerSupplier.cost();
                        }
                    };
                }

                @Override
                public Matches matches(LeafReaderContext context, int doc) throws IOException {
                    return innerWeight.matches(context, doc);
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    ScorerSupplier scorerSupplier = scorerSupplier(context);
                    if (scorerSupplier == null) {
                        return null;
                    }
                    return scorerSupplier.get(Long.MAX_VALUE);
                }
            };
        } else {
            return innerWeight;
        }
    }

    @Override
    public String toString(String field) {
        return new StringBuilder("CappedScore(").append(query.toString(field)).append(')').toString();
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && maxScore == ((CappedScoreQuery) other).maxScore && 
                query.equals(((CappedScoreQuery) other).query);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + query.hashCode() + Float.hashCode(maxScore);
    }
}
