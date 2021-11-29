/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.randomsample;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.xpack.ml.math.FastGeometric;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;

/**
 * A query that randomly matches documents with a user-provided probability within a geometric distribution
 */
public final class RandomSamplingQuery extends Query {

    private final double p;
    private final IntSupplier rng;
    private final boolean cacheable;
    private final Query query;

    /**
     * @param p         The sampling probability e.g. 0.05 == 5% probability a document will match
     * @param rng       Random int supplier
     * @param cacheable True if the seed is static (provided by the user) so that we can cache the query, false otherwise
     */
    RandomSamplingQuery(double p, IntSupplier rng, boolean cacheable, Query query) {
        if (p <= 0.0 || p >= 1.0) {
            throw new IllegalArgumentException("RandomSampling probability must be between 0.0 and 1.0");
        }

        this.p = p;
        this.rng = rng;
        this.cacheable = cacheable;
        this.query = query;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (query == null) {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public Scorer scorer(LeafReaderContext context) {
                    int maxDoc = context.reader().maxDoc();
                    return new ConstantScoreScorer(this, score(), ScoreMode.COMPLETE_NO_SCORES, new RandomSamplingIterator(maxDoc, p, rng));
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return cacheable;
                }
            };
        } else {
            final Weight innerWeight = query.createWeight(searcher, scoreMode, boost);
            return new ConstantScoreWeight(query, boost) {
                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    int maxDoc = context.reader().maxDoc();
                    Scorer scorer = new ConstantScoreScorer(
                        this,
                        score(),
                        ScoreMode.COMPLETE_NO_SCORES,
                        new RandomSamplingIterator(maxDoc, p, rng)
                    );
                    Scorer queryScorer = innerWeight.scorer(context);
                    if (queryScorer == null) {
                        return null;
                    }
                    return new ConstantScoreScorer(
                        this,
                        score(),
                        ScoreMode.COMPLETE,
                        ConjunctionUtils.intersectScorers(List.of(scorer, queryScorer))
                    );
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return cacheable;
                }
            };
        }
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (query != null) {
            query.visit(visitor.getSubVisitor(BooleanClause.Occur.FILTER, this));
        }
    }

    /**
     * A DocIDSetIter that skips a geometrically random number of documents
     */
    static class RandomSamplingIterator extends DocIdSetIterator {
        private final int maxDoc;
        private final double p;
        private final FastGeometric distribution;
        private int doc = -1;

        RandomSamplingIterator(int maxDoc, double p, IntSupplier rng) {
            this.maxDoc = maxDoc;
            this.p = p;
            this.distribution = new FastGeometric(rng, p);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            int next = target + distribution.next();
            doc = next < maxDoc ? next : NO_MORE_DOCS;
            return doc;
        }

        @Override
        public long cost() {
            return (long) (maxDoc * p);
        }
    }

    @Override
    public String toString(String field) {
        return "RandomSamplingQuery{" + "p=" + p + ", cacheable=" + cacheable + ", query=" + query + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RandomSamplingQuery that = (RandomSamplingQuery) o;
        return Double.compare(that.p, p) == 0 && cacheable == that.cacheable && Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(p, cacheable, query);
    }
}
