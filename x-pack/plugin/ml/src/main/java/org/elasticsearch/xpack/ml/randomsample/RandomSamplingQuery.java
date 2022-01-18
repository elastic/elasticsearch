/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.randomsample;

import com.carrotsearch.hppc.BitMixer;

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
import java.util.SplittableRandom;
import java.util.function.IntSupplier;

/**
 * A query that randomly matches documents with a user-provided probability within a geometric distribution
 */
public final class RandomSamplingQuery extends Query {

    private final double p;
    private final Query query;
    private final SplittableRandom splittableRandom;
    private final int seed;
    private final int hash;

    /**
     * @param p         The sampling probability e.g. 0.05 == 5% probability a document will match
     * @param seed      The seed from the builder
     * @param hash      A unique hash so that if the same seed is used between multiple queries, unique random number streams
     *                  can be generated
     * @param query     An additional filter to apply to the resulting document set
     */
    public RandomSamplingQuery(double p, int seed, int hash, Query query) {
        if (p <= 0.0 || p >= 1.0) {
            throw new IllegalArgumentException("RandomSampling probability must be between 0.0 and 1.0");
        }
        this.p = p;
        this.query = query;
        this.seed = seed;
        this.hash = hash;
        this.splittableRandom = new SplittableRandom(BitMixer.mix(hash, seed));
    }

    @Override
    public String toString(String field) {
        return "RandomSamplingQuery{" + "p=" + p + ", query=" + query + ", seed=" + seed + ", hash=" + hash + '}';
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (query == null) {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }

                @Override
                public Scorer scorer(LeafReaderContext context) {
                    final SplittableRandom random = splittableRandom.split();
                    int maxDoc = context.reader().maxDoc();
                    return new ConstantScoreScorer(
                        this,
                        score(),
                        ScoreMode.COMPLETE_NO_SCORES,
                        new RandomSamplingIterator(maxDoc, p, random::nextInt)
                    );
                }
            };
        } else {
            final Weight innerWeight = searcher.createWeight(query, scoreMode, boost);
            return new ConstantScoreWeight(query, boost) {
                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    final SplittableRandom random = splittableRandom.split();
                    int maxDoc = context.reader().maxDoc();
                    Scorer scorer = new ConstantScoreScorer(
                        this,
                        score(),
                        ScoreMode.COMPLETE_NO_SCORES,
                        new RandomSamplingIterator(maxDoc, p, random::nextInt)
                    );
                    Scorer queryScorer = innerWeight.scorer(context);
                    if (queryScorer == null) {
                        return null;
                    }
                    return new ConstantScoreScorer(
                        this,
                        score(),
                        ScoreMode.COMPLETE_NO_SCORES,
                        ConjunctionUtils.intersectScorers(List.of(scorer, queryScorer))
                    );
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
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
            while (doc < target && doc < maxDoc) {
                doc += distribution.next();
            }
            doc = doc < maxDoc ? doc : NO_MORE_DOCS;
            return doc;
        }

        @Override
        public long cost() {
            return (long) (maxDoc * p);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RandomSamplingQuery that = (RandomSamplingQuery) o;
        return Double.compare(that.p, p) == 0 && seed == that.seed && hash == that.seed && Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(p, seed, hash, query);
    }
}
