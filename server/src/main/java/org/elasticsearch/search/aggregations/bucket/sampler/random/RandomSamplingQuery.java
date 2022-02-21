/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.function.IntSupplier;

/**
 * A query that randomly matches documents with a user-provided probability within a geometric distribution
 */
public final class RandomSamplingQuery extends Query {

    private final double p;
    private final int seed;
    private final int hash;

    /**
     * @param p         The sampling probability e.g. 0.05 == 5% probability a document will match
     * @param seed      The seed from the builder
     * @param hash      A unique hash so that if the same seed is used between multiple queries, unique random number streams
     *                  can be generated
     */
    public RandomSamplingQuery(double p, int seed, int hash) {
        if (p <= 0.0 || p >= 1.0) {
            throw new IllegalArgumentException("RandomSampling probability must be between 0.0 and 1.0, was [" + p + "]");
        }
        this.p = p;
        this.seed = seed;
        this.hash = hash;
    }

    @Override
    public String toString(String field) {
        return "RandomSamplingQuery{" + "p=" + p + ", seed=" + seed + ", hash=" + hash + '}';
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new Weight(this) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                final Scorer s = scorer(context);
                final boolean exists = s.iterator().advance(doc) == doc;
                if (exists) {
                    return Explanation.match(boost, getQuery().toString());
                } else {
                    return Explanation.noMatch(getQuery().toString() + " doesn't match id " + doc);
                }
            }

            @Override
            public Scorer scorer(LeafReaderContext context) {
                final SplittableRandom random = new SplittableRandom(BitMixer.mix(hash, seed));
                int maxDoc = context.reader().maxDoc();
                return new ConstantScoreScorer(
                    this,
                    boost,
                    ScoreMode.COMPLETE_NO_SCORES,
                    new RandomSamplingIterator(maxDoc, p, random::nextInt)
                );
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
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
        return Double.compare(that.p, p) == 0 && seed == that.seed && hash == that.hash;
    }

    @Override
    public int hashCode() {
        return Objects.hash(p, seed, hash);
    }
}
