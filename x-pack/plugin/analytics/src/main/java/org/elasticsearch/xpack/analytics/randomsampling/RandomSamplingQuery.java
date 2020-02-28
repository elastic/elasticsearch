/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.randomsampling;

import com.carrotsearch.hppc.BitMixer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that randomly matches documents with a user-provided probability.  May
 * optionally include a seed so that matches are reproducible.
 *
 * This query uses two strategies depending on the size of the segment.
 * For small segments, this uses simple random sampling per-document.  For larger
 * segments, sampling is sped up by approximating the _gap_ between samples, then
 * skipping forward that amount.
 *
 * Gap-sampling is based on the work of Jeffrey Vitter in "An Efficient Algorithm
 * for Sequential Random Sampling" (http://www.ittc.ku.edu/~jsv/Papers/Vit87.RandomSampling.pdf),
 * and more recently documented by Erik Erlandson
 * (http://erikerlandson.github.io/blog/2014/09/11/faster-random-samples-with-gap-sampling/)
 */
public final class RandomSamplingQuery extends Query {

    // Above this threshold, it is probably faster to just use simple random sampling
    private static final double PROBABILITY_THRESHOLD = 0.5;
    private static final float EPSILON = 1e-10f;

    private final double p;
    private final int seed;
    private final boolean cacheable;

    /**
     * @param p         The sampling probability e.g. 0.05 == 5% probability a document will match
     * @param seed      A seed to make the sample deterministic
     * @param cacheable True if the seed is static (provided by the user) so that we can cache the query, false otherwise
     */
    RandomSamplingQuery(double p, int seed, boolean cacheable) {
        if (p < 0.0 || p > 1.0) {
            throw new IllegalArgumentException("RandomSampling probability must be between 0.0 and 1.0");
        }

        this.p = p;
        this.seed = BitMixer.mix(seed);
        this.cacheable = cacheable;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) {
                int maxDoc = context.reader().maxDoc();

                // For small doc sets, it's easier/more accurate to just sample directly
                // instead of sampling gaps. Or, if the probability is high, faster to use SRS
                if (maxDoc < 100 || p > PROBABILITY_THRESHOLD) {
                    return new ConstantScoreScorer(this, score(), ScoreMode.COMPLETE, new RandomSamplingIterator(maxDoc, p, seed));
                } else {
                    return new ConstantScoreScorer(this, score(), ScoreMode.COMPLETE, new RandomGapIterator(maxDoc, p, seed));
                }
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return cacheable;
            }
        };
    }

    /**
     * A DocIDSetIter that samples on each document.  Empirically, this tends to
     * be more accurate when the segment is small.  It may also be faster
     * when the probability is high, since many docs are collected and gap approximation
     * uses more expensive math
     */
    static class RandomSamplingIterator extends DocIdSetIterator {
        private final int maxDoc;
        private final double p;
        private final int seed;
        private int doc = -1;

        RandomSamplingIterator(int maxDoc, double p, int seed) {
            this.maxDoc = maxDoc;
            this.p = p;
            this.seed = seed;
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
            // Although this advances `doc` to `target`, the random value that decides which docs are returned
            // is only dependent on maxDoc of segment, the docID and the seed.  So the same documents will be matched
            // for a particular segment regardless of the order the iterator is consumed (although obviously not all
            // will be returned depending on how `advance()` is invoked
            doc = target;
            while (doc < maxDoc) {
                if (getRandomFloat(maxDoc, doc, seed) <= p) {
                    return doc;
                }
                doc = doc + 1;
            }
            return NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return (long)(maxDoc * p);
        }
    }

    /**
     * A DocIDSetIter that approximates the gaps between sampled documents, and advances
     * according to the gap.  This is more efficient for low probabilities
     * because it can skip forward without having to consult the RNG on each document.
     */
    static class RandomGapIterator extends DocIdSetIterator {
        private final int maxDoc;
        private final double p;
        private final int seed;
        private final double logInverseP;
        private int doc = -1;

        RandomGapIterator(int maxDoc, double p, int seed) {
            this.maxDoc = maxDoc;
            this.p = p;
            this.seed = seed;
            this.logInverseP = (float)Math.log(1-p);
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
            // Keep approximating gaps until we hit or surpass the target.  Although this can introduce
            // more work if another iterator has moved us forward a lot, it is required so that the results
            // returned by this iterator are consistent (e.g. always matches the same docs).
            //
            // If we set doc = target, the gaps generated would change depending on how `advance()` is invoked
            while (doc <= target) {
                doc += getGap(doc);
            }
            if (doc >= maxDoc) {
                doc = NO_MORE_DOCS;
            }
            return doc;
        }

        @Override
        public long cost() {
            return (long)(maxDoc * p);
        }

        private int getGap(int doc) {
            float u = Math.max(getRandomFloat(maxDoc, doc, seed), EPSILON);
            return (int)(Math.log(u) / logInverseP) + 1;
        }
    }

    private static float getRandomFloat(int maxDoc, int value, int seed) {
        int hash = BitMixer.mix(value, seed + maxDoc);
        return (hash & 0x00FFFFFF) / (float)(1 << 24); // only use the lower 24 bits to construct a float from 0.0-1.0
    }

    @Override
    public String toString(String field) {
        return "RandomSample[p=" + this.p + "](*:*)";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RandomSamplingQuery other = (RandomSamplingQuery)o;
        return Objects.equals(this.p, other.p) &&
            Objects.equals(this.seed, other.seed);
    }

    @Override
    public int hashCode() {
        return classHash();
    }


}
