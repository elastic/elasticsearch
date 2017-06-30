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

package org.elasticsearch.index.query;


import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Random;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.Randomness;

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
public final class RandomSampleQuery extends Query {

    private final double p;
    private final Long seed;

    // Above this threshold, it is probably faster to just use simple random sampling
    private static final double PROBABILITY_THRESHOLD = 0.7;
    private static final double EPSILON = 1e-10;
    private static double LOG_INVERSE_P;

    RandomSampleQuery(double p, Long seed) {
        assert(p > 0.0 && p <= 1.0);
        this.p = p;
        this.seed = seed;
        LOG_INVERSE_P =  Math.log(1-p);
    }

    static int getGap(Random rng) {
        double u = Math.max(rng.nextDouble(), EPSILON);
        return (int)(Math.floor(Math.log(u) / LOG_INVERSE_P)) + 1;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public String toString() {
                return "weight(" + RandomSampleQuery.this + ")";
            }
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final Random rng = seed == null ? Randomness.get() : new Random(seed);
                int maxDoc = context.reader().maxDoc();

                // For small doc sets, it's easier/more accurate to just sample directly
                // instead of sampling gaps. Or, if the probability is high, faster to use SRS
                if (maxDoc < 100 || p > PROBABILITY_THRESHOLD) {
                    return new ConstantScoreScorer(this, score(), new RandomSamplingDocIdSetIterator(maxDoc, rng, p));
                } else {
                    return new ConstantScoreScorer(this, score(), new RandomGapSamplingDocIdSetIterator(maxDoc, rng, p));
                }

            }
            @Override
            public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
                final float score = score();
                final int maxDoc = context.reader().maxDoc();
                final Random rng = seed == null ? Randomness.get() : new Random(seed);

                // For small doc sets, it's easier/more accurate to just sample directly
                // instead of sampling gaps. Or, if the probability is high, faster to use SRS
                if (maxDoc < 100 || p > PROBABILITY_THRESHOLD) {
                    return new BulkScorer() {
                        @Override
                        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                            max = Math.min(max, maxDoc);
                            FakeScorer scorer = new FakeScorer();
                            scorer.score = score;
                            collector.setScorer(scorer);

                            for (int current = min; current < max; current++) {
                                if (rng.nextDouble() <= p) {
                                    scorer.doc = current;
                                    if (acceptDocs == null || acceptDocs.get(current)) {
                                        collector.collect(current);
                                    }
                                }
                            }
                            return max == maxDoc ? DocIdSetIterator.NO_MORE_DOCS : max;
                        }
                        @Override
                        public long cost() {
                            return maxDoc;
                        }
                    };
                } else {
                    return new BulkScorer() {
                        @Override
                        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                            max = Math.min(max, maxDoc);
                            FakeScorer scorer = new FakeScorer();
                            scorer.score = score;
                            collector.setScorer(scorer);

                            int current = min;
                            while (current < max) {
                                int gap = getGap(rng);
                                current = current + gap;
                                if (current >= maxDoc) {
                                    return DocIdSetIterator.NO_MORE_DOCS;
                                }
                                scorer.doc = current;
                                if (acceptDocs == null || acceptDocs.get(current)) {
                                    collector.collect(current);
                                }
                            }
                            return max == maxDoc ? DocIdSetIterator.NO_MORE_DOCS : max;
                        }
                        @Override
                        public long cost() {
                            return maxDoc;
                        }
                    };
                }

            }
        };
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
        RandomSampleQuery other = (RandomSampleQuery)o;
        return Objects.equals(this.p, other.p) &&
            Objects.equals(this.seed, other.seed);
    }

    @Override
    public int hashCode() {
        return classHash();
    }

    /**
     * A DocIDSetIter that samples on each document.  Empirically, this tends to
     * be more accurate when the segment is small.  It may also be faster
     * when the probability is high, since many docs are collected and gap approximation
     * uses more expensive math
     */
    private static class RandomSamplingDocIdSetIterator extends DocIdSetIterator {
        int doc = -1;
        final int maxDoc;
        final Random rng;
        final double p;

        RandomSamplingDocIdSetIterator(int maxDoc, Random rng, double p) {
            this.maxDoc = maxDoc;
            this.rng = rng;
            this.p = p;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            doc = target;
            while (doc < maxDoc) {
                if (rng.nextDouble() <= p) {
                    return doc;
                }
                doc = doc + 1;
            }
            return NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }

    /**
     * A DocIDSetIter that approximates the gaps between sampled documents, and advances
     * according to the gap.  This is more efficient, especially for low probabilities,
     * because it can skip by many documents entirely.
     */
    private static class RandomGapSamplingDocIdSetIterator extends DocIdSetIterator {
        final int maxDoc;
        final Random rng;
        final double p;
        int doc = -1;

        RandomGapSamplingDocIdSetIterator(int maxDoc, Random rng, double p) {
            this.maxDoc = maxDoc;
            this.rng = rng;
            this.p = p;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc);
        }

        @Override
        public int advance(int target) throws IOException {
            // Keep approximating gaps until we hit or surpass the target
            while (doc <= target) {
                doc += getGap(rng);
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
    }

    /**
     *  This is a copy of Lucene's FakeScorer since it is package-private
     */
    final class FakeScorer extends Scorer {
        float score;
        int doc = -1;
        int freq = 1;

        FakeScorer() {
            super(null);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int freq() {
            return freq;
        }

        @Override
        public float score() {
            return score;
        }

        @Override
        public DocIdSetIterator iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Weight getWeight() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<ChildScorer> getChildren() {
            throw new UnsupportedOperationException();
        }
    }

}
