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

package org.elasticsearch.test.engine;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Helper class that adds some extra checks to ensure correct
 * usage of {@code ContextIndexSearcher} and {@code Weight}.
 */
// Modified fork of Lucene's AssertingIndexSearcher that adds extra checks for ContextIndexSearcher
public class AssertingContextIndexSearcher extends ContextIndexSearcher {

    final Random random;

    public AssertingContextIndexSearcher(Random random, ContextIndexSearcher indexSearcher) {
        super(indexSearcher.getIndexReader(), indexSearcher.getSimilarity(true), indexSearcher.getQueryCache(), indexSearcher.getQueryCachingPolicy());
        this.random = new Random(random.nextLong());
    }

    /** Ensures, that the returned {@code Weight} is not normalized again, which may produce wrong scores. */
    @Override
    public Weight createNormalizedWeight(Query query, boolean needsScores) throws IOException {
        final Weight w = super.createNormalizedWeight(query, needsScores);
        return new AssertingWeight(random, w) {

            @Override
            public void normalize(float norm, float topLevelBoost) {
                throw new IllegalStateException("Weight already normalized.");
            }

            @Override
            public float getValueForNormalization() {
                throw new IllegalStateException("Weight already normalized.");
            }

        };
    }

    @Override
    public Weight createWeight(Query query, boolean needsScores) throws IOException {
        // this adds assertions to the inner weights/scorers too
        return new AssertingWeight(random, super.createWeight(query, needsScores));
    }

    @Override
    public Query rewrite(Query original) throws IOException {
        // TODO: use the more sophisticated QueryUtils.check sometimes!
        QueryUtils.check(original);
        Query rewritten = super.rewrite(original);
        QueryUtils.check(rewritten);
        return rewritten;
    }

    @Override
    public void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        // TODO: shouldn't we AssertingCollector.wrap(collector) here?
        super.search(leaves, AssertingWeight.wrap(random, weight), AssertingCollector.wrap(random, collector));
    }

    @Override
    public String toString() {
        return "AssertingIndexSearcher(" + super.toString() + ")";
    }

    // Also copied from Lucene:
    // TODO: make these classes in Lucene public
    static class AssertingWeight extends Weight {

        static Weight wrap(Random random, Weight other) {
            return other instanceof AssertingWeight ? other : new AssertingWeight(random, other);
        }

        final Random random;
        final Weight in;

        AssertingWeight(Random random, Weight in) {
            super(in.getQuery());
            this.random = random;
            this.in = in;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            in.extractTerms(terms);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return in.explain(context, doc);
        }

        @Override
        public float getValueForNormalization() throws IOException {
            return in.getValueForNormalization();
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            in.normalize(norm, topLevelBoost);
        }

        @Override
        public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            final Scorer inScorer = in.scorer(context, acceptDocs);
            assert inScorer == null || inScorer.docID() == -1;
            return AssertingScorer.wrap(new Random(random.nextLong()), inScorer);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            BulkScorer inScorer = in.bulkScorer(context, acceptDocs);
            if (inScorer == null) {
                return null;
            }

            return AssertingBulkScorer.wrap(new Random(random.nextLong()), inScorer, context.reader().maxDoc());
        }

    }

    static final class AssertingBulkScorer extends BulkScorer {

        public static BulkScorer wrap(Random random, BulkScorer other, int maxDoc) {
            if (other == null || other instanceof AssertingBulkScorer) {
                return other;
            }
            return new AssertingBulkScorer(random, other, maxDoc);
        }

        final Random random;
        final BulkScorer in;
        final int maxDoc;
        int max = 0;

        private AssertingBulkScorer(Random random, BulkScorer in, int maxDoc) {
            this.random = random;
            this.in = in;
            this.maxDoc = maxDoc;
        }

        public BulkScorer getIn() {
            return in;
        }

        @Override
        public long cost() {
            return in.cost();
        }

        @Override
        public void score(LeafCollector collector) throws IOException {
            assert max == 0;
            collector = new AssertingLeafCollector(random, collector, 0, PostingsEnum.NO_MORE_DOCS);
            if (random.nextBoolean()) {
                try {
                    final int next = score(collector, 0, PostingsEnum.NO_MORE_DOCS);
                    assert next == DocIdSetIterator.NO_MORE_DOCS;
                } catch (UnsupportedOperationException e) {
                    in.score(collector);
                }
            } else {
                in.score(collector);
            }
        }

        @Override
        public int score(LeafCollector collector, int min, final int max) throws IOException {
            assert min >= this.max: "Scoring backward: min=" + min + " while previous max was max=" + this.max;
            assert min <= max : "max must be greater than min, got min=" + min + ", and max=" + max;
            this.max = max;
            collector = new AssertingLeafCollector(random, collector, min, max);
            final int next = in.score(collector, min, max);
            assert next >= max;
            if (max >= maxDoc || next >= maxDoc) {
                assert next == DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            } else {
                return RandomInts.randomIntBetween(random, max, next);
            }
        }

        @Override
        public String toString() {
            return "AssertingBulkScorer(" + in + ")";
        }

    }

    static class AssertingCollector extends FilterCollector {

        private final Random random;
        private int maxDoc = -1;

        /** Wrap the given collector in order to add assertions. */
        public static Collector wrap(Random random, Collector in) {
            if (in instanceof AssertingCollector) {
                return in;
            }
            return new AssertingCollector(random, in);
        }

        private AssertingCollector(Random random, Collector in) {
            super(in);
            this.random = random;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            final LeafCollector in = super.getLeafCollector(context);
            final int docBase = context.docBase;
            return new AssertingLeafCollector(random, in, 0, DocIdSetIterator.NO_MORE_DOCS) {
                @Override
                public void collect(int doc) throws IOException {
                    // check that documents are scored in order globally,
                    // not only per segment
                    assert docBase + doc >= maxDoc : "collection is not in order: current doc="
                            + (docBase + doc) + " while " + maxDoc + " has already been collected";
                    super.collect(doc);
                    maxDoc = docBase + doc;
                }
            };
        }

    }

    static class AssertingLeafCollector extends FilterLeafCollector {

        private final Random random;
        private final int min;
        private final int max;

        private Scorer scorer;
        private int lastCollected = -1;

        AssertingLeafCollector(Random random, LeafCollector collector, int min, int max) {
            super(collector);
            this.random = random;
            this.min = min;
            this.max = max;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
            super.setScorer(AssertingScorer.wrap(random, scorer));
        }

        @Override
        public void collect(int doc) throws IOException {
            assert doc > lastCollected : "Out of order : " + lastCollected + " " + doc;
            assert doc >= min : "Out of range: " + doc + " < " + min;
            assert doc < max : "Out of range: " + doc + " >= " + max;
            assert scorer.docID() == doc : "Collected: " + doc + " but scorer: " + scorer.docID();
            in.collect(doc);
            lastCollected = doc;
        }

    }
}
