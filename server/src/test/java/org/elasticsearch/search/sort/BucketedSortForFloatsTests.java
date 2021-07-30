/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class BucketedSortForFloatsTests extends BucketedSortTestCase<BucketedSort.ForFloats> {
    @Override
    public BucketedSort.ForFloats build(SortOrder sortOrder, DocValueFormat format, int bucketSize,
            BucketedSort.ExtraData extra, double[] values) {
        return new BucketedSort.ForFloats(bigArrays(), sortOrder, format, bucketSize, extra) {
            @Override
            public boolean needsScores() {
                return false;
            }

            @Override
            public Leaf forLeaf(LeafReaderContext ctx) {
                return new Leaf(ctx) {
                    int index = -1;

                    @Override
                    protected boolean advanceExact(int doc) {
                        index = doc;
                        return doc < values.length;
                    }

                    @Override
                    protected float docValue() {
                        return (float) values[index];
                    }

                    @Override
                    public void setScorer(Scorable scorer) {}
                };
            }
        };
    }

    private BucketedSort.ForFloats buildForScores(SortOrder sortOrder, DocValueFormat format, int bucketSize) {
        return new BucketedSort.ForFloats(bigArrays(), sortOrder, format, bucketSize, BucketedSort.NOOP_EXTRA_DATA) {
            @Override
            public Leaf forLeaf(LeafReaderContext ctx) {
                return new Leaf(ctx) {
                    Scorable scorer;
                    float score;

                    @Override
                    public void setScorer(Scorable scorer) {
                        this.scorer = scorer;
                    }

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        score = scorer.score();
                        return true;
                    }

                    @Override
                    protected float docValue() {
                        return score;
                    }
                };
            }

            @Override
            public boolean needsScores() {
                return true;
            }
        };
    }

    @Override
    protected SortValue expectedSortValue(double v) {
        /*
         * The explicit cast to float is important because it reduces
         * the expected precision to the one we can provide. Sneaky. Computers
         * are sneaky.
         */
        return SortValue.from((float) v);
    }

    @Override
    protected double randomValue() {
        return randomFloat();
    }

    public void testScorer() throws IOException {
        try (BucketedSort.ForFloats sort = buildForScores(SortOrder.DESC, DocValueFormat.RAW, 2)) {
            assertTrue(sort.needsScores());
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            MockScorable scorer = new MockScorable();
            leaf.setScorer(scorer);
            scorer.score = 10;
            leaf.collect(0, 0);
            scorer.score = 1;
            leaf.collect(0, 0);
            scorer.score = 0;
            leaf.collect(3, 0);
            assertThat(sort.getValues(0), contains(SortValue.from(10.0), SortValue.from(1.0)));
        }
    }

    private class MockScorable extends Scorable {
        private int doc;
        private float score;

        @Override
        public float score() throws IOException {
            return score;
        }

        @Override
        public int docID() {
            return doc;
        }
    }

    /**
     * Check that we can store the largest bucket theoretically possible.
     */
    public void testBiggest() throws IOException {
        try (BucketedSort sort = new BucketedSort.ForFloats(bigArrays(), SortOrder.DESC, DocValueFormat.RAW,
                BucketedSort.ForFloats.MAX_BUCKET_SIZE, BucketedSort.NOOP_EXTRA_DATA) {
            @Override
            public boolean needsScores() { return false; }

            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new Leaf(ctx) {
                    int doc;
                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        this.doc = doc;
                        return true;
                    }

                    @Override
                    protected float docValue() {
                        return doc;
                    }

                    @Override
                    public void setScorer(Scorable scorer) {}
                };
            }
        }) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            int extra = between(0, 1000);
            int max = BucketedSort.ForFloats.MAX_BUCKET_SIZE + extra;
            for (int i = 0; i < max; i++) {
                leaf.advanceExact(i);
                leaf.collect(i, 0);
                leaf.collect(i, 1);
            }
            assertThat(sort.getValue(0), equalTo(SortValue.from((double) extra)));
            assertThat(sort.getValue(BucketedSort.ForFloats.MAX_BUCKET_SIZE), equalTo(SortValue.from((double) extra)));
        }
    }

    public void testTooBig() {
        int tooBig = BucketedSort.ForFloats.MAX_BUCKET_SIZE + 1;
        Exception e = expectThrows(IllegalArgumentException.class, () ->
                build(randomFrom(SortOrder.values()), DocValueFormat.RAW, tooBig, BucketedSort.NOOP_EXTRA_DATA, new double[] {}));
        assertThat(e.getMessage(), equalTo("bucket size must be less than [2^24] but was [" + tooBig + "]"));
    }
}
