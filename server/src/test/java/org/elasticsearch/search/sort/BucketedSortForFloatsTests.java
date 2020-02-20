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

package org.elasticsearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

public class BucketedSortForFloatsTests extends BucketedSortTestCase<BucketedSort.ForFloats> {
    @Override
    public BucketedSort.ForFloats build(SortOrder sortOrder, DocValueFormat format, double[] values) {
        return new BucketedSort.ForFloats(bigArrays(), sortOrder, format) {
            @Override
            public boolean needsScores() {
                return false;
            }

            @Override
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new Leaf() {
                    int index = -1;

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        index = doc;
                        return doc < values.length;
                    }

                    @Override
                    protected float docValue() throws IOException {
                        return (float) values[index];
                    }

                    @Override
                    public void setScorer(Scorable scorer) {}
                };
            }
        };
    }

    private BucketedSort.ForFloats buildForScores(SortOrder sortOrder, DocValueFormat format) {
        return new BucketedSort.ForFloats(bigArrays(), sortOrder, format) {
            @Override
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new Leaf() {
                    Scorable scorer;

                    @Override
                    public void setScorer(Scorable scorer) {
                        this.scorer = scorer;
                    }

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        return scorer.docID() == doc;
                    }

                    @Override
                    protected float docValue() throws IOException {
                        return scorer.score();
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
        return SortValue.from(v);
    }

    public void testScorer() throws IOException {
        try (BucketedSort.ForFloats sort = buildForScores(SortOrder.DESC, DocValueFormat.RAW)) {
            assertTrue(sort.needsScores());
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            MockScorable scorer = new MockScorable();
            leaf.setScorer(scorer);
            scorer.doc = 1;
            scorer.score = 10;
            assertFalse(leaf.collectIfCompetitive(0, 0));
            assertTrue(leaf.collectIfCompetitive(1, 0));
            assertEquals(sort.getValue(0), SortValue.from(10.0));
            scorer.doc = 2;
            scorer.score = 1;
            assertFalse(leaf.collectIfCompetitive(2, 0));
            assertEquals(sort.getValue(0), SortValue.from(10.0));
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

}
