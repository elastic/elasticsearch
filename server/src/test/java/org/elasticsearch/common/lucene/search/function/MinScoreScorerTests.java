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

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MinScoreScorerTests extends LuceneTestCase {

    private static DocIdSetIterator iterator(final int... docs) {
        return new DocIdSetIterator() {

            int i = -1;

            @Override
            public int nextDoc() throws IOException {
                if (i + 1 == docs.length) {
                    return NO_MORE_DOCS;
                } else {
                    return docs[++i];
                }
            }

            @Override
            public int docID() {
                return i < 0 ? -1 : i == docs.length ? NO_MORE_DOCS : docs[i];
            }

            @Override
            public long cost() {
                return docs.length;
            }

            @Override
            public int advance(int target) throws IOException {
                return slowAdvance(target);
            }
        };
    }

    private static Weight fakeWeight() {
        return new Weight(new MatchAllDocsQuery()) {
            @Override
            public void extractTerms(Set<Term> terms) {

            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return null;
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                return null;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    private static Scorer scorer(int maxDoc, final int[] docs, final float[] scores, final boolean twoPhase) {
        final DocIdSetIterator iterator = twoPhase ? DocIdSetIterator.all(maxDoc) : iterator(docs);
        return new Scorer(fakeWeight()) {

            int lastScoredDoc = -1;

            public DocIdSetIterator iterator() {
                if (twoPhase) {
                    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
                } else {
                    return iterator;
                }
            }

            public TwoPhaseIterator twoPhaseIterator() {
                if (twoPhase) {
                    return new TwoPhaseIterator(iterator) {

                        @Override
                        public boolean matches() throws IOException {
                            return Arrays.binarySearch(docs, iterator.docID()) >= 0;
                        }

                        @Override
                        public float matchCost() {
                            return 10;
                        }
                    };
                } else {
                    return null;
                }
            }

            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public float score() throws IOException {
                assertNotEquals("score() called twice on doc " + docID(), lastScoredDoc, docID());
                lastScoredDoc = docID();
                final int idx = Arrays.binarySearch(docs, docID());
                return scores[idx];
            }

            @Override
            public float getMaxScore(int upTo) throws IOException {
                return Float.MAX_VALUE;
            }
        };
    }

    public void doTestRandom(boolean twoPhase) throws IOException {
        final int maxDoc = TestUtil.nextInt(random(), 10, 10000);
        final int numDocs = TestUtil.nextInt(random(), 1, maxDoc / 2);
        final Set<Integer> uniqueDocs = new HashSet<>();
        while (uniqueDocs.size() < numDocs) {
            uniqueDocs.add(random().nextInt(maxDoc));
        }
        final int[] docs = new int[numDocs];
        int i = 0;
        for (int doc : uniqueDocs) {
            docs[i++] = doc;
        }
        Arrays.sort(docs);
        final float[] scores = new float[numDocs];
        for (i = 0; i < numDocs; ++i) {
            scores[i] = random().nextFloat();
        }
        Scorer scorer = scorer(maxDoc, docs, scores, twoPhase);
        final float minScore = random().nextFloat();
        Scorer minScoreScorer = new MinScoreScorer(fakeWeight(), scorer, minScore);
        int doc = -1;
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            final int target;
            if (random().nextBoolean()) {
                target = doc + 1;
                doc = minScoreScorer.iterator().nextDoc();
            } else {
                target = doc + TestUtil.nextInt(random(), 1, 10);
                doc = minScoreScorer.iterator().advance(target);
            }
            int idx = Arrays.binarySearch(docs, target);
            if (idx < 0) {
                idx = -1 - idx;
            }
            while (idx < docs.length && scores[idx] < minScore) {
                idx += 1;
            }
            if (idx == docs.length) {
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, doc);
            } else {
                assertEquals(docs[idx], doc);
                assertEquals(scores[idx], minScoreScorer.score(), 0f);
            }
        }
    }

    public void testRegularIterator() throws IOException {
        final int iters = atLeast(5);
        for (int iter = 0; iter < iters; ++iter) {
            doTestRandom(false);
        }
    }

    public void testTwoPhaseIterator() throws IOException {
        final int iters = atLeast(5);
        for (int iter = 0; iter < iters; ++iter) {
            doTestRandom(true);
        }
    }
}
