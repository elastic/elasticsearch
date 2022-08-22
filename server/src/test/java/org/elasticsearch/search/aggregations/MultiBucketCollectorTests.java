/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class MultiBucketCollectorTests extends ESTestCase {
    private static class ScoreAndDoc extends Scorable {
        float score;
        int doc = -1;

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public float score() {
            return score;
        }
    }

    private static class TerminateAfterBucketCollector extends BucketCollector {

        private int count = 0;
        private final int terminateAfter;
        private final BucketCollector in;

        TerminateAfterBucketCollector(BucketCollector in, int terminateAfter) {
            this.in = in;
            this.terminateAfter = terminateAfter;
        }

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
            if (count >= terminateAfter) {
                return LeafBucketCollector.NO_OP_COLLECTOR;
            }
            final LeafBucketCollector leafCollector = in.getLeafCollector(aggCtx);
            return new LeafBucketCollectorBase(leafCollector, null) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (count >= terminateAfter) {
                        throw new CollectionTerminatedException();
                    }
                    super.collect(doc, bucket);
                    count++;
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }

        @Override
        public void preCollection() {}

        @Override
        public void postCollection() {}
    }

    private static class TotalHitCountBucketCollector extends BucketCollector {

        private int count = 0;

        TotalHitCountBucketCollector() {}

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) {
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    count++;
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        public void preCollection() {}

        @Override
        public void postCollection() {}

        int getTotalHits() {
            return count;
        }
    }

    private static class SetScorerBucketCollector extends BucketCollector {
        private final BucketCollector in;
        private final AtomicBoolean setScorerCalled;

        SetScorerBucketCollector(BucketCollector in, AtomicBoolean setScorerCalled) {
            this.in = in;
            this.setScorerCalled = setScorerCalled;
        }

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
            final LeafBucketCollector leafCollector = in.getLeafCollector(aggCtx);
            return new LeafBucketCollectorBase(leafCollector, null) {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    super.setScorer(scorer);
                    setScorerCalled.set(true);
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }

        @Override
        public void preCollection() {}

        @Override
        public void postCollection() {}
    }

    public void testCollectionTerminatedExceptionHandling() throws IOException {
        final int iters = atLeast(3);
        for (int iter = 0; iter < iters; ++iter) {
            Directory dir = newDirectory();
            RandomIndexWriter w = new RandomIndexWriter(random(), dir);
            final int numDocs = randomIntBetween(100, 1000);
            final Document doc = new Document();
            for (int i = 0; i < numDocs; ++i) {
                w.addDocument(doc);
            }
            final IndexReader reader = w.getReader();
            w.close();
            final IndexSearcher searcher = newSearcher(reader);
            Map<TotalHitCountBucketCollector, Integer> expectedCounts = new HashMap<>();
            List<BucketCollector> collectors = new ArrayList<>();
            final int numCollectors = randomIntBetween(1, 5);
            for (int i = 0; i < numCollectors; ++i) {
                final int terminateAfter = random().nextInt(numDocs + 10);
                final int expectedCount = terminateAfter > numDocs ? numDocs : terminateAfter;
                TotalHitCountBucketCollector collector = new TotalHitCountBucketCollector();
                expectedCounts.put(collector, expectedCount);
                collectors.add(new TerminateAfterBucketCollector(collector, terminateAfter));
            }
            searcher.search(new MatchAllDocsQuery(), MultiBucketCollector.wrap(true, collectors).asCollector());
            for (Map.Entry<TotalHitCountBucketCollector, Integer> expectedCount : expectedCounts.entrySet()) {
                assertEquals(expectedCount.getValue().intValue(), expectedCount.getKey().getTotalHits());
            }
            reader.close();
            dir.close();
        }
    }

    public void testNotTerminated() throws IOException {
        final int iters = atLeast(3);
        for (int iter = 0; iter < iters; ++iter) {
            try (Directory dir = newDirectory()) {
                RandomIndexWriter w = new RandomIndexWriter(random(), dir);
                final int numDocs = randomIntBetween(100, 1000);
                final Document doc = new Document();
                for (int i = 0; i < numDocs; ++i) {
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    w.close();
                    Map<TotalHitCountBucketCollector, Integer> expectedCounts = new HashMap<>();
                    List<BucketCollector> collectors = new ArrayList<>();
                    final int numCollectors = randomIntBetween(1, 5);
                    for (int i = 0; i < numCollectors; ++i) {
                        final int terminateAfter = random().nextInt(numDocs + 10);
                        final int expectedCount = terminateAfter > numDocs ? numDocs : terminateAfter;
                        TotalHitCountBucketCollector collector = new TotalHitCountBucketCollector();
                        expectedCounts.put(collector, expectedCount);
                        collectors.add(new TerminateAfterBucketCollector(collector, terminateAfter));
                    }
                    BucketCollector wrapped = MultiBucketCollector.wrap(false, collectors);
                    for (LeafReaderContext ctx : reader.leaves()) {
                        boolean shouldNoop = true;
                        for (Map.Entry<TotalHitCountBucketCollector, Integer> expectedCount : expectedCounts.entrySet()) {
                            shouldNoop &= expectedCount.getValue().intValue() <= expectedCount.getKey().getTotalHits();
                        }
                        LeafBucketCollector collector = wrapped.getLeafCollector(new AggregationExecutionContext(ctx, null, null));
                        assertThat(collector.isNoop(), equalTo(shouldNoop));
                        if (false == collector.isNoop()) {
                            for (int docId = 0; docId < ctx.reader().numDocs(); docId++) {
                                try {
                                    collector.collect(docId);
                                } catch (CollectionTerminatedException e) {
                                    break;
                                }
                            }
                        }
                    }
                    for (Map.Entry<TotalHitCountBucketCollector, Integer> expectedCount : expectedCounts.entrySet()) {
                        assertEquals(expectedCount.getValue().intValue(), expectedCount.getKey().getTotalHits());
                    }
                }
            }
        }
    }

    public void testSetScorerAfterCollectionTerminated() throws IOException {
        BucketCollector collector1 = new TotalHitCountBucketCollector();
        BucketCollector collector2 = new TotalHitCountBucketCollector();

        AtomicBoolean setScorerCalled1 = new AtomicBoolean();
        collector1 = new SetScorerBucketCollector(collector1, setScorerCalled1);

        AtomicBoolean setScorerCalled2 = new AtomicBoolean();
        collector2 = new SetScorerBucketCollector(collector2, setScorerCalled2);

        collector1 = new TerminateAfterBucketCollector(collector1, 1);
        collector2 = new TerminateAfterBucketCollector(collector2, 2);

        Scorable scorer = new ScoreAndDoc();

        List<BucketCollector> collectors = Arrays.asList(collector1, collector2);
        Collections.shuffle(collectors, random());
        BucketCollector collector = MultiBucketCollector.wrap(true, collectors);

        LeafBucketCollector leafCollector = collector.getLeafCollector(null);
        leafCollector.setScorer(scorer);
        assertTrue(setScorerCalled1.get());
        assertTrue(setScorerCalled2.get());

        leafCollector.collect(0);
        leafCollector.collect(1);

        setScorerCalled1.set(false);
        setScorerCalled2.set(false);
        leafCollector.setScorer(scorer);
        assertFalse(setScorerCalled1.get());
        assertTrue(setScorerCalled2.get());

        expectThrows(CollectionTerminatedException.class, () -> { leafCollector.collect(1); });

        setScorerCalled1.set(false);
        setScorerCalled2.set(false);
        leafCollector.setScorer(scorer);
        assertFalse(setScorerCalled1.get());
        assertFalse(setScorerCalled2.get());
    }
}
