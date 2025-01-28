/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.search.query.PartialHitCountCollector.HitsThresholdChecker;

public class PartialHitCountCollectorTests extends ESTestCase {

    private Directory dir;
    private IndexSearcher searcher;
    private int numDocs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        numDocs = scaledRandomIntBetween(900, 1000);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("string", "a" + i, Field.Store.NO));
            if (i < 3) {
                doc.add(new StringField("string", "foo", Field.Store.NO));
            }
            writer.addDocument(doc);
        }
        if (randomBoolean()) {
            writer.deleteDocuments(new Term("string", "a10"));
            numDocs--;
        }
        IndexReader reader = writer.getReader();
        writer.close();
        searcher = newSearcher(reader);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        searcher.getIndexReader().close();
        dir.close();
    }

    public void testEarlyTerminatesWithoutCollection() throws IOException {
        Query query = new NonCountingTermQuery(new Term("string", "a1"));
        PartialHitCountCollector hitCountCollector = new PartialHitCountCollector(new HitsThresholdChecker(0)) {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                return new FilterLeafCollector(super.getLeafCollector(context)) {
                    @Override
                    public void collect(int doc) {
                        throw new AssertionError("unexpected collection");
                    }
                };
            }
        };
        searcher.search(query, hitCountCollector);
        assertEquals(0, hitCountCollector.getTotalHits());
        assertTrue(hitCountCollector.hasEarlyTerminated());
    }

    public void testHitCountFromWeightNoTracking() throws IOException {
        CollectorManager<PartialHitCountCollector, Result> collectorManager = createCollectorManager(new HitsThresholdChecker(0));
        Result result = searcher.search(new MatchAllDocsQuery(), collectorManager);
        assertEquals(0, result.totalHits);
        assertTrue(result.terminatedAfter);
    }

    public void testHitCountFromWeightDoesNotEarlyTerminate() throws IOException {
        {
            CollectorManager<PartialHitCountCollector, Result> collectorManager = createCollectorManager(new HitsThresholdChecker(numDocs));
            Result result = searcher.search(new MatchAllDocsQuery(), collectorManager);
            assertEquals(numDocs, result.totalHits);
            assertFalse(result.terminatedAfter);
        }
        {
            int threshold = randomIntBetween(1, numDocs - 1);
            CollectorManager<PartialHitCountCollector, Result> collectorManager = createCollectorManager(
                new HitsThresholdChecker(threshold)
            );
            Result result = searcher.search(new MatchAllDocsQuery(), collectorManager);
            assertEquals(numDocs, result.totalHits);
            assertFalse(result.terminatedAfter);
        }
        {
            int threshold = randomIntBetween(numDocs + 1, 10000);
            CollectorManager<PartialHitCountCollector, Result> collectorManager = createCollectorManager(
                new HitsThresholdChecker(threshold)
            );
            Result result = searcher.search(new MatchAllDocsQuery(), collectorManager);
            assertEquals(numDocs, result.totalHits);
            assertFalse(result.terminatedAfter);
        }
    }

    public void testCollectedHitCount() throws Exception {
        Query query = new NonCountingTermQuery(new Term("string", "a1"));
        int threshold = randomIntBetween(2, 10000);
        // there's one doc matching the query: any totalHitsThreshold greater than 1 will not cause early termination
        CollectorManager<PartialHitCountCollector, Result> collectorManager = createCollectorManager(new HitsThresholdChecker(threshold));
        Result result = searcher.search(query, collectorManager);
        assertEquals(1, result.totalHits);
        assertFalse(result.terminatedAfter);
    }

    public void testThresholdOne() throws Exception {
        Query query = new NonCountingTermQuery(new Term("string", "a1"));
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 0f);
        CollectorManager<PartialHitCountCollector, Result> collectorManager = createCollectorManager(new HitsThresholdChecker(1));
        // threshold 1 behaves differently depending on whether there is a single segment (no early termination) or multiple segments.
        // With inter-segment concurrency the behaviour is not deterministic and depends on the timing of the different threads.
        // Without inter-segment concurrency the behaviour depends on which segment holds the matching document.
        // This is because the check for early termination is performed every time a leaf collector is pulled for a segment, as well
        // as for every collected doc.
        PartialHitCountCollector partialHitCountCollector = collectorManager.newCollector();
        int i = 0;
        while (partialHitCountCollector.getTotalHits() == 0 && i < searcher.getLeafContexts().size()) {
            LeafReaderContext ctx = searcher.getLeafContexts().get(i++);
            LeafCollector leafCollector = partialHitCountCollector.getLeafCollector(ctx);
            BulkScorer bulkScorer = weight.bulkScorer(ctx);
            bulkScorer.score(leafCollector, ctx.reader().getLiveDocs(), 0, DocIdSetIterator.NO_MORE_DOCS);
        }
        assertEquals(1, partialHitCountCollector.getTotalHits());
        assertFalse(partialHitCountCollector.hasEarlyTerminated());
        expectThrows(
            CollectionTerminatedException.class,
            () -> partialHitCountCollector.getLeafCollector(randomFrom(searcher.getLeafContexts()))
        );
        assertTrue(partialHitCountCollector.hasEarlyTerminated());
    }

    public void testCollectedHitCountEarlyTerminated() throws Exception {
        Query query = new NonCountingTermQuery(new Term("string", "foo"));
        // there's three docs matching the query: any totalHitsThreshold lower than 3 will trigger early termination
        int totalHitsThreshold = randomInt(2);
        CollectorManager<PartialHitCountCollector, Result> collectorManager = createCollectorManager(
            new HitsThresholdChecker(totalHitsThreshold)
        );
        Result result = searcher.search(query, collectorManager);
        assertEquals(totalHitsThreshold, result.totalHits);
        assertTrue(result.terminatedAfter);
    }

    public void testCollectedAccurateHitCount() throws Exception {
        Query query = new NonCountingTermQuery(new Term("string", "a1"));
        // make sure there is no overhead caused by early termination functionality when performing accurate total hit counting
        CollectorManager<PartialHitCountCollector, Result> collectorManager = createCollectorManager(NO_OVERHEAD_HITS_CHECKER);
        Result result = searcher.search(query, collectorManager);
        assertEquals(1, result.totalHits);
        assertFalse(result.terminatedAfter);
    }

    public void testScoreModeEarlyTermination() {
        PartialHitCountCollector hitCountCollector = new PartialHitCountCollector(
            new HitsThresholdChecker(randomIntBetween(0, Integer.MAX_VALUE - 1))
        );
        assertEquals(ScoreMode.TOP_DOCS, hitCountCollector.scoreMode());
    }

    public void testScoreModeAccurateHitCount() {
        PartialHitCountCollector hitCountCollector = new PartialHitCountCollector(new HitsThresholdChecker(Integer.MAX_VALUE));
        assertEquals(ScoreMode.COMPLETE_NO_SCORES, hitCountCollector.scoreMode());
    }

    private static final HitsThresholdChecker NO_OVERHEAD_HITS_CHECKER = new HitsThresholdChecker(Integer.MAX_VALUE) {
        @Override
        void incrementHitCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        boolean isThresholdReached() {
            throw new UnsupportedOperationException();
        }
    };

    /**
     * Returns a {@link CollectorManager} that creates {@link PartialHitCountCollector}s and reduces their results. Its purpose is to
     * test {@link PartialHitCountCollector} in concurrent scenarios, despite this is not the collector manager that we use for it in
     * production code. Using {@link QueryPhaseCollectorManager} in this test would excessively broaden the scope of this test.
     */
    private static CollectorManager<PartialHitCountCollector, Result> createCollectorManager(HitsThresholdChecker hitsThresholdChecker) {
        return new CollectorManager<>() {
            @Override
            public PartialHitCountCollector newCollector() {
                return new PartialHitCountCollector(hitsThresholdChecker);
            }

            @Override
            public Result reduce(Collection<PartialHitCountCollector> collectors) {
                Integer totalHits = collectors.stream().map(PartialHitCountCollector::getTotalHits).reduce(0, Integer::sum);
                boolean terminatedAfter = collectors.stream().anyMatch(PartialHitCountCollector::hasEarlyTerminated);
                return new Result(totalHits, terminatedAfter);
            }
        };
    }

    private record Result(int totalHits, boolean terminatedAfter) {}
}
