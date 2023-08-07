/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

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
        PartialHitCountCollector hitCountCollector = new PartialHitCountCollector(new PartialHitCountCollector.HitsThresholdChecker(0)) {
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
        PartialHitCountCollector.CollectorManager collectorManager = new PartialHitCountCollector.CollectorManager(0);
        searcher.search(new MatchAllDocsQuery(), collectorManager);
        assertEquals(0, collectorManager.getTotalHits());
        assertTrue(collectorManager.hasEarlyTerminated());
    }

    public void testHitCountFromWeightDoesNotEarlyTerminate() throws IOException {
        {
            PartialHitCountCollector.CollectorManager collectorManager = new PartialHitCountCollector.CollectorManager(numDocs);
            searcher.search(new MatchAllDocsQuery(), collectorManager);
            assertEquals(numDocs, collectorManager.getTotalHits());
            assertFalse(collectorManager.hasEarlyTerminated());
        }
        {
            int threshold = randomIntBetween(1, numDocs - 1);
            PartialHitCountCollector.CollectorManager collectorManager = new PartialHitCountCollector.CollectorManager(threshold);
            searcher.search(new MatchAllDocsQuery(), collectorManager);
            assertEquals(numDocs, collectorManager.getTotalHits());
            assertFalse(collectorManager.hasEarlyTerminated());
        }
        {
            int threshold = randomIntBetween(numDocs + 1, 10000);
            PartialHitCountCollector.CollectorManager collectorManager = new PartialHitCountCollector.CollectorManager(threshold);
            searcher.search(new MatchAllDocsQuery(), collectorManager);
            assertEquals(numDocs, collectorManager.getTotalHits());
            assertFalse(collectorManager.hasEarlyTerminated());
        }
    }

    public void testCollectedHitCount() throws Exception {
        Query query = new NonCountingTermQuery(new Term("string", "a1"));
        int threshold = randomIntBetween(1, 10000);
        // there's one doc matching the query: any totalHitsThreshold greater than or equal to 1 will not cause early termination
        PartialHitCountCollector.CollectorManager collectorManager = new PartialHitCountCollector.CollectorManager(threshold);
        searcher.search(query, collectorManager);
        assertEquals(1, collectorManager.getTotalHits());
        assertFalse(collectorManager.hasEarlyTerminated());
    }

    public void testCollectedHitCountEarlyTerminated() throws Exception {
        Query query = new NonCountingTermQuery(new Term("string", "foo"));
        // there's three docs matching the query: any totalHitsThreshold lower than 3 will trigger early termination
        int totalHitsThreshold = randomInt(2);
        PartialHitCountCollector.CollectorManager collectorManager = new PartialHitCountCollector.CollectorManager(totalHitsThreshold);
        searcher.search(query, collectorManager);
        assertEquals(totalHitsThreshold, collectorManager.getTotalHits());
        assertTrue(collectorManager.hasEarlyTerminated());
    }

    public void testCollectedAccurateHitCount() throws Exception {
        Query query = new NonCountingTermQuery(new Term("string", "a1"));
        // make sure there is no overhead caused by early termination functionality when performing accurate total hit counting
        PartialHitCountCollector.CollectorManager collectorManager = new PartialHitCountCollector.CollectorManager(
            NO_OVERHEAD_HITS_CHECKER
        );
        searcher.search(query, collectorManager);
        assertEquals(1, collectorManager.getTotalHits());
        assertFalse(collectorManager.hasEarlyTerminated());
    }

    public void testScoreModeEarlyTermination() {
        PartialHitCountCollector hitCountCollector = new PartialHitCountCollector(
            new PartialHitCountCollector.HitsThresholdChecker(randomIntBetween(0, Integer.MAX_VALUE - 1))
        );
        assertEquals(ScoreMode.TOP_DOCS, hitCountCollector.scoreMode());
    }

    public void testScoreModeAccurateHitCount() {
        PartialHitCountCollector hitCountCollector = new PartialHitCountCollector(
            new PartialHitCountCollector.HitsThresholdChecker(Integer.MAX_VALUE)
        );
        assertEquals(ScoreMode.COMPLETE_NO_SCORES, hitCountCollector.scoreMode());
    }

    private static final PartialHitCountCollector.HitsThresholdChecker NO_OVERHEAD_HITS_CHECKER =
        new PartialHitCountCollector.HitsThresholdChecker(Integer.MAX_VALUE) {
            @Override
            void incrementHitCount() {
                throw new UnsupportedOperationException();
            }

            @Override
            boolean isThresholdReached() {
                throw new UnsupportedOperationException();
            }
        };
}
