/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.DummyTotalHitCountCollector;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ProfileCollectorManagerTests extends ESTestCase {

    private static class TestCollector extends DummyTotalHitCountCollector {

        private final int id;

        TestCollector(int id) {
            this.id = id;
        }
    }

    /**
     * This test checks that each new collector is a different instance on each call and that
     * the call to reduce() is forwarded to the wrapped collector manager.
     */
    public void testBasic() throws IOException {
        final SetOnce<Boolean> reduceCalled = new SetOnce<>();
        ProfileCollectorManager<Integer> pcm = new ProfileCollectorManager<>(new CollectorManager<TestCollector, Integer>() {

            private int counter = 0;

            @Override
            public TestCollector newCollector() {
                return new TestCollector(counter++);
            }

            @Override
            public Integer reduce(Collection<TestCollector> collectors) {
                reduceCalled.set(true);
                return counter;
            }
        }, CollectorResult.REASON_SEARCH_TOP_HITS);
        int runs = randomIntBetween(5, 10);
        List<InternalProfileCollector> collectors = new ArrayList<>();
        for (int i = 0; i < runs; i++) {
            collectors.add(pcm.newCollector());
            assertEquals(i, ((TestCollector) collectors.get(i).getWrappedCollector()).id);
        }
        Integer returnValue = pcm.reduce(collectors);
        assertEquals(runs, returnValue.intValue());
        assertTrue(reduceCalled.get());
    }

    public void testReduceEmpty() {
        ProfileCollectorManager<TopDocs> pcm = new ProfileCollectorManager<>(
            TopScoreDocCollector.createSharedManager(10, null, 1000),
            CollectorResult.REASON_SEARCH_TOP_HITS
        );
        AssertionError ae = expectThrows(AssertionError.class, () -> pcm.reduce(Collections.emptyList()));
        assertEquals("at least one collector expected", ae.getMessage());
    }

    /**
     * This test checks functionality with potentially more than one slice on a real searcher,
     * wrapping a {@link TopScoreDocCollector} into  {@link ProfileCollectorManager} and checking the
     * result from calling the collector tree contains profile results for each slice.
     */
    public void testManagerWithSearcher() throws IOException {
        Directory directory = newDirectory();
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig())) {
            int numDocs = randomIntBetween(900, 1000);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("field1", "value", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.flush();
            IndexReader reader = writer.getReader();
            IndexSearcher searcher = newSearcher(reader);
            searcher.setSimilarity(new BM25Similarity());

            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(10, null, 1000);
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);

            String profileReason = "profiler_reason";
            ProfileCollectorManager<TopDocs> profileCollectorManager = new ProfileCollectorManager<>(topDocsManager, profileReason);

            searcher.search(new MatchAllDocsQuery(), profileCollectorManager);

            CollectorResult result = profileCollectorManager.getCollectorTree();
            assertEquals("profiler_reason", result.getReason());
            assertEquals("SimpleTopScoreDocCollector", result.getName());
            assertTrue(result.getTime() > 0);

            reader.close();
        }
        directory.close();
    }
}
