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
import org.apache.lucene.sandbox.search.ProfilerCollectorResult;
import org.apache.lucene.search.Collector;
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

    public void testNewCollector() throws IOException {
        ProfileCollectorManager pcm = ProfileCollectorManager.create(new CollectorManager<>() {

            private static int counter = 0;

            @Override
            public Collector newCollector() {
                return new TestCollector(counter++);
            }

            @Override
            public Void reduce(Collection<Collector> collectors) {
                return null;
            }
        });
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            InternalProfileCollector internalProfileCollector = pcm.newCollector();
            assertEquals(i, ((TestCollector) internalProfileCollector.getWrappedCollector()).id);
        }
    }

    public void testReduce() throws IOException {
        final SetOnce<Boolean> reduceCalled = new SetOnce<>();
        ProfileCollectorManager pcm = ProfileCollectorManager.create(new CollectorManager<>() {

            @Override
            public Collector newCollector() {
                return new TestCollector(0);
            }

            @Override
            public Void reduce(Collection<Collector> collectors) {
                reduceCalled.set(true);
                return null;
            }
        });
        pcm.reduce(Collections.emptyList());
        assertTrue(reduceCalled.get());
    }

    public void testGetCollectorTree() throws IOException {
        ProfileCollectorManager pcm = ProfileCollectorManager.create(new CollectorManager<>() {

            @Override
            public Collector newCollector() {
                return new TestCollector(0);
            }

            @Override
            public Void reduce(Collection<Collector> collectors) {
                return null;
            }
        });
        expectThrows(IllegalStateException.class, pcm::getCollectorTree);
        List<InternalProfileCollector> collectors = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            collectors.add(pcm.newCollector());
        }
        pcm.reduce(collectors);
        CollectorResult collectorTree = pcm.getCollectorTree();
        assertEquals("ProfileCollectorManager", collectorTree.getName());
        assertEquals("search_top_hits_max", collectorTree.getReason());
        assertEquals(0, collectorTree.getTime());
        List<CollectorResult> nestedResults = collectorTree.getCollectorResults();
        assertNotNull(nestedResults);
        assertEquals(5, nestedResults.size());
        for (CollectorResult cr : nestedResults) {
            assertEquals("TestCollector", cr.getName());
            assertEquals("search_top_hits", cr.getReason());
            assertEquals(0L, cr.getTime());
        }
    }

    public void testManager() throws IOException {
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
            int numSlices = searcher.getSlices() == null ? 1 : searcher.getSlices().length;
            searcher.setSimilarity(new BM25Similarity());

            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(10, null, 1000);
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
            ProfileCollectorManager profileCollectorManager = ProfileCollectorManager.create(topDocsManager);

            searcher.search(new MatchAllDocsQuery(), profileCollectorManager);
            CollectorResult parent = profileCollectorManager.getCollectorTree();
            assertEquals("ProfileCollectorManager", parent.getName());
            assertEquals("search_top_hits_max", parent.getReason());
            assertTrue(parent.getTime() > 0);
            List<ProfilerCollectorResult> delegateCollectorResults = parent.getProfiledChildren();
            assertEquals(numSlices, delegateCollectorResults.size());
            for (ProfilerCollectorResult pcr : delegateCollectorResults) {
                assertEquals("SimpleTopScoreDocCollector", pcr.getName());
                assertEquals("search_top_hits", pcr.getReason());
                assertTrue(pcr.getTime() > 0);
            }
            reader.close();
        }
        directory.close();
    }
}
