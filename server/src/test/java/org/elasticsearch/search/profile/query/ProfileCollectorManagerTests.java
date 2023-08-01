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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.search.ProfilerCollectorResult;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.DummyTotalHitCountCollector;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ProfileCollectorManagerTests extends ESTestCase {

    private Directory directory;
    private int numDocs;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    private static class TestCollector extends DummyTotalHitCountCollector {

        private final int id;

        TestCollector(int id) {
            this.id = id;
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig())) {
            numDocs = randomIntBetween(900, 1000);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("field1", "value", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.flush();
        }
        reader = DirectoryReader.open(directory);
        searcher = newSearcher(reader);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        reader.close();
        directory.close();
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
        }, "test_reason");
        int runs = randomIntBetween(5, 10);
        List<InternalProfileCollector> collectors = new ArrayList<>();
        for (int i = 0; i < runs; i++) {
            collectors.add(pcm.newCollector());
            assertEquals(i, ((TestCollector) collectors.get(i).getWrappedCollector()).id);
        }

        long totalTime = 0;
        LeafReaderContext leafReaderContext = reader.leaves().get(0);
        for (InternalProfileCollector collector : collectors) {
            LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);
            leafCollector.collect(0);
            totalTime += collector.getTime();
        }
        Integer returnValue = pcm.reduce(collectors);
        assertEquals(runs, returnValue.intValue());
        assertTrue(reduceCalled.get());
        assertEquals(totalTime, pcm.getCollectorTree().getTime());
        assertEquals("test_reason", pcm.getCollectorTree().getReason());
        assertEquals("TestCollector", pcm.getCollectorTree().getName());
        assertEquals(0, pcm.getCollectorTree().getProfiledChildren().size());
    }

    /**
     * This test checks functionality with potentially more than one slice on a real searcher,
     * wrapping a {@link TopScoreDocCollector} into  {@link ProfileCollectorManager} and checking the
     * result from calling the collector tree contains profile results for each slice.
     */
    public void testManagerWithSearcher() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(10, null, 1000);
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(10, null, 1000);
            String profileReason = "profiler_reason";
            ProfileCollectorManager<TopDocs> profileCollectorManager = new ProfileCollectorManager<>(topDocsManager, profileReason);
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), profileCollectorManager);
            assertEquals(numDocs, topDocs.totalHits.value);
            CollectorResult result = profileCollectorManager.getCollectorTree();
            assertEquals("profiler_reason", result.getReason());
            assertEquals("SimpleTopScoreDocCollector", result.getName());
            assertTrue(result.getTime() > 0);
        }
    }

    public void testManagerWithChildren() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(10, null, 1000);
            TotalHitCountCollectorManager totalHitCountCollectorManager = new TotalHitCountCollectorManager();
            MultiCollectorManager multiCollectorManager = new MultiCollectorManager(topDocsManager, totalHitCountCollectorManager);
            Object[] results = searcher.search(new MatchAllDocsQuery(), multiCollectorManager);
            assertEquals(numDocs, ((TopDocs) results[0]).totalHits.value);
            assertEquals(numDocs, (int) results[1]);
        }
        {
            ProfileCollectorManager<TopDocs> topDocsManager = new ProfileCollectorManager<>(
                TopScoreDocCollector.createSharedManager(10, null, 1000),
                "top_docs"
            );
            ProfileCollectorManager<Integer> aggsManager = new ProfileCollectorManager<>(
                new TotalHitCountCollectorManager(),
                "total_hit_count"
            );
            MultiCollectorManager multiCollectorManager = new MultiCollectorManager(topDocsManager, aggsManager);
            ProfileCollectorManager<Object[]> profileCollectorManager = new ProfileCollectorManager<>(
                multiCollectorManager,
                "multi_collector",
                topDocsManager,
                aggsManager
            );
            Object[] results = searcher.search(new MatchAllDocsQuery(), profileCollectorManager);
            assertEquals(numDocs, ((TopDocs) results[0]).totalHits.value);
            assertEquals(numDocs, (int) results[1]);
            CollectorResult result = profileCollectorManager.getCollectorTree();
            assertEquals("multi_collector", result.getReason());
            assertEquals("MultiCollector", result.getName());
            assertTrue(result.getTime() > 0);
            assertEquals(2, result.getProfiledChildren().size());
            ProfilerCollectorResult topDocsCollectorResult = result.getProfiledChildren().get(0);
            assertEquals("top_docs", topDocsCollectorResult.getReason());
            assertEquals("SimpleTopScoreDocCollector", topDocsCollectorResult.getName());
            assertTrue(topDocsCollectorResult.getTime() > 0);
            ProfilerCollectorResult aggsCollectorResult = result.getProfiledChildren().get(1);
            assertEquals("total_hit_count", aggsCollectorResult.getReason());
            assertEquals("TotalHitCountCollector", aggsCollectorResult.getName());
            assertTrue(aggsCollectorResult.getTime() > 0);
        }
    }

    public void testManagerWithSingleChild() throws IOException {
        {
            CollectorManager<DummyTotalHitCountCollector, Integer> manager = DummyTotalHitCountCollector.createManager();
            CollectorManagerWrapper collectorManagerWrapper = new CollectorManagerWrapper(manager);
            Integer hitCount = searcher.search(new MatchAllDocsQuery(), collectorManagerWrapper);
            assertEquals(numDocs, hitCount.longValue());
        }
        {
            CollectorManager<DummyTotalHitCountCollector, Integer> manager = DummyTotalHitCountCollector.createManager();
            ProfileCollectorManager<Integer> profileTopDocsManager = new ProfileCollectorManager<>(manager, "dummy_total_hit_count");
            CollectorManagerWrapper collectorManagerWrapper = new CollectorManagerWrapper(profileTopDocsManager);
            ProfileCollectorManager<Integer> profileCollectorManager = new ProfileCollectorManager<>(
                collectorManagerWrapper,
                "collector_wrapper",
                profileTopDocsManager,
                null
            );
            Integer hitCount = searcher.search(new MatchAllDocsQuery(), profileCollectorManager);
            assertEquals(numDocs, hitCount.longValue());
            CollectorResult result = profileCollectorManager.getCollectorTree();
            assertEquals("collector_wrapper", result.getReason());
            assertEquals("CollectorWrapper", result.getName());
            assertTrue(result.getTime() > 0);
            assertEquals(1, result.getProfiledChildren().size());
            ProfilerCollectorResult topDocsCollectorResult = result.getProfiledChildren().get(0);
            assertEquals("dummy_total_hit_count", topDocsCollectorResult.getReason());
            assertEquals("DummyTotalHitCountCollector", topDocsCollectorResult.getName());
            assertTrue(topDocsCollectorResult.getTime() > 0);
        }
    }

    private static class CollectorManagerWrapper implements CollectorManager<CollectorWrapper, Integer> {

        private final CollectorManager<?, Integer> collectorManager;

        CollectorManagerWrapper(CollectorManager<?, Integer> collectorManager) {
            this.collectorManager = collectorManager;
        }

        @Override
        public CollectorWrapper newCollector() throws IOException {
            return new CollectorWrapper(collectorManager.newCollector());
        }

        @Override
        public Integer reduce(Collection<CollectorWrapper> collectors) throws IOException {
            List<Collector> collectorList = collectors.stream().map(collectorWrapper -> collectorWrapper.collector).toList();
            @SuppressWarnings("unchecked")
            CollectorManager<Collector, Integer> manager = (CollectorManager<Collector, Integer>) collectorManager;
            return manager.reduce(collectorList);
        }
    }

    private static class CollectorWrapper extends FilterCollector {
        private final Collector collector;

        CollectorWrapper(Collector collector) {
            super(collector);
            this.collector = collector;
        }
    }
}
