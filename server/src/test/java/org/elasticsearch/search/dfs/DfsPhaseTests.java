/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.dfs;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

public class DfsPhaseTests extends ESTestCase {

    ThreadPoolExecutor threadPoolExecutor;
    private TestThreadPool threadPool;

    @Before
    public final void init() {
        int numThreads = randomIntBetween(2, 4);
        threadPool = new TestThreadPool(DfsPhaseTests.class.getName());
        threadPoolExecutor = EsExecutors.newFixed(
            "test",
            numThreads,
            10,
            EsExecutors.daemonThreadFactory("test"),
            threadPool.getThreadContext(),
            randomFrom(TaskTrackingConfig.DEFAULT, TaskTrackingConfig.DO_NOT_TRACK)
        );
    }

    @After
    public void cleanup() {
        threadPoolExecutor.shutdown();
        terminate(threadPool);
    }

    public void testSingleKnnSearch() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {
            int numDocs = randomIntBetween(900, 1000);
            for (int i = 0; i < numDocs; i++) {
                Document d = new Document();
                d.add(new KnnFloatVectorField("float_vector", new float[] { i, 0, 0 }));
                w.addDocument(d);
            }
            w.flush();

            IndexReader reader = w.getReader();
            ContextIndexSearcher searcher = new ContextIndexSearcher(
                reader,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy(),
                randomBoolean(),
                this.threadPoolExecutor
            ) {
                @Override
                protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
                    // get a thread per segment
                    return slices(leaves, 1, 1);
                }
            };

            Query query = new KnnFloatVectorQuery("float_vector", new float[] { 0, 0, 0 }, numDocs, null);

            int k = 10;
            // run without profiling enabled
            DfsKnnResults dfsKnnResults = DfsPhase.singleKnnSearch(query, k, null, searcher);
            assertEquals(k, dfsKnnResults.scoreDocs().length);

            // run with profiling enabled
            Profilers profilers = new Profilers(searcher);
            dfsKnnResults = DfsPhase.singleKnnSearch(query, k, profilers, searcher);
            assertEquals(k, dfsKnnResults.scoreDocs().length);
            SearchProfileDfsPhaseResult searchProfileDfsPhaseResult = profilers.getDfsProfiler().buildDfsPhaseResults();
            List<QueryProfileShardResult> queryProfileShardResult = searchProfileDfsPhaseResult.getQueryProfileShardResult();
            assertNotNull(queryProfileShardResult);
            CollectorResult collectorResult = queryProfileShardResult.get(0).getCollectorResult();
            assertEquals("SimpleTopScoreDocCollector", (collectorResult.getName()));
            assertEquals("search_top_hits", (collectorResult.getReason()));
            assertTrue(collectorResult.getTime() > 0);
            List<CollectorResult> children = collectorResult.getCollectorResults();
            if (children.size() > 0) {
                long totalTime = 0L;
                for (CollectorResult child : children) {
                    assertEquals("SimpleTopScoreDocCollector", (child.getName()));
                    assertEquals("search_top_hits", (child.getReason()));
                    totalTime += child.getTime();
                }
                assertEquals(totalTime, collectorResult.getTime());
            }
            reader.close();
        }
    }
}
