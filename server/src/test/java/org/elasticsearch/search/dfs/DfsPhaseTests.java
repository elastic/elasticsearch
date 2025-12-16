/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.dfs;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.search.dfs.DfsPhase.executeKnnVectorQuery;

public class DfsPhaseTests extends IndexShardTestCase {

    ThreadPoolExecutor threadPoolExecutor;
    private TestThreadPool threadPool;

    @Before
    public final void init() {
        threadPool = new TestThreadPool(DfsPhaseTests.class.getName());
        threadPoolExecutor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
    }

    @After
    public void cleanup() {
        threadPoolExecutor.shutdown();
        terminate(threadPool);
    }

    public void testKnnSearch() throws IOException {
        AtomicLong queryCount = new AtomicLong();
        AtomicLong queryTime = new AtomicLong();

        IndexShard indexShard = newShard(true, List.of(new SearchOperationListener() {
            @Override
            public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
                queryCount.incrementAndGet();
                queryTime.addAndGet(tookInNanos);
            }
        }));
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
                threadPoolExecutor,
                threadPoolExecutor.getMaximumPoolSize(),
                1
            );
            IndexSettings indexSettings = new IndexSettings(
                IndexMetadata.builder("index")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .creationDate(System.currentTimeMillis())
                    .build(),
                Settings.EMPTY
            );
            BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetFilterCache.Listener() {
                @Override
                public void onCache(ShardId shardId, Accountable accountable) {

                }

                @Override
                public void onRemoval(ShardId shardId, Accountable accountable) {

                }
            });
            SearchExecutionContext searchExecutionContext = new SearchExecutionContext(
                0,
                0,
                indexSettings,
                bitsetFilterCache,
                null,
                null,
                MappingLookup.EMPTY,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                Collections.emptyMap(),
                null,
                MapperMetrics.NOOP
            );

            Query query = new KnnFloatVectorQuery("float_vector", new float[] { 0, 0, 0 }, numDocs, null);
            try (TestSearchContext context = new TestSearchContext(searchExecutionContext, indexShard, searcher) {
                @Override
                public DfsSearchResult dfsResult() {
                    return new DfsSearchResult(null, null, null);
                }
            }) {
                context.request()
                    .source(
                        new SearchSourceBuilder().knnSearch(
                            List.of(new KnnSearchBuilder("float_vector", new float[] { 0, 0, 0 }, numDocs, numDocs, 100f, null, null, null))
                        )
                    );
                context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
                context.parsedQuery(new ParsedQuery(query));
                executeKnnVectorQuery(context);
                assertTrue(queryCount.get() > 0);
                assertTrue(queryTime.get() > 0);
                reader.close();
                closeShards(indexShard);
            }
        }
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
                threadPoolExecutor,
                threadPoolExecutor.getMaximumPoolSize(),
                1
            );

            Query query = new KnnFloatVectorQuery("float_vector", new float[] { 0, 0, 0 }, numDocs, null);

            int k = 10;
            // run without profiling enabled
            DfsKnnResults dfsKnnResults = DfsPhase.singleKnnSearch(query, k, null, searcher, null);
            assertEquals(k, dfsKnnResults.scoreDocs().length);

            // run with profiling enabled
            Profilers profilers = new Profilers(searcher);
            dfsKnnResults = DfsPhase.singleKnnSearch(query, k, profilers, searcher, null);
            assertEquals(k, dfsKnnResults.scoreDocs().length);
            SearchProfileDfsPhaseResult searchProfileDfsPhaseResult = profilers.getDfsProfiler().buildDfsPhaseResults();
            List<QueryProfileShardResult> queryProfileShardResult = searchProfileDfsPhaseResult.getQueryProfileShardResult();
            assertNotNull(queryProfileShardResult);
            CollectorResult collectorResult = queryProfileShardResult.get(0).getCollectorResult();
            assertEquals("TopScoreDocCollector", (collectorResult.getName()));
            assertEquals("search_top_hits", (collectorResult.getReason()));
            assertTrue(collectorResult.getTime() > 0);
            List<CollectorResult> children = collectorResult.getChildrenResults();
            if (children.size() > 0) {
                long totalTime = 0L;
                for (CollectorResult child : children) {
                    assertEquals("TopScoreDocCollector", (child.getName()));
                    assertEquals("search_top_hits", (child.getReason()));
                    totalTime += child.getTime();
                }
                assertEquals(totalTime, collectorResult.getTime());
            }
            reader.close();
        }
    }
}
