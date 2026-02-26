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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SearchExecutionContextHelper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.TestSearchContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.lucene.search.Queries.ALL_DOCS_INSTANCE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DfsPhaseTimeoutTests extends IndexShardTestCase {

    private static final int KNN_K = 10;

    private static Directory dir;
    private static IndexReader reader;
    private static int numDocs;
    private IndexShard indexShard;

    @BeforeClass
    public static void init() throws Exception {
        dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        numDocs = scaledRandomIntBetween(500, 4500);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            doc.add(new StringField("field", Integer.toString(i), Field.Store.NO));
            doc.add(new KnnByteVectorField("byte_vector", new byte[] { (byte) (i % 128), (byte) ((i + 1) % 128), (byte) ((i + 2) % 128) }));
            doc.add(new KnnFloatVectorField("float_vector", new float[] { i * 0.1f, (i + 1) * 0.1f, (i + 2) * 0.1f }));
            w.addDocument(doc);
        }
        w.close();
        reader = DirectoryReader.open(dir);
    }

    @AfterClass
    public static void destroy() throws Exception {
        if (reader != null) {
            reader.close();
        }
        dir.close();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexShard = newShard(true);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        closeShards(indexShard);
    }

    public void testExecuteWithKnnTimeoutExceededAllowPartial() throws Exception {
        DfsSearchResult dfsResult = new DfsSearchResult(null, null, null);
        ContextIndexSearcher cis = newThrowingOnFirstSearchSearcher(reader);

        try (TestSearchContext context = new TestSearchContext(createSearchExecutionContext(), indexShard, cis) {
            @Override
            public TimeValue timeout() {
                return TimeValue.ZERO;
            }

            @Override
            public DfsSearchResult dfsResult() {
                return dfsResult;
            }
        }) {
            context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
            context.parsedQuery(new ParsedQuery(ALL_DOCS_INSTANCE));
            context.request()
                .source(
                    new SearchSourceBuilder().knnSearch(
                        List.of(new KnnSearchBuilder("float_vector", new float[] { 0.1f, 0.2f, 0.3f }, KNN_K, numDocs, 100f, null, null))
                    )
                );

            DfsPhase.execute(context);

            assertNotNull(dfsResult.knnResults());
            assertTrue(dfsResult.knnResults().isEmpty());
            assertTrue(dfsResult.searchTimedOut());
        }
    }

    public void testExecuteWithKnnTimeoutExceededDisallowPartial() throws Exception {
        DfsSearchResult dfsResult = new DfsSearchResult(null, null, null);
        ContextIndexSearcher cis = newThrowingOnFirstSearchSearcher(reader);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(
            new SearchSourceBuilder().knnSearch(
                List.of(new KnnSearchBuilder("float_vector", new float[] { 0.1f, 0.2f, 0.3f }, KNN_K, numDocs, 100f, null, null))
            )
        );
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            System.currentTimeMillis(),
            null
        );

        try (TestSearchContext context = new TestSearchContext(createSearchExecutionContext(), indexShard, cis) {
            @Override
            public ShardSearchRequest request() {
                return shardRequest;
            }

            @Override
            public TimeValue timeout() {
                return TimeValue.ZERO;
            }

            @Override
            public DfsSearchResult dfsResult() {
                return dfsResult;
            }
        }) {
            context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
            context.parsedQuery(new ParsedQuery(ALL_DOCS_INSTANCE));

            SearchTimeoutException ex = expectThrows(SearchTimeoutException.class, () -> DfsPhase.execute(context));
            assertThat(ex.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertTrue(ex.isTimeout());
        }
    }

    public void testExecuteWrapsExceptionInDfsPhaseExecutionException() throws Exception {
        ContextIndexSearcher cis = newContextSearcher(reader);
        DfsSearchResult dfsResult = new DfsSearchResult(null, null, null);

        try (TestSearchContext context = new TestSearchContext(createSearchExecutionContext(), indexShard, cis) {
            @Override
            public DfsSearchResult dfsResult() {
                return dfsResult;
            }
        }) {
            context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
            context.request().source(new SearchSourceBuilder());

            SearchException e = expectThrows(DfsPhaseExecutionException.class, () -> DfsPhase.execute(context));
            assertThat(e.getCause(), instanceOf(IllegalStateException.class));
        }
    }

    public void testExecuteWithProfilersSetsProfileResult() throws Exception {
        ContextIndexSearcher cis = newContextSearcher(reader);
        DfsSearchResult dfsResult = new DfsSearchResult(null, null, null);
        Profilers profilers = new Profilers(cis);

        try (TestSearchContext context = new TestSearchContext(createSearchExecutionContext(), indexShard, cis) {
            @Override
            public DfsSearchResult dfsResult() {
                return dfsResult;
            }

            @Override
            public Profilers getProfilers() {
                return profilers;
            }
        }) {
            context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
            context.parsedQuery(new ParsedQuery(ALL_DOCS_INSTANCE));
            context.request()
                .source(
                    new SearchSourceBuilder().knnSearch(
                        List.of(new KnnSearchBuilder("float_vector", new float[] { 0.1f, 0.2f, 0.3f }, KNN_K, numDocs, 100f, null, null))
                    )
                );

            DfsPhase.execute(context);

            SearchProfileDfsPhaseResult profileResult = dfsResult.searchProfileDfsPhaseResult();
            assertNotNull(profileResult);
            assertNotNull(profileResult.getQueryProfileShardResult());
        }
    }

    private SearchExecutionContext createSearchExecutionContext() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .creationDate(System.currentTimeMillis())
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        final long nowInMillis = randomNonNegativeLong();
        return new SearchExecutionContext(
            0,
            0,
            indexSettings,
            new BitsetFilterCache(indexSettings, BitsetFilterCache.Listener.NOOP),
            (ft, fdc) -> ft.fielddataBuilder(fdc).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService()),
            null,
            MappingLookup.EMPTY,
            null,
            null,
            parserConfig(),
            writableRegistry(),
            null,
            new IndexSearcher(reader),
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null,
            Collections.emptyMap(),
            null,
            MapperMetrics.NOOP,
            SearchExecutionContextHelper.SHARD_SEARCH_STATS
        );
    }

    private static ContextIndexSearcher newContextSearcher(IndexReader reader) throws IOException {
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            LuceneTestCase.MAYBE_CACHE_POLICY,
            true
        );
    }

    private static ContextIndexSearcher newThrowingOnFirstSearchSearcher(IndexReader reader) throws IOException {
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            LuceneTestCase.MAYBE_CACHE_POLICY,
            true
        ) {
            private final AtomicBoolean searchCalled = new AtomicBoolean(false);

            @Override
            public <C extends org.apache.lucene.search.Collector, T> T search(Query query, CollectorManager<C, T> collectorManager)
                throws IOException {
                if (searchCalled.compareAndSet(false, true)) {
                    throwTimeExceededException();
                }
                return super.search(query, collectorManager);
            }
        };
    }
}
