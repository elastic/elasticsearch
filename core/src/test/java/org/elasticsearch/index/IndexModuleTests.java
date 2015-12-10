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
package org.elasticsearch.index;

import org.apache.lucene.index.AssertingDirectoryReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.cache.query.index.IndexQueryCache;
import org.elasticsearch.index.cache.query.none.NoneQueryCache;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCacheListener;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.engine.MockEngineFactory;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class IndexModuleTests extends ESTestCase {
    private Index index;
    private Settings settings;
    private IndexSettings indexSettings;
    private Environment environment;
    private NodeEnvironment nodeEnvironment;
    private NodeServicesProvider nodeServicesProvider;
    private IndexService.ShardStoreDeleter deleter = new IndexService.ShardStoreDeleter() {
        @Override
        public void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException {
        }
        @Override
        public void addPendingDelete(ShardId shardId, IndexSettings indexSettings) {
        }
    };
    private MapperRegistry mapperRegistry;

    static NodeServicesProvider newNodeServiceProvider(Settings settings, Environment environment, Client client, ScriptEngineService... scriptEngineServices) throws IOException {
        // TODO this can be used in other place too - lets first refactor the IndicesQueriesRegistry
        ThreadPool threadPool = new ThreadPool("test");
        IndicesWarmer warmer = new IndicesWarmer(settings, threadPool);
        IndicesQueryCache indicesQueryCache = new IndicesQueryCache(settings);
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        PageCacheRecycler recycler = new PageCacheRecycler(settings, threadPool);
        BigArrays bigArrays = new BigArrays(recycler, circuitBreakerService);
        IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(settings, new IndicesFieldDataCacheListener(circuitBreakerService), threadPool);
        Set<ScriptEngineService> scriptEngines = new HashSet<>();
        scriptEngines.addAll(Arrays.asList(scriptEngineServices));
        ScriptService scriptService = new ScriptService(settings, environment, scriptEngines, new ResourceWatcherService(settings, threadPool), new ScriptContextRegistry(Collections.emptyList()));
        IndicesQueriesRegistry indicesQueriesRegistry = new IndicesQueriesRegistry(settings, Collections.emptySet(), new NamedWriteableRegistry());
        return new NodeServicesProvider(threadPool, indicesQueryCache, null, warmer, bigArrays, client, scriptService, indicesQueriesRegistry, indicesFieldDataCache, circuitBreakerService);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        index = new Index("foo");
        settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put("path.home", createTempDir().toString()).build();
        indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        environment = new Environment(settings);
        nodeServicesProvider = newNodeServiceProvider(settings, environment, null);
        nodeEnvironment = new NodeEnvironment(settings, environment);
        mapperRegistry = new IndicesModule().getMapperRegistry();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        nodeEnvironment.close();
        nodeServicesProvider.getThreadPool().shutdown();
        if (nodeServicesProvider.getThreadPool().awaitTermination(10, TimeUnit.SECONDS) == false) {
            nodeServicesProvider.getThreadPool().shutdownNow();
        }
    }

    public void testWrapperIsBound() throws IOException {
        IndexModule module = new IndexModule(indexSettings, null, new AnalysisRegistry(null, environment));
        module.setSearcherWrapper((s) -> new Wrapper());
        module.engineFactory.set(new MockEngineFactory(AssertingDirectoryReader.class));
        IndexService indexService = module.newIndexService(nodeEnvironment, deleter, nodeServicesProvider, mapperRegistry);
        assertTrue(indexService.getSearcherWrapper() instanceof Wrapper);
        assertSame(indexService.getEngineFactory(), module.engineFactory.get());
        indexService.close("simon says", false);
    }


    public void testRegisterIndexStore() throws IOException {
        final Index index = new Index("foo");
        final Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put("path.home", createTempDir().toString()).put(IndexModule.STORE_TYPE, "foo_store").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        IndexModule module = new IndexModule(indexSettings, null, new AnalysisRegistry(null, environment));
        module.addIndexStore("foo_store", FooStore::new);
        IndexService indexService = module.newIndexService(nodeEnvironment, deleter, nodeServicesProvider, mapperRegistry);
        assertTrue(indexService.getIndexStore() instanceof FooStore);
        try {
            module.addIndexStore("foo_store", FooStore::new);
            fail("already registered");
        } catch (IllegalArgumentException ex) {
            // fine
        }
        indexService.close("simon says", false);
    }

    public void testOtherServiceBound() throws IOException {
        final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        final IndexEventListener eventListener = new IndexEventListener() {
            @Override
            public void beforeIndexDeleted(IndexService indexService) {
                atomicBoolean.set(true);
            }
        };
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        IndexModule module = new IndexModule(indexSettings, null, new AnalysisRegistry(null, environment));
        Consumer<Settings> listener = (s) -> {};
        module.addIndexSettingsListener(listener);
        module.addIndexEventListener(eventListener);
        IndexService indexService = module.newIndexService(nodeEnvironment, deleter, nodeServicesProvider, mapperRegistry);
        IndexSettings x = indexService.getIndexSettings();
        assertEquals(x.getSettings().getAsMap(), indexSettings.getSettings().getAsMap());
        assertEquals(x.getIndex(), index);
        assertSame(x.getUpdateListeners().get(0), listener);
        indexService.getIndexEventListener().beforeIndexDeleted(null);
        assertTrue(atomicBoolean.get());
        indexService.close("simon says", false);
    }


    public void testListener() throws IOException {
        IndexModule module = new IndexModule(indexSettings, null, new AnalysisRegistry(null, environment));
        Consumer<Settings> listener = (s) -> {
        };
        module.addIndexSettingsListener(listener);

        try {
            module.addIndexSettingsListener(listener);
            fail("already added");
        } catch (IllegalStateException ex) {

        }

        try {
            module.addIndexSettingsListener(null);
            fail("must not be null");
        } catch (IllegalArgumentException ex) {

        }
        IndexService indexService = module.newIndexService(nodeEnvironment, deleter, nodeServicesProvider, mapperRegistry);
        IndexSettings x = indexService.getIndexSettings();
        assertEquals(1, x.getUpdateListeners().size());
        assertSame(x.getUpdateListeners().get(0), listener);
        indexService.close("simon says", false);
    }

    public void testAddSimilarity() throws IOException {
        Settings indexSettings = Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.similarity.my_similarity.type", "test_similarity")
                .put("index.similarity.my_similarity.key", "there is a key")
                .put("path.home", createTempDir().toString())
                .build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings), null, new AnalysisRegistry(null, environment));
        module.addSimilarity("test_similarity", (string, settings) -> new SimilarityProvider() {
            @Override
            public String name() {
                return string;
            }

            @Override
            public Similarity get() {
                return new TestSimilarity(settings.get("key"));
            }
        });

        IndexService indexService = module.newIndexService(nodeEnvironment, deleter, nodeServicesProvider, mapperRegistry);
        SimilarityService similarityService = indexService.similarityService();
        assertNotNull(similarityService.getSimilarity("my_similarity"));
        assertTrue(similarityService.getSimilarity("my_similarity").get() instanceof TestSimilarity);
        assertEquals("my_similarity", similarityService.getSimilarity("my_similarity").name());
        assertEquals("there is a key", ((TestSimilarity) similarityService.getSimilarity("my_similarity").get()).key);
        indexService.close("simon says", false);
    }

    public void testSetupUnknownSimilarity() throws IOException {
        Settings indexSettings = Settings.settingsBuilder()
                .put("index.similarity.my_similarity.type", "test_similarity")
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("path.home", createTempDir().toString())
                .build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings), null, new AnalysisRegistry(null, environment));
        try {
            module.newIndexService(nodeEnvironment, deleter, nodeServicesProvider, mapperRegistry);
        } catch (IllegalArgumentException ex) {
            assertEquals("Unknown Similarity type [test_similarity] for [my_similarity]", ex.getMessage());
        }
    }

    public void testSetupWithoutType() throws IOException {
        Settings indexSettings = Settings.settingsBuilder()
                .put("index.similarity.my_similarity.foo", "bar")
                .put("path.home", createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings), null, new AnalysisRegistry(null, environment));
        try {
            module.newIndexService(nodeEnvironment, deleter, nodeServicesProvider, mapperRegistry);
        } catch (IllegalArgumentException ex) {
            assertEquals("Similarity [my_similarity] must have an associated type", ex.getMessage());
        }
    }

    public void testCannotRegisterProvidedImplementations() {
        Settings indexSettings = Settings.settingsBuilder()
                .put("path.home", createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings), null, new AnalysisRegistry(null, environment));
        try {
            module.registerQueryCache("index", IndexQueryCache::new);
            fail("only once");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_cache] more than once for [index]");
        }

        try {
            module.registerQueryCache("none", (settings, x) -> new NoneQueryCache(settings));
            fail("only once");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_cache] more than once for [none]");
        }

        try {
            module.registerQueryCache("index", null);
            fail("must not be null");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "provider must not be null");
        }
    }

    public void testRegisterCustomQueryCache() throws IOException {
        Settings indexSettings = Settings.settingsBuilder()
                .put(IndexModule.QUERY_CACHE_TYPE, "custom")
                .put("path.home", createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings), null, new AnalysisRegistry(null, environment));
        module.registerQueryCache("custom", (a, b) -> new CustomQueryCache());
        try {
            module.registerQueryCache("custom", (a, b) -> new CustomQueryCache());
            fail("only once");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_cache] more than once for [custom]");
        }

        IndexService indexService = module.newIndexService(nodeEnvironment, deleter, nodeServicesProvider, mapperRegistry);
        assertTrue(indexService.cache().query() instanceof CustomQueryCache);
        indexService.close("simon says", false);
    }

    public void testDefaultQueryCacheImplIsSelected() throws IOException {
        Settings indexSettings = Settings.settingsBuilder()
                .put("path.home", createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings), null, new AnalysisRegistry(null, environment));
        IndexService indexService = module.newIndexService(nodeEnvironment, deleter, nodeServicesProvider, mapperRegistry);
        assertTrue(indexService.cache().query() instanceof IndexQueryCache);
        indexService.close("simon says", false);
    }

    class CustomQueryCache implements QueryCache {

        @Override
        public void clear(String reason) {
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public Index index() {
            return new Index("test");
        }

        @Override
        public Weight doCache(Weight weight, QueryCachingPolicy policy) {
            return weight;
        }
    }

    private static class TestSimilarity extends Similarity {
        private final Similarity delegate = new BM25Similarity();
        private final String key;


        public TestSimilarity(String key) {
            if (key == null) {
                throw new AssertionError("key is null");
            }
            this.key = key;
        }

        @Override
        public long computeNorm(FieldInvertState state) {
            return delegate.computeNorm(state);
        }

        @Override
        public SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
            return delegate.computeWeight(collectionStats, termStats);
        }

        @Override
        public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
            return delegate.simScorer(weight, context);
        }
    }



    public static final class FooStore extends IndexStore {

        public FooStore(IndexSettings indexSettings, IndexStoreConfig config) {
            super(indexSettings, config);
        }
    }

    public static final class Wrapper extends IndexSearcherWrapper {

        @Override
        public DirectoryReader wrap(DirectoryReader reader) {
            return null;
        }

        @Override
        public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
            return null;
        }
    }

}
