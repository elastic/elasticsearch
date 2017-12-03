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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SetOnce.AlreadySetException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.cache.query.IndexQueryCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngineTests;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.test.engine.MockEngineFactory;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;

public class IndexModuleTests extends ESTestCase {
    private Index index;
    private Settings settings;
    private IndexSettings indexSettings;
    private Environment environment;
    private AnalysisRegistry emptyAnalysisRegistry;
    private NodeEnvironment nodeEnvironment;
    private IndicesQueryCache indicesQueryCache;

    private IndexService.ShardStoreDeleter deleter = new IndexService.ShardStoreDeleter() {
        @Override
        public void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException {
        }
        @Override
        public void addPendingDelete(ShardId shardId, IndexSettings indexSettings) {
        }
    };

    private final IndexFieldDataCache.Listener listener = new IndexFieldDataCache.Listener() {};
    private MapperRegistry mapperRegistry;
    private ThreadPool threadPool;
    private CircuitBreakerService circuitBreakerService;
    private BigArrays bigArrays;
    private ScriptService scriptService;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        indicesQueryCache = new IndicesQueryCache(settings);
        indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        index = indexSettings.getIndex();
        environment = TestEnvironment.newEnvironment(settings);
        emptyAnalysisRegistry = new AnalysisRegistry(environment, emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap(),
                emptyMap(), emptyMap(), emptyMap());
        threadPool = new TestThreadPool("test");
        circuitBreakerService = new NoneCircuitBreakerService();
        bigArrays = new BigArrays(settings, circuitBreakerService);
        scriptService = new ScriptService(settings, Collections.emptyMap(), Collections.emptyMap());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        nodeEnvironment = new NodeEnvironment(settings, environment);
        mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(nodeEnvironment, indicesQueryCache, clusterService);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private IndexService newIndexService(IndexModule module) throws IOException {
        return module.newIndexService(nodeEnvironment, xContentRegistry(), deleter, circuitBreakerService, bigArrays, threadPool,
                scriptService, null, indicesQueryCache, mapperRegistry,
                new IndicesFieldDataCache(settings, listener), writableRegistry());
    }

    public void testWrapperIsBound() throws IOException {
        IndexModule module = new IndexModule(indexSettings, emptyAnalysisRegistry);
        module.setSearcherWrapper((s) -> new Wrapper());
        module.engineFactory.set(new MockEngineFactory(AssertingDirectoryReader.class));

        IndexService indexService = newIndexService(module);
        assertTrue(indexService.getSearcherWrapper() instanceof Wrapper);
        assertSame(indexService.getEngineFactory(), module.engineFactory.get());
        indexService.close("simon says", false);
    }


    public void testRegisterIndexStore() throws IOException {
        final Settings settings = Settings
            .builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "foo_store")
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        IndexModule module = new IndexModule(indexSettings, emptyAnalysisRegistry);
        module.addIndexStore("foo_store", FooStore::new);
        try {
            module.addIndexStore("foo_store", FooStore::new);
            fail("already registered");
        } catch (IllegalArgumentException ex) {
            // fine
        }

        IndexService indexService = newIndexService(module);
        assertTrue(indexService.getIndexStore() instanceof FooStore);

        indexService.close("simon says", false);
    }

    public void testOtherServiceBound() throws IOException {
        final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        final IndexEventListener eventListener = new IndexEventListener() {
            @Override
            public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
                atomicBoolean.set(true);
            }
        };
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        IndexModule module = new IndexModule(indexSettings, emptyAnalysisRegistry);
        module.addIndexEventListener(eventListener);
        IndexService indexService = newIndexService(module);
        IndexSettings x = indexService.getIndexSettings();
        assertEquals(x.getSettings(), indexSettings.getSettings());
        assertEquals(x.getIndex(), index);
        indexService.getIndexEventListener().beforeIndexRemoved(null, null);
        assertTrue(atomicBoolean.get());
        indexService.close("simon says", false);
    }


    public void testListener() throws IOException {
        Setting<Boolean> booleanSetting = Setting.boolSetting("index.foo.bar", false, Property.Dynamic, Property.IndexScope);
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(index, settings, booleanSetting), emptyAnalysisRegistry);
        Setting<Boolean> booleanSetting2 = Setting.boolSetting("index.foo.bar.baz", false, Property.Dynamic, Property.IndexScope);
        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        module.addSettingsUpdateConsumer(booleanSetting, atomicBoolean::set);

        try {
            module.addSettingsUpdateConsumer(booleanSetting2, atomicBoolean::set);
            fail("not registered");
        } catch (IllegalArgumentException ex) {

        }

        IndexService indexService = newIndexService(module);
        assertSame(booleanSetting, indexService.getIndexSettings().getScopedSettings().get(booleanSetting.getKey()));

        indexService.close("simon says", false);
    }

    public void testAddIndexOperationListener() throws IOException {
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(index, settings), emptyAnalysisRegistry);
        AtomicBoolean executed = new AtomicBoolean(false);
        IndexingOperationListener listener = new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
                executed.set(true);
                return operation;
            }
        };
        module.addIndexOperationListener(listener);

        expectThrows(IllegalArgumentException.class, () -> module.addIndexOperationListener(listener));
        expectThrows(IllegalArgumentException.class, () -> module.addIndexOperationListener(null));

        IndexService indexService = newIndexService(module);
        assertEquals(2, indexService.getIndexOperationListeners().size());
        assertEquals(IndexingSlowLog.class, indexService.getIndexOperationListeners().get(0).getClass());
        assertSame(listener, indexService.getIndexOperationListeners().get(1));

        ParsedDocument doc = InternalEngineTests.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(new Term("_uid",  Uid.createUidAsBytes(doc.type(), doc.id())), doc);
        ShardId shardId = new ShardId(new Index("foo", "bar"), 0);
        for (IndexingOperationListener l : indexService.getIndexOperationListeners()) {
            l.preIndex(shardId, index);
        }
        assertTrue(executed.get());
        indexService.close("simon says", false);
    }

    public void testAddSearchOperationListener() throws IOException {
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(index, settings), emptyAnalysisRegistry);
        AtomicBoolean executed = new AtomicBoolean(false);
        SearchOperationListener listener = new SearchOperationListener() {

            @Override
            public void onNewContext(SearchContext context) {
                executed.set(true);
            }
        };
        module.addSearchOperationListener(listener);

        expectThrows(IllegalArgumentException.class, () -> module.addSearchOperationListener(listener));
        expectThrows(IllegalArgumentException.class, () -> module.addSearchOperationListener(null));

        IndexService indexService = newIndexService(module);
        assertEquals(2, indexService.getSearchOperationListener().size());
        assertEquals(SearchSlowLog.class, indexService.getSearchOperationListener().get(0).getClass());
        assertSame(listener, indexService.getSearchOperationListener().get(1));

        for (SearchOperationListener l : indexService.getSearchOperationListener()) {
            l.onNewContext(new TestSearchContext(null));
        }
        assertTrue(executed.get());
        indexService.close("simon says", false);
    }

    public void testAddSimilarity() throws IOException {
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.similarity.my_similarity.type", "test_similarity")
                .put("index.similarity.my_similarity.key", "there is a key")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings("foo", indexSettings), emptyAnalysisRegistry);
        module.addSimilarity("test_similarity", (string, providerSettings, indexLevelSettings, scriptService) -> new SimilarityProvider() {
            @Override
            public String name() {
                return string;
            }

            @Override
            public Similarity get() {
                return new TestSimilarity(providerSettings.get("key"));
            }
        });

        IndexService indexService = newIndexService(module);
        SimilarityService similarityService = indexService.similarityService();
        assertNotNull(similarityService.getSimilarity("my_similarity"));
        assertTrue(similarityService.getSimilarity("my_similarity").get() instanceof TestSimilarity);
        assertEquals("my_similarity", similarityService.getSimilarity("my_similarity").name());
        assertEquals("there is a key", ((TestSimilarity) similarityService.getSimilarity("my_similarity").get()).key);
        indexService.close("simon says", false);
    }

    public void testFrozen() {
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(index, settings), emptyAnalysisRegistry);
        module.freeze();
        String msg = "Can't modify IndexModule once the index service has been created";
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addSearchOperationListener(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addIndexEventListener(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addIndexOperationListener(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addSimilarity(null, null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.setSearcherWrapper(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.forceQueryCacheProvider(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addIndexStore("foo", null)).getMessage());
    }

    public void testSetupUnknownSimilarity() throws IOException {
        Settings indexSettings = Settings.builder()
                .put("index.similarity.my_similarity.type", "test_similarity")
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings("foo", indexSettings), emptyAnalysisRegistry);
        Exception ex = expectThrows(IllegalArgumentException.class, () -> newIndexService(module));
        assertEquals("Unknown Similarity type [test_similarity] for [my_similarity]", ex.getMessage());
    }

    public void testSetupWithoutType() throws IOException {
        Settings indexSettings = Settings.builder()
                .put("index.similarity.my_similarity.foo", "bar")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings("foo", indexSettings), emptyAnalysisRegistry);
        Exception ex = expectThrows(IllegalArgumentException.class, () -> newIndexService(module));
        assertEquals("Similarity [my_similarity] must have an associated type", ex.getMessage());
    }

    public void testForceCustomQueryCache() throws IOException {
        Settings indexSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings("foo", indexSettings), emptyAnalysisRegistry);
        module.forceQueryCacheProvider((a, b) -> new CustomQueryCache());
        expectThrows(AlreadySetException.class, () -> module.forceQueryCacheProvider((a, b) -> new CustomQueryCache()));
        IndexService indexService = newIndexService(module);
        assertTrue(indexService.cache().query() instanceof CustomQueryCache);
        indexService.close("simon says", false);
    }

    public void testDefaultQueryCacheImplIsSelected() throws IOException {
        Settings indexSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings("foo", indexSettings), emptyAnalysisRegistry);
        IndexService indexService = newIndexService(module);
        assertTrue(indexService.cache().query() instanceof IndexQueryCache);
        indexService.close("simon says", false);
    }

    public void testDisableQueryCacheHasPrecedenceOverForceQueryCache() throws IOException {
        Settings indexSettings = Settings.builder()
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings("foo", indexSettings), emptyAnalysisRegistry);
        module.forceQueryCacheProvider((a, b) -> new CustomQueryCache());
        IndexService indexService = newIndexService(module);
        assertTrue(indexService.cache().query() instanceof DisabledQueryCache);
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
            return new Index("test", "_na_");
        }

        @Override
        public Weight doCache(Weight weight, QueryCachingPolicy policy) {
            return weight;
        }
    }

    private static class TestSimilarity extends Similarity {
        private final Similarity delegate = new BM25Similarity();
        private final String key;


        TestSimilarity(String key) {
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
        public SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
            return delegate.computeWeight(boost, collectionStats, termStats);
        }

        @Override
        public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
            return delegate.simScorer(weight, context);
        }
    }

    public static final class FooStore extends IndexStore {

        public FooStore(IndexSettings indexSettings) {
            super(indexSettings);
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
