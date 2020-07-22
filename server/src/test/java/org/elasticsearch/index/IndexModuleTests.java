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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.index.AssertingDirectoryReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.SetOnce.AlreadySetException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.cache.query.IndexQueryCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.engine.InternalEngineTests;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.similarity.NonNegativeScoresSimilarity;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.test.engine.MockEngineFactory;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.IndexService.IndexCreationContext.CREATE_INDEX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

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
        settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        indicesQueryCache = new IndicesQueryCache(settings);
        indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        index = indexSettings.getIndex();
        environment = TestEnvironment.newEnvironment(settings);
        emptyAnalysisRegistry = new AnalysisRegistry(environment, emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap(),
                emptyMap(), emptyMap(), emptyMap(), emptyMap());
        threadPool = new TestThreadPool("test");
        circuitBreakerService = new NoneCircuitBreakerService();
        PageCacheRecycler pageCacheRecycler = new PageCacheRecycler(settings);
        bigArrays = new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
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
        return module.newIndexService(CREATE_INDEX, nodeEnvironment, xContentRegistry(), deleter, circuitBreakerService, bigArrays,
                threadPool, scriptService, clusterService, null, indicesQueryCache, mapperRegistry,
                new IndicesFieldDataCache(settings, listener), writableRegistry(), () -> false, null);
    }

    public void testWrapperIsBound() throws IOException {
        final MockEngineFactory engineFactory = new MockEngineFactory(AssertingDirectoryReader.class);
        IndexModule module = new IndexModule(
                indexSettings,
                emptyAnalysisRegistry,
                engineFactory,
                Collections.emptyMap(),
                () -> true,
                new IndexNameExpressionResolver(),
                Collections.emptyMap());
        module.setReaderWrapper(s -> new Wrapper());

        IndexService indexService = newIndexService(module);
        assertTrue(indexService.getReaderWrapper() instanceof Wrapper);
        assertSame(indexService.getEngineFactory(), module.getEngineFactory());
        indexService.close("simon says", false);
    }


    public void testRegisterIndexStore() throws IOException {
        final Settings settings = Settings
            .builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "foo_store")
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        final Map<String, IndexStorePlugin.DirectoryFactory> indexStoreFactories = singletonMap(
            "foo_store", new FooFunction());
        final IndexModule module = new IndexModule(indexSettings, emptyAnalysisRegistry, new InternalEngineFactory(), indexStoreFactories,
            () -> true, new IndexNameExpressionResolver(), Collections.emptyMap());

        final IndexService indexService = newIndexService(module);
        assertThat(indexService.getDirectoryFactory(), instanceOf(FooFunction.class));

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
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry);
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
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings, booleanSetting);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry);
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
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry);
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
        Engine.Index index = new Engine.Index(new Term("_id",  Uid.encodeId(doc.id())), randomNonNegativeLong(), doc);
        ShardId shardId = new ShardId(new Index("foo", "bar"), 0);
        for (IndexingOperationListener l : indexService.getIndexOperationListeners()) {
            l.preIndex(shardId, index);
        }
        assertTrue(executed.get());
        indexService.close("simon says", false);
    }

    public void testAddSearchOperationListener() throws IOException {
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry);
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
        Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.similarity.my_similarity.type", "test_similarity")
                .put("index.similarity.my_similarity.key", "there is a key")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module =
                createIndexModule(indexSettings, emptyAnalysisRegistry);
        module.addSimilarity("test_similarity",
                (providerSettings, indexCreatedVersion, scriptService) -> new TestSimilarity(providerSettings.get("key")));

        IndexService indexService = newIndexService(module);
        SimilarityService similarityService = indexService.similarityService();
        Similarity similarity = similarityService.getSimilarity("my_similarity").get();
        assertNotNull(similarity);
        assertThat(similarity, Matchers.instanceOf(NonNegativeScoresSimilarity.class));
        similarity = ((NonNegativeScoresSimilarity) similarity).getDelegate();
        assertThat(similarity, Matchers.instanceOf(TestSimilarity.class));
        assertEquals("my_similarity", similarityService.getSimilarity("my_similarity").name());
        assertEquals("there is a key", ((TestSimilarity) similarity).key);
        indexService.close("simon says", false);
    }



    public void testFrozen() {
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry);
        module.freeze();
        String msg = "Can't modify IndexModule once the index service has been created";
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addSearchOperationListener(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addIndexEventListener(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addIndexOperationListener(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addSimilarity(null, null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.setReaderWrapper(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.forceQueryCacheProvider(null)).getMessage());
    }

    public void testSetupUnknownSimilarity() {
        Settings settings = Settings.builder()
                .put("index.similarity.my_similarity.type", "test_similarity")
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module =
                createIndexModule(indexSettings, emptyAnalysisRegistry);
        Exception ex = expectThrows(IllegalArgumentException.class, () -> newIndexService(module));
        assertEquals("Unknown Similarity type [test_similarity] for [my_similarity]", ex.getMessage());
    }

    public void testSetupWithoutType() {
        Settings settings = Settings.builder()
                .put("index.similarity.my_similarity.foo", "bar")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry);
        Exception ex = expectThrows(IllegalArgumentException.class, () -> newIndexService(module));
        assertEquals("Similarity [my_similarity] must have an associated type", ex.getMessage());
    }

    public void testForceCustomQueryCache() throws IOException {
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry);
        final Set<CustomQueryCache> liveQueryCaches = new HashSet<>();
        module.forceQueryCacheProvider((a, b) -> {
            final CustomQueryCache customQueryCache = new CustomQueryCache(liveQueryCaches);
            liveQueryCaches.add(customQueryCache);
            return customQueryCache;
        });
        expectThrows(AlreadySetException.class, () -> module.forceQueryCacheProvider((a, b) -> {
            throw new AssertionError("never called");
        }));
        IndexService indexService = newIndexService(module);
        assertTrue(indexService.cache().query() instanceof CustomQueryCache);
        indexService.close("simon says", false);
        assertThat(liveQueryCaches, empty());
    }

    public void testDefaultQueryCacheImplIsSelected() throws IOException {
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry);
        IndexService indexService = newIndexService(module);
        assertTrue(indexService.cache().query() instanceof IndexQueryCache);
        indexService.close("simon says", false);
    }

    public void testDisableQueryCacheHasPrecedenceOverForceQueryCache() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry);
        module.forceQueryCacheProvider((a, b) -> new CustomQueryCache(null));
        IndexService indexService = newIndexService(module);
        assertTrue(indexService.cache().query() instanceof DisabledQueryCache);
        indexService.close("simon says", false);
    }

    public void testCustomQueryCacheCleanedUpIfIndexServiceCreationFails() {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry);
        final Set<CustomQueryCache> liveQueryCaches = new HashSet<>();
        module.forceQueryCacheProvider((a, b) -> {
            final CustomQueryCache customQueryCache = new CustomQueryCache(liveQueryCaches);
            liveQueryCaches.add(customQueryCache);
            return customQueryCache;
        });
        threadPool.shutdown(); // causes index service creation to fail
        expectThrows(EsRejectedExecutionException.class, () -> newIndexService(module));
        assertThat(liveQueryCaches, empty());
    }

    public void testIndexAnalyzersCleanedUpIfIndexServiceCreationFails() {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);

        final HashSet<Analyzer> openAnalyzers = new HashSet<>();
        final AnalysisModule.AnalysisProvider<AnalyzerProvider<?>> analysisProvider = (i,e,n,s) -> new AnalyzerProvider<>() {
            @Override
            public String name() {
                return "test";
            }

            @Override
            public AnalyzerScope scope() {
                return AnalyzerScope.INDEX;
            }

            @Override
            public Analyzer get() {
                final Analyzer analyzer = new Analyzer() {
                    @Override
                    protected TokenStreamComponents createComponents(String fieldName) {
                        return new TokenStreamComponents(new StandardTokenizer());
                    }

                    @Override
                    public void close() {
                        super.close();
                        openAnalyzers.remove(this);
                    }
                };
                openAnalyzers.add(analyzer);
                return analyzer;
            }
        };
        final AnalysisRegistry analysisRegistry = new AnalysisRegistry(environment, emptyMap(), emptyMap(), emptyMap(),
            singletonMap("test", analysisProvider), emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap());
        IndexModule module = createIndexModule(indexSettings, analysisRegistry);
        threadPool.shutdown(); // causes index service creation to fail
        expectThrows(EsRejectedExecutionException.class, () -> newIndexService(module));
        assertThat(openAnalyzers, empty());
    }

    public void testMmapNotAllowed() {
        String storeType = randomFrom(IndexModule.Type.HYBRIDFS.getSettingsKey(), IndexModule.Type.MMAPFS.getSettingsKey());
        final Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put("index.store.type", storeType)
                .build();
        final Settings nodeSettings = Settings.builder()
                .put(IndexModule.NODE_STORE_ALLOW_MMAP.getKey(), false)
                .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(new Index("foo", "_na_"), settings, nodeSettings);
        final IndexModule module =
                createIndexModule(indexSettings, emptyAnalysisRegistry);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newIndexService(module));
        assertThat(e, hasToString(containsString("store type [" + storeType + "] is not allowed")));
    }

    public void testRegisterCustomRecoveryStateFactory() throws IOException {
        final Settings settings = Settings
            .builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexModule.INDEX_RECOVERY_TYPE_SETTING.getKey(), "test_recovery")
            .build();

        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);

        RecoveryState recoveryState = mock(RecoveryState.class);
        final Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories = singletonMap(
            "test_recovery", (shardRouting, targetNode, sourceNode) -> recoveryState);

        final IndexModule module = new IndexModule(indexSettings,
            emptyAnalysisRegistry,
            new InternalEngineFactory(),
            Collections.emptyMap(),
            () -> true,
            new IndexNameExpressionResolver(),
            recoveryStateFactories);

        final IndexService indexService = newIndexService(module);

        ShardRouting shard = createInitializedShardRouting();

        assertThat(indexService.createRecoveryState(shard, mock(DiscoveryNode.class), mock(DiscoveryNode.class)), is(recoveryState));

        indexService.close("closing", false);
    }

    private ShardRouting createInitializedShardRouting() {
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard = shard.initialize("node1", null, -1);
        return shard;
    }

    private static IndexModule createIndexModule(IndexSettings indexSettings, AnalysisRegistry emptyAnalysisRegistry) {
        return new IndexModule(indexSettings, emptyAnalysisRegistry, new InternalEngineFactory(), Collections.emptyMap(), () -> true,
            new IndexNameExpressionResolver(), Collections.emptyMap());
    }

    class CustomQueryCache implements QueryCache {

        private final Set<CustomQueryCache> liveQueryCaches;

        CustomQueryCache(Set<CustomQueryCache> liveQueryCaches) {
            this.liveQueryCaches = liveQueryCaches;
        }

        @Override
        public void clear(String reason) {
        }

        @Override
        public void close() {
            assertTrue(liveQueryCaches == null || liveQueryCaches.remove(this));
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
        public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
            return delegate.scorer(boost, collectionStats, termStats);
        }
    }

    public static final class FooFunction implements IndexStorePlugin.DirectoryFactory {

        @Override
        public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
            return new FsDirectoryFactory().newDirectory(indexSettings, shardPath);
        }
    }

    public static final class Wrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {
        @Override
        public DirectoryReader apply(DirectoryReader reader) {
            return null;
        }
    }
}
