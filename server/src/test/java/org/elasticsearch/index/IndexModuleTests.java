/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.tests.index.AssertingDirectoryReader;
import org.apache.lucene.util.SetOnce.AlreadySetException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
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
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.similarity.NonNegativeScoresSimilarity;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.engine.MockEngineFactory;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.IndexService.IndexCreationContext.CREATE_INDEX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
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
        public void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException {}

        @Override
        public void addPendingDelete(ShardId shardId, IndexSettings indexSettings) {}
    };

    private IndexStorePlugin.IndexFoldersDeletionListener indexDeletionListener = new IndexStorePlugin.IndexFoldersDeletionListener() {
        @Override
        public void beforeIndexFoldersDeleted(Index index, IndexSettings indexSettings, Path[] indexPaths) {}

        @Override
        public void beforeShardFoldersDeleted(ShardId shardId, IndexSettings indexSettings, Path[] shardPaths) {}
    };

    private final IndexFieldDataCache.Listener listener = new IndexFieldDataCache.Listener() {
    };
    private MapperRegistry mapperRegistry;
    private ThreadPool threadPool;
    private CircuitBreakerService circuitBreakerService;
    private BigArrays bigArrays;
    private ScriptService scriptService;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        indicesQueryCache = new IndicesQueryCache(settings);
        indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        index = indexSettings.getIndex();
        environment = TestEnvironment.newEnvironment(settings);
        emptyAnalysisRegistry = new AnalysisRegistry(
            environment,
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );
        threadPool = new TestThreadPool("test");
        circuitBreakerService = new NoneCircuitBreakerService();
        PageCacheRecycler pageCacheRecycler = new PageCacheRecycler(settings);
        bigArrays = new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
        scriptService = new ScriptService(settings, Collections.emptyMap(), Collections.emptyMap(), () -> 1L);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        nodeEnvironment = new NodeEnvironment(settings, environment);
        mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
        indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(nodeEnvironment, indicesQueryCache, clusterService);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private IndexService newIndexService(IndexModule module) throws IOException {
        return module.newIndexService(
            CREATE_INDEX,
            nodeEnvironment,
            parserConfig(),
            deleter,
            circuitBreakerService,
            bigArrays,
            threadPool,
            scriptService,
            clusterService,
            null,
            indicesQueryCache,
            mapperRegistry,
            new IndicesFieldDataCache(settings, listener),
            writableRegistry(),
            module.indexSettings().getMode().idFieldMapperWithoutFieldData(),
            null,
            indexDeletionListener,
            emptyMap()
        );
    }

    public void testWrapperIsBound() throws IOException {
        final MockEngineFactory engineFactory = new MockEngineFactory(AssertingDirectoryReader.class);
        IndexModule module = new IndexModule(
            indexSettings,
            emptyAnalysisRegistry,
            engineFactory,
            Collections.emptyMap(),
            () -> true,
            indexNameExpressionResolver,
            Collections.emptyMap()
        );
        module.setReaderWrapper(s -> new Wrapper());

        IndexService indexService = newIndexService(module);
        assertTrue(indexService.getReaderWrapper() instanceof Wrapper);
        assertSame(indexService.getEngineFactory(), module.getEngineFactory());
        indexService.close("simon says", false);
    }

    public void testRegisterIndexStore() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "foo_store")
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        final Map<String, IndexStorePlugin.DirectoryFactory> indexStoreFactories = singletonMap("foo_store", new FooFunction());
        final IndexModule module = new IndexModule(
            indexSettings,
            emptyAnalysisRegistry,
            new InternalEngineFactory(),
            indexStoreFactories,
            () -> true,
            indexNameExpressionResolver,
            Collections.emptyMap()
        );

        final IndexService indexService = newIndexService(module);
        assertThat(indexService.getDirectoryFactory(), instanceOf(FooFunction.class));

        indexService.close("simon says", false);
    }

    public void testDirectoryWrapper() throws IOException {
        final Path homeDir = createTempDir();
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), homeDir.toString())
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        final IndexModule module = new IndexModule(
            indexSettings,
            emptyAnalysisRegistry,
            new InternalEngineFactory(),
            Map.of(),
            () -> true,
            indexNameExpressionResolver,
            Collections.emptyMap()
        );

        module.setDirectoryWrapper(new TestDirectoryWrapper());

        final IndexService indexService = newIndexService(module);
        assertSame(indexService.getEngineFactory(), module.getEngineFactory());
        final IndexStorePlugin.DirectoryFactory directoryFactory = indexService.getDirectoryFactory();
        assertThat(directoryFactory, notNullValue());

        final ShardId shardId = new ShardId(indexSettings.getIndex(), randomIntBetween(0, 5));
        final Path dataPath = new NodeEnvironment.DataPath(homeDir).resolve(shardId);
        Directory directory = directoryFactory.newDirectory(indexSettings, new ShardPath(false, dataPath, dataPath, shardId));
        assertThat(directory, instanceOf(WrappedDirectory.class));
        assertThat(((WrappedDirectory) directory).shardRouting, nullValue());
        assertThat(directory, instanceOf(FilterDirectory.class));

        final ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            randomIdentifier(),
            randomBoolean(),
            ShardRoutingState.INITIALIZING
        );
        directory = directoryFactory.newDirectory(indexSettings, new ShardPath(false, dataPath, dataPath, shardId), shardRouting);
        assertThat(directory, instanceOf(WrappedDirectory.class));
        assertThat(((WrappedDirectory) directory).shardRouting, sameInstance(shardRouting));
        assertThat(directory, instanceOf(FilterDirectory.class));

        indexService.close("test done", false);
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
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
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
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
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
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
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

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(new Term("_id", Uid.encodeId(doc.id())), randomNonNegativeLong(), doc);
        ShardId shardId = new ShardId(new Index("foo", "bar"), 0);
        for (IndexingOperationListener l : indexService.getIndexOperationListeners()) {
            l.preIndex(shardId, index);
        }
        assertTrue(executed.get());
        indexService.close("simon says", false);
    }

    public void testAddSearchOperationListener() throws IOException {
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
        AtomicBoolean executed = new AtomicBoolean(false);
        SearchOperationListener listener = new SearchOperationListener() {
            @Override
            public void onNewReaderContext(ReaderContext readerContext) {
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
            l.onNewReaderContext(mock(ReaderContext.class));
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
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
        module.addSimilarity(
            "test_similarity",
            (providerSettings, indexCreatedVersion, scriptService) -> new TestSimilarity(providerSettings.get("key"))
        );

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
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
        module.freeze();
        String msg = "Can't modify IndexModule once the index service has been created";
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addSearchOperationListener(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addIndexEventListener(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addIndexOperationListener(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.addSimilarity(null, null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.setReaderWrapper(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.forceQueryCacheProvider(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.setDirectoryWrapper(null)).getMessage());
        assertEquals(msg, expectThrows(IllegalStateException.class, () -> module.setIndexCommitListener(null)).getMessage());
    }

    public void testSetupUnknownSimilarity() {
        Settings settings = Settings.builder()
            .put("index.similarity.my_similarity.type", "test_similarity")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
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
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
        Exception ex = expectThrows(IllegalArgumentException.class, () -> newIndexService(module));
        assertEquals("Similarity [my_similarity] must have an associated type", ex.getMessage());
    }

    public void testForceCustomQueryCache() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
        final Set<CustomQueryCache> liveQueryCaches = new HashSet<>();
        module.forceQueryCacheProvider((a, b) -> {
            final CustomQueryCache customQueryCache = new CustomQueryCache(liveQueryCaches);
            liveQueryCaches.add(customQueryCache);
            return customQueryCache;
        });
        expectThrows(
            AlreadySetException.class,
            () -> module.forceQueryCacheProvider((a, b) -> { throw new AssertionError("never called"); })
        );
        IndexService indexService = newIndexService(module);
        assertTrue(indexService.cache().query() instanceof CustomQueryCache);
        indexService.close("simon says", false);
        assertThat(liveQueryCaches, empty());
    }

    public void testDefaultQueryCacheImplIsSelected() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
        IndexService indexService = newIndexService(module);
        assertTrue(indexService.cache().query() instanceof IndexQueryCache);
        indexService.close("simon says", false);
    }

    public void testDisableQueryCacheHasPrecedenceOverForceQueryCache() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
        module.forceQueryCacheProvider((a, b) -> new CustomQueryCache(null));
        IndexService indexService = newIndexService(module);
        assertTrue(indexService.cache().query() instanceof DisabledQueryCache);
        indexService.close("simon says", false);
    }

    public void testCustomQueryCacheCleanedUpIfIndexServiceCreationFails() {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
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
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);

        final HashSet<Analyzer> openAnalyzers = new HashSet<>();
        final AnalysisModule.AnalysisProvider<AnalyzerProvider<?>> analysisProvider = (i, e, n, s) -> new AnalyzerProvider<>() {
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
        final AnalysisRegistry analysisRegistry = new AnalysisRegistry(
            environment,
            emptyMap(),
            emptyMap(),
            emptyMap(),
            singletonMap("test", analysisProvider),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );
        IndexModule module = createIndexModule(indexSettings, analysisRegistry, indexNameExpressionResolver);
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
        final Settings nodeSettings = Settings.builder().put(IndexModule.NODE_STORE_ALLOW_MMAP.getKey(), false).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(new Index("foo", "_na_"), settings, nodeSettings);
        final IndexModule module = createIndexModule(indexSettings, emptyAnalysisRegistry, indexNameExpressionResolver);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newIndexService(module));
        assertThat(e, hasToString(containsString("store type [" + storeType + "] is not allowed")));
    }

    public void testRegisterCustomRecoveryStateFactory() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexModule.INDEX_RECOVERY_TYPE_SETTING.getKey(), "test_recovery")
            .build();

        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);

        RecoveryState recoveryState = mock(RecoveryState.class);
        final Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories = singletonMap(
            "test_recovery",
            (shardRouting, targetNode, sourceNode) -> recoveryState
        );

        final IndexModule module = new IndexModule(
            indexSettings,
            emptyAnalysisRegistry,
            new InternalEngineFactory(),
            Collections.emptyMap(),
            () -> true,
            indexNameExpressionResolver,
            recoveryStateFactories
        );

        final IndexService indexService = newIndexService(module);

        ShardRouting shard = createInitializedShardRouting();

        assertThat(indexService.createRecoveryState(shard, mock(DiscoveryNode.class), mock(DiscoveryNode.class)), is(recoveryState));

        indexService.close("closing", false);
    }

    public void testIndexCommitListenerIsBound() throws IOException, ExecutionException, InterruptedException {
        IndexModule module = new IndexModule(
            indexSettings,
            emptyAnalysisRegistry,
            InternalEngine::new,
            Collections.emptyMap(),
            () -> true,
            indexNameExpressionResolver,
            Collections.emptyMap()
        );

        final AtomicLong lastAcquiredPrimaryTerm = new AtomicLong();
        final AtomicReference<Engine.IndexCommitRef> lastAcquiredCommit = new AtomicReference<>();
        final AtomicReference<IndexCommit> lastDeletedCommit = new AtomicReference<>();

        module.setIndexCommitListener(new Engine.IndexCommitListener() {
            @Override
            public void onNewCommit(
                ShardId shardId,
                Store store,
                long primaryTerm,
                Engine.IndexCommitRef indexCommitRef,
                Set<String> additionalFiles
            ) {
                lastAcquiredPrimaryTerm.set(primaryTerm);
                lastAcquiredCommit.set(indexCommitRef);
            }

            @Override
            public void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit) {
                lastDeletedCommit.set(deletedCommit);
            }
        });

        final List<Closeable> closeables = new ArrayList<>();
        try {
            ShardId shardId = new ShardId("index", UUIDs.randomBase64UUID(random()), 0);
            ShardRouting shardRouting = ShardRouting.newUnassigned(
                shardId,
                true,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
                ShardRouting.Role.DEFAULT
            ).initialize("_node_id", null, -1);

            IndexService indexService = newIndexService(module);
            closeables.add(() -> indexService.close("close index service at end of test", false));

            IndexShard indexShard = indexService.createShard(shardRouting, IndexShardTestCase.NOOP_GCP_SYNCER, RetentionLeaseSyncer.EMPTY);
            closeables.add(() -> indexShard.close("close shard at end of test", true));
            indexShard.markAsRecovering("test", new RecoveryState(shardRouting, DiscoveryNodeUtils.create("_node_id", "_node_id"), null));

            final PlainActionFuture<Boolean> recoveryFuture = PlainActionFuture.newFuture();
            indexShard.recoverFromStore(recoveryFuture);
            recoveryFuture.get();

            assertThat(lastAcquiredPrimaryTerm.get(), equalTo(indexShard.getOperationPrimaryTerm()));
            Engine.IndexCommitRef lastCommitRef = lastAcquiredCommit.get();
            assertThat(lastCommitRef, notNullValue());
            IndexCommit lastCommit = lastCommitRef.getIndexCommit();
            assertThat(lastCommit.getGeneration(), equalTo(2L));
            IndexCommit lastDeleted = lastDeletedCommit.get();
            assertThat(lastDeleted, nullValue());

            lastCommitRef.close();

            indexShard.flush(new FlushRequest("index").force(true));

            lastDeleted = lastDeletedCommit.get();
            assertThat(lastDeleted.getGeneration(), equalTo(lastCommit.getGeneration()));
            assertThat(lastDeleted.getSegmentsFileName(), equalTo(lastCommit.getSegmentsFileName()));
            assertThat(lastDeleted.isDeleted(), equalTo(true));

            lastCommitRef = lastAcquiredCommit.get();
            assertThat(lastCommitRef, notNullValue());
            lastCommit = lastCommitRef.getIndexCommit();
            assertThat(lastCommit.getGeneration(), equalTo(3L));

            lastCommitRef.close();
        } finally {
            IOUtils.close(closeables);
        }
    }

    private ShardRouting createInitializedShardRouting() {
        ShardRouting shard = ShardRouting.newUnassigned(
            new ShardId("test", "_na_", 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        );
        shard = shard.initialize("node1", null, -1);
        return shard;
    }

    private static IndexModule createIndexModule(
        IndexSettings indexSettings,
        AnalysisRegistry emptyAnalysisRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        return new IndexModule(
            indexSettings,
            emptyAnalysisRegistry,
            new InternalEngineFactory(),
            Collections.emptyMap(),
            () -> true,
            indexNameExpressionResolver,
            Collections.emptyMap()
        );
    }

    class CustomQueryCache implements QueryCache {

        private final Set<CustomQueryCache> liveQueryCaches;

        CustomQueryCache(Set<CustomQueryCache> liveQueryCaches) {
            this.liveQueryCaches = liveQueryCaches;
        }

        @Override
        public void clear(String reason) {}

        @Override
        public void close() {
            assertTrue(liveQueryCaches == null || liveQueryCaches.remove(this));
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

    private static final class TestDirectoryWrapper implements IndexModule.DirectoryWrapper {

        @Override
        public Directory wrap(Directory delegate, ShardRouting shardRouting) {
            return new WrappedDirectory(delegate, shardRouting);
        }
    }

    private static final class WrappedDirectory extends FilterDirectory {

        final ShardRouting shardRouting;

        protected WrappedDirectory(Directory in, ShardRouting shardRouting) {
            super(in);
            this.shardRouting = shardRouting;
        }
    }
}
