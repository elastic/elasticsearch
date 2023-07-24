/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader.CacheHelper;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.bulk.stats.BulkStats;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.DataRewriteContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.GlobalCheckpointSyncer;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.store.CompositeIndexFoldersDeletionListener;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.util.CollectionUtils.arrayAsArrayList;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.IndexService.IndexCreationContext.CREATE_INDEX;
import static org.elasticsearch.index.IndexService.IndexCreationContext.METADATA_VERIFICATION;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;
import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

public class IndicesService extends AbstractLifecycleComponent
    implements
        IndicesClusterStateService.AllocatedIndices<IndexShard, IndexService>,
        IndexService.ShardStoreDeleter {
    private static final Logger logger = LogManager.getLogger(IndicesService.class);

    public static final String INDICES_SHARDS_CLOSED_TIMEOUT = "indices.shards_closed_timeout";
    public static final Setting<TimeValue> INDICES_CACHE_CLEAN_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "indices.cache.cleanup_interval",
        TimeValue.timeValueMinutes(1),
        Property.NodeScope
    );
    public static final Setting<Boolean> INDICES_ID_FIELD_DATA_ENABLED_SETTING = Setting.boolSetting(
        "indices.id_field_data.enabled",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> WRITE_DANGLING_INDICES_INFO_SETTING = Setting.boolSetting(
        "gateway.write_dangling_indices_info",
        true,
        Setting.Property.NodeScope
    );

    /**
     * The node's settings.
     */
    private final Settings settings;
    private final PluginsService pluginsService;
    private final NodeEnvironment nodeEnv;
    private final XContentParserConfiguration parserConfig;
    private final TimeValue shardsClosedTimeout;
    private final AnalysisRegistry analysisRegistry;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;
    private final IndicesFieldDataCache indicesFieldDataCache;
    private final CacheCleaner cacheCleaner;
    private final ThreadPool threadPool;
    private final CircuitBreakerService circuitBreakerService;
    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final Client client;
    private volatile Map<String, IndexService> indices = Map.of();
    private final Map<Index, List<PendingDelete>> pendingDeletes = new HashMap<>();
    private final AtomicInteger numUncompletedDeletes = new AtomicInteger();
    private final OldShardsStats oldShardsStats = new OldShardsStats();
    private final MapperRegistry mapperRegistry;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final Map<String, IndexStorePlugin.SnapshotCommitSupplier> snapshotCommitSuppliers;
    private final IndexingMemoryController indexingMemoryController;
    private final TimeValue cleanInterval;
    final IndicesRequestCache indicesRequestCache; // pkg-private for testing
    private final IndicesQueryCache indicesQueryCache;
    private final MetaStateService metaStateService;
    private final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders;
    private final Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories;
    private final Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories;
    private final IndexStorePlugin.IndexFoldersDeletionListener indexFoldersDeletionListeners;
    final AbstractRefCounted indicesRefCount; // pkg-private for testing
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    private volatile boolean idFieldDataEnabled;
    private volatile boolean allowExpensiveQueries;

    private final Function<IndexMode, IdFieldMapper> idFieldMappers;

    @Nullable
    private final EsThreadPoolExecutor danglingIndicesThreadPoolExecutor;
    private final Set<Index> danglingIndicesToWrite = ConcurrentCollections.newConcurrentSet();
    private final boolean nodeWriteDanglingIndicesInfo;
    private final ValuesSourceRegistry valuesSourceRegistry;
    private final TimestampFieldMapperService timestampFieldMapperService;
    private final CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> requestCacheKeyDifferentiator;

    @Override
    protected void doStart() {
        // Start thread that will manage cleaning the field data cache periodically
        threadPool.schedule(this.cacheCleaner, this.cleanInterval, ThreadPool.Names.SAME);

        // Start watching for timestamp fields
        clusterService.addStateApplier(timestampFieldMapperService);
    }

    public IndicesService(
        Settings settings,
        PluginsService pluginsService,
        NodeEnvironment nodeEnv,
        NamedXContentRegistry xContentRegistry,
        AnalysisRegistry analysisRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MapperRegistry mapperRegistry,
        NamedWriteableRegistry namedWriteableRegistry,
        ThreadPool threadPool,
        IndexScopedSettings indexScopedSettings,
        CircuitBreakerService circuitBreakerService,
        BigArrays bigArrays,
        ScriptService scriptService,
        ClusterService clusterService,
        Client client,
        MetaStateService metaStateService,
        Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders,
        Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories,
        ValuesSourceRegistry valuesSourceRegistry,
        Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories,
        List<IndexStorePlugin.IndexFoldersDeletionListener> indexFoldersDeletionListeners,
        Map<String, IndexStorePlugin.SnapshotCommitSupplier> snapshotCommitSuppliers,
        CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> requestCacheKeyDifferentiator
    ) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.pluginsService = pluginsService;
        this.nodeEnv = nodeEnv;
        this.parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE)
            .withRegistry(xContentRegistry);
        this.valuesSourceRegistry = valuesSourceRegistry;
        this.shardsClosedTimeout = settings.getAsTime(INDICES_SHARDS_CLOSED_TIMEOUT, new TimeValue(1, TimeUnit.DAYS));
        this.analysisRegistry = analysisRegistry;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indicesRequestCache = new IndicesRequestCache(settings);
        this.indicesQueryCache = new IndicesQueryCache(settings);
        this.mapperRegistry = mapperRegistry;
        this.namedWriteableRegistry = namedWriteableRegistry;
        indexingMemoryController = new IndexingMemoryController(
            settings,
            threadPool,
            // ensure we pull an iter with new shards - flatten makes a copy
            () -> Iterables.flatten(this).iterator()
        );
        this.indexScopedSettings = indexScopedSettings;
        this.circuitBreakerService = circuitBreakerService;
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.client = client;
        this.idFieldDataEnabled = INDICES_ID_FIELD_DATA_ENABLED_SETTING.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(INDICES_ID_FIELD_DATA_ENABLED_SETTING, this::setIdFieldDataEnabled);
        this.indicesFieldDataCache = new IndicesFieldDataCache(settings, new IndexFieldDataCache.Listener() {
            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                assert sizeInBytes >= 0
                    : "When reducing circuit breaker, it should be adjusted with a number higher or "
                        + "equal to 0 and not ["
                        + sizeInBytes
                        + "]";
                circuitBreakerService.getBreaker(CircuitBreaker.FIELDDATA).addWithoutBreaking(-sizeInBytes);
            }
        });
        this.cleanInterval = INDICES_CACHE_CLEAN_INTERVAL_SETTING.get(settings);
        this.cacheCleaner = new CacheCleaner(indicesFieldDataCache, indicesRequestCache, logger, threadPool, this.cleanInterval);
        this.metaStateService = metaStateService;
        this.engineFactoryProviders = engineFactoryProviders;

        // do not allow any plugin-provided index store type to conflict with a built-in type
        for (final String indexStoreType : directoryFactories.keySet()) {
            if (IndexModule.isBuiltinType(indexStoreType)) {
                throw new IllegalStateException("registered index store type [" + indexStoreType + "] conflicts with a built-in type");
            }
        }

        this.directoryFactories = directoryFactories;
        this.recoveryStateFactories = recoveryStateFactories;
        this.indexFoldersDeletionListeners = new CompositeIndexFoldersDeletionListener(indexFoldersDeletionListeners);
        this.snapshotCommitSuppliers = snapshotCommitSuppliers;
        this.requestCacheKeyDifferentiator = requestCacheKeyDifferentiator;
        // doClose() is called when shutting down a node, yet there might still be ongoing requests
        // that we need to wait for before closing some resources such as the caches. In order to
        // avoid closing these resources while ongoing requests are still being processed, we use a
        // ref count which will only close them when both this service and all index services are
        // actually closed
        indicesRefCount = AbstractRefCounted.of(() -> {
            try {
                IOUtils.close(
                    analysisRegistry,
                    indexingMemoryController,
                    indicesFieldDataCache,
                    cacheCleaner,
                    indicesRequestCache,
                    indicesQueryCache
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                closeLatch.countDown();
            }
        });

        Map<IndexMode, IdFieldMapper> idFieldMappers = new EnumMap<>(IndexMode.class);
        for (IndexMode mode : IndexMode.values()) {
            idFieldMappers.put(mode, mode.buildIdFieldMapper(() -> idFieldDataEnabled));
        }
        this.idFieldMappers = idFieldMappers::get;

        final String nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(settings));
        nodeWriteDanglingIndicesInfo = WRITE_DANGLING_INDICES_INFO_SETTING.get(settings);
        danglingIndicesThreadPoolExecutor = nodeWriteDanglingIndicesInfo
            ? EsExecutors.newScaling(
                nodeName + "/" + DANGLING_INDICES_UPDATE_THREAD_NAME,
                1,
                1,
                0,
                TimeUnit.MILLISECONDS,
                true,
                daemonThreadFactory(nodeName, DANGLING_INDICES_UPDATE_THREAD_NAME),
                threadPool.getThreadContext()
            )
            : null;

        this.allowExpensiveQueries = ALLOW_EXPENSIVE_QUERIES.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ALLOW_EXPENSIVE_QUERIES, this::setAllowExpensiveQueries);

        this.timestampFieldMapperService = new TimestampFieldMapperService(settings, threadPool, this);
    }

    private static final String DANGLING_INDICES_UPDATE_THREAD_NAME = "DanglingIndices#updateTask";

    public ClusterService clusterService() {
        return clusterService;
    }

    @Override
    protected void doStop() {
        clusterService.removeApplier(timestampFieldMapperService);
        timestampFieldMapperService.doStop();

        ThreadPool.terminate(danglingIndicesThreadPoolExecutor, 10, TimeUnit.SECONDS);

        ExecutorService indicesStopExecutor = Executors.newFixedThreadPool(5, daemonThreadFactory(settings, "indices_shutdown"));

        // Copy indices because we modify it asynchronously in the body of the loop
        final Set<Index> indices = this.indices.values().stream().map(s -> s.index()).collect(Collectors.toSet());
        final CountDownLatch latch = new CountDownLatch(indices.size());
        for (final Index index : indices) {
            indicesStopExecutor.execute(() -> {
                try {
                    removeIndex(index, IndexRemovalReason.SHUTDOWN, "shutdown");
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            if (latch.await(shardsClosedTimeout.seconds(), TimeUnit.SECONDS) == false) {
                logger.warn("Not all shards are closed yet, waited {}sec - stopping service", shardsClosedTimeout.seconds());
            }
        } catch (InterruptedException e) {
            // ignore
        } finally {
            indicesStopExecutor.shutdown();
        }
    }

    @Override
    protected void doClose() throws IOException {
        indicesRefCount.decRef();
    }

    /**
     * Wait for this {@link IndicesService} to be effectively closed. When this returns {@code true}, all shards and shard stores
     * are closed and all shard {@link CacheHelper#addClosedListener(org.apache.lucene.index.IndexReader.ClosedListener) closed
     * listeners} have run. However some {@link IndexEventListener#onStoreClosed(ShardId) shard closed listeners} might not have
     * run.
     * @return true if all shards closed within the given timeout, false otherwise
     * @throws InterruptedException if the current thread got interrupted while waiting for shards to close
     */
    public boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return closeLatch.await(timeout, timeUnit);
    }

    public NodeIndicesStats stats(CommonStatsFlags flags) {
        CommonStats commonStats = new CommonStats(flags);
        // the cumulative statistics also account for shards that are no longer on this node, which is tracked by oldShardsStats
        for (Flag flag : flags.getFlags()) {
            switch (flag) {
                case Get -> commonStats.get.add(oldShardsStats.getStats);
                case Indexing -> commonStats.indexing.add(oldShardsStats.indexingStats);
                case Search -> commonStats.search.add(oldShardsStats.searchStats);
                case Merge -> commonStats.merge.add(oldShardsStats.mergeStats);
                case Refresh -> commonStats.refresh.add(oldShardsStats.refreshStats);
                case Recovery -> commonStats.recoveryStats.add(oldShardsStats.recoveryStats);
                case Flush -> commonStats.flush.add(oldShardsStats.flushStats);
                case Bulk -> commonStats.bulk.add(oldShardsStats.bulkStats);
            }
        }

        return new NodeIndicesStats(commonStats, statsByIndex(this, flags), statsByShard(this, flags));
    }

    static Map<Index, CommonStats> statsByIndex(final IndicesService indicesService, final CommonStatsFlags flags) {
        // Currently only the Mappings flag is the only possible index-level flag.
        if (flags.isSet(CommonStatsFlags.Flag.Mappings) == false) {
            return Map.of();
        }

        final Map<Index, CommonStats> statsByIndex = Maps.newHashMapWithExpectedSize(indicesService.indices.size());
        for (final IndexService indexService : indicesService) {
            Index index = indexService.index();
            CommonStats commonStats = new CommonStats(CommonStatsFlags.NONE);
            commonStats.nodeMappings = indexService.getNodeMappingStats();
            var existing = statsByIndex.putIfAbsent(index, commonStats);
            assert existing == null;
        }
        return statsByIndex;
    }

    static Map<Index, List<IndexShardStats>> statsByShard(final IndicesService indicesService, final CommonStatsFlags flags) {
        final Map<Index, List<IndexShardStats>> statsByShard = new HashMap<>();

        for (final IndexService indexService : indicesService) {
            for (final IndexShard indexShard : indexService) {
                try {
                    final IndexShardStats indexShardStats = indicesService.indexShardStats(indicesService, indexShard, flags);

                    if (indexShardStats == null) {
                        continue;
                    }

                    if (statsByShard.containsKey(indexService.index()) == false) {
                        statsByShard.put(indexService.index(), arrayAsArrayList(indexShardStats));
                    } else {
                        statsByShard.get(indexService.index()).add(indexShardStats);
                    }
                } catch (IllegalIndexShardStateException | AlreadyClosedException e) {
                    // we can safely ignore illegal state on ones that are closing for example
                    logger.trace(() -> format("%s ignoring shard stats", indexShard.shardId()), e);
                }
            }
        }

        return statsByShard;
    }

    IndexShardStats indexShardStats(final IndicesService indicesService, final IndexShard indexShard, final CommonStatsFlags flags) {
        if (indexShard.routingEntry() == null) {
            return null;
        }

        CommitStats commitStats;
        SeqNoStats seqNoStats;
        RetentionLeaseStats retentionLeaseStats;
        try {
            commitStats = indexShard.commitStats();
            seqNoStats = indexShard.seqNoStats();
            retentionLeaseStats = indexShard.getRetentionLeaseStats();
        } catch (AlreadyClosedException e) {
            // shard is closed - no stats is fine
            commitStats = null;
            seqNoStats = null;
            retentionLeaseStats = null;
        }

        return new IndexShardStats(
            indexShard.shardId(),
            new ShardStats[] {
                new ShardStats(
                    indexShard.routingEntry(),
                    indexShard.shardPath(),
                    CommonStats.getShardLevelStats(indicesService.getIndicesQueryCache(), indexShard, flags),
                    commitStats,
                    seqNoStats,
                    retentionLeaseStats,
                    indexShard.isSearchIdle(),
                    indexShard.searchIdleTime()
                ) }
        );
    }

    /**
     * Checks if changes (adding / removing) indices, shards and so on are allowed.
     *
     * @throws IllegalStateException if no changes allowed.
     */
    private void ensureChangesAllowed() {
        if (lifecycle.started() == false) {
            throw new IllegalStateException("Can't make changes to indices service, node is closed");
        }
    }

    @Override
    public Iterator<IndexService> iterator() {
        return indices.values().iterator();
    }

    public boolean hasIndex(Index index) {
        return indices.containsKey(index.getUUID());
    }

    /**
     * Returns an IndexService for the specified index if exists otherwise returns <code>null</code>.
     */
    @Override
    @Nullable
    public IndexService indexService(Index index) {
        return indices.get(index.getUUID());
    }

    /**
     * Returns an IndexService for the specified index if exists otherwise a {@link IndexNotFoundException} is thrown.
     */
    public IndexService indexServiceSafe(Index index) {
        IndexService indexService = indices.get(index.getUUID());
        if (indexService == null) {
            throw new IndexNotFoundException(index);
        }
        assert indexService.indexUUID().equals(index.getUUID())
            : "uuid mismatch local: " + indexService.indexUUID() + " incoming: " + index.getUUID();
        return indexService;
    }

    /**
     * Creates a new {@link IndexService} for the given metadata.
     *
     * @param indexMetadata          the index metadata to create the index for
     * @param builtInListeners       a list of built-in lifecycle {@link IndexEventListener} that should should be used along side with the
     *                               per-index listeners
     * @throws ResourceAlreadyExistsException if the index already exists.
     */
    @Override
    public synchronized IndexService createIndex(
        final IndexMetadata indexMetadata,
        final List<IndexEventListener> builtInListeners,
        final boolean writeDanglingIndices
    ) throws IOException {
        ensureChangesAllowed();
        if (indexMetadata.getIndexUUID().equals(IndexMetadata.INDEX_UUID_NA_VALUE)) {
            throw new IllegalArgumentException("index must have a real UUID found value: [" + indexMetadata.getIndexUUID() + "]");
        }
        final Index index = indexMetadata.getIndex();
        if (hasIndex(index)) {
            throw new ResourceAlreadyExistsException(index);
        }
        List<IndexEventListener> finalListeners = new ArrayList<>(builtInListeners);
        final IndexEventListener onStoreClose = new IndexEventListener() {
            @Override
            public void onStoreCreated(ShardId shardId) {
                indicesRefCount.incRef();
            }

            @Override
            public void onStoreClosed(ShardId shardId) {
                try {
                    indicesQueryCache.onClose(shardId);
                } finally {
                    indicesRefCount.decRef();
                }
            }
        };
        final IndexEventListener beforeIndexShardRecovery = new IndexEventListener() {
            volatile boolean reloaded;

            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                try {
                    if (indexShard.mapperService() != null) {
                        // we need to reload once, not on every shard recovery in case multiple shards are on the same node
                        if (reloaded == false) {
                            synchronized (indexShard.mapperService()) {
                                if (reloaded == false) {
                                    // we finish loading analyzers from resources here
                                    // during shard recovery in the generic thread pool,
                                    // as this may require longer running operations and blocking calls
                                    indexShard.mapperService().reloadSearchAnalyzers(getAnalysis(), null, false);
                                }
                                reloaded = true;
                            }
                        }
                    }
                    listener.onResponse(null);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        };
        finalListeners.add(onStoreClose);
        finalListeners.add(oldShardsStats);
        finalListeners.add(beforeIndexShardRecovery);
        IndexService indexService;
        try (var ignored = threadPool.getThreadContext().newStoredContext()) {
            indexService = createIndexService(
                CREATE_INDEX,
                indexMetadata,
                indicesQueryCache,
                indicesFieldDataCache,
                finalListeners,
                indexingMemoryController
            );
        }
        boolean success = false;
        try {
            if (writeDanglingIndices && nodeWriteDanglingIndicesInfo) {
                indexService.addMetadataListener(imd -> updateDanglingIndicesInfo(index));
            }
            indexService.getIndexEventListener().afterIndexCreated(indexService);
            indices = Maps.copyMapWithAddedEntry(indices, index.getUUID(), indexService);
            if (writeDanglingIndices) {
                if (nodeWriteDanglingIndicesInfo) {
                    updateDanglingIndicesInfo(index);
                } else {
                    indexService.deleteDanglingIndicesInfo();
                }
            }
            success = true;
            return indexService;
        } finally {
            if (success == false) {
                indexService.close("plugins_failed", true);
            }
        }
    }

    public <T, E extends Exception> T withTempIndexService(
        final IndexMetadata indexMetadata,
        CheckedFunction<IndexService, T, E> indexServiceConsumer
    ) throws IOException, E {
        final Index index = indexMetadata.getIndex();
        if (hasIndex(index)) {
            throw new ResourceAlreadyExistsException(index);
        }
        List<IndexEventListener> finalListeners = List.of(
            // double check that shard is not created.
            new IndexEventListener() {
                @Override
                public void beforeIndexShardCreated(ShardRouting shardRouting, Settings indexSettings) {
                    assert false : "temp index should not trigger shard creation";
                    throw new ElasticsearchException("temp index should not trigger shard creation [{}]", index);
                }

                @Override
                public void onStoreCreated(ShardId shardId) {
                    assert false : "temp index should not trigger store creation";
                    throw new ElasticsearchException("temp index should not trigger store creation [{}]", index);
                }
            }
        );
        final IndexService indexService = createIndexService(
            CREATE_INDEX,
            indexMetadata,
            indicesQueryCache,
            indicesFieldDataCache,
            finalListeners,
            indexingMemoryController
        );
        try (Closeable dummy = () -> indexService.close("temp", false)) {
            return indexServiceConsumer.apply(indexService);
        }
    }

    /**
     * This creates a new IndexService without registering it
     */
    private synchronized IndexService createIndexService(
        IndexService.IndexCreationContext indexCreationContext,
        IndexMetadata indexMetadata,
        IndicesQueryCache indicesQueryCache,
        IndicesFieldDataCache indicesFieldDataCache,
        List<IndexEventListener> builtInListeners,
        IndexingOperationListener... indexingOperationListeners
    ) throws IOException {
        final IndexSettings idxSettings = new IndexSettings(indexMetadata, settings, indexScopedSettings);
        // we ignore private settings since they are not registered settings
        indexScopedSettings.validate(indexMetadata.getSettings(), true, true, true);
        logger.debug(
            "creating Index [{}], shards [{}]/[{}] - reason [{}]",
            indexMetadata.getIndex(),
            idxSettings.getNumberOfShards(),
            idxSettings.getNumberOfReplicas(),
            indexCreationContext
        );

        final IndexModule indexModule = new IndexModule(
            idxSettings,
            analysisRegistry,
            getEngineFactory(idxSettings),
            directoryFactories,
            () -> allowExpensiveQueries,
            indexNameExpressionResolver,
            recoveryStateFactories
        );
        for (IndexingOperationListener operationListener : indexingOperationListeners) {
            indexModule.addIndexOperationListener(operationListener);
        }
        pluginsService.forEach(p -> p.onIndexModule(indexModule));
        for (IndexEventListener listener : builtInListeners) {
            indexModule.addIndexEventListener(listener);
        }
        return indexModule.newIndexService(
            indexCreationContext,
            nodeEnv,
            parserConfig,
            this,
            circuitBreakerService,
            bigArrays,
            threadPool,
            scriptService,
            clusterService,
            client,
            indicesQueryCache,
            mapperRegistry,
            indicesFieldDataCache,
            namedWriteableRegistry,
            idFieldMappers.apply(idxSettings.getMode()),
            valuesSourceRegistry,
            indexFoldersDeletionListeners,
            snapshotCommitSuppliers
        );
    }

    private EngineFactory getEngineFactory(final IndexSettings idxSettings) {
        final IndexMetadata indexMetadata = idxSettings.getIndexMetadata();
        if (indexMetadata != null && indexMetadata.getState() == IndexMetadata.State.CLOSE) {
            // NoOpEngine takes precedence as long as the index is closed
            return NoOpEngine::new;
        }

        final List<Optional<EngineFactory>> engineFactories = engineFactoryProviders.stream()
            .map(engineFactoryProvider -> engineFactoryProvider.apply(idxSettings))
            .filter(maybe -> Objects.requireNonNull(maybe).isPresent())
            .toList();
        if (engineFactories.isEmpty()) {
            return new InternalEngineFactory();
        } else if (engineFactories.size() == 1) {
            assert engineFactories.get(0).isPresent();
            return engineFactories.get(0).get();
        } else {
            final String message = String.format(
                Locale.ROOT,
                "multiple engine factories provided for %s: %s",
                idxSettings.getIndex(),
                engineFactories.stream().map(t -> {
                    assert t.isPresent();
                    return "[" + t.get().getClass().getName() + "]";
                }).collect(Collectors.joining(","))
            );
            throw new IllegalStateException(message);
        }
    }

    /**
     * creates a new mapper service for the given index, in order to do administrative work like mapping updates.
     * This *should not* be used for document parsing. Doing so will result in an exception.
     *
     * Note: the returned {@link MapperService} should be closed when unneeded.
     */
    public synchronized MapperService createIndexMapperServiceForValidation(IndexMetadata indexMetadata) throws IOException {
        final IndexSettings idxSettings = new IndexSettings(indexMetadata, this.settings, indexScopedSettings);
        final IndexModule indexModule = new IndexModule(
            idxSettings,
            analysisRegistry,
            getEngineFactory(idxSettings),
            directoryFactories,
            () -> allowExpensiveQueries,
            indexNameExpressionResolver,
            recoveryStateFactories
        );
        pluginsService.forEach(p -> p.onIndexModule(indexModule));
        return indexModule.newIndexMapperService(clusterService, parserConfig, mapperRegistry, scriptService);
    }

    /**
     * This method verifies that the given {@code metadata} holds sane values to create an {@link IndexService}.
     * This method tries to update the meta data of the created {@link IndexService} if the given {@code metadataUpdate}
     * is different from the given {@code metadata}.
     * This method will throw an exception if the creation or the update fails.
     * The created {@link IndexService} will not be registered and will be closed immediately.
     */
    public synchronized void verifyIndexMetadata(IndexMetadata metadata, IndexMetadata metadataUpdate) throws IOException {
        final List<Closeable> closeables = new ArrayList<>();
        try {
            IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(settings, new IndexFieldDataCache.Listener() {
            });
            closeables.add(indicesFieldDataCache);
            IndicesQueryCache indicesQueryCache = new IndicesQueryCache(settings);
            closeables.add(indicesQueryCache);
            // this will also fail if some plugin fails etc. which is nice since we can verify that early
            final IndexService service = createIndexService(
                METADATA_VERIFICATION,
                metadata,
                indicesQueryCache,
                indicesFieldDataCache,
                emptyList()
            );
            closeables.add(() -> service.close("metadata verification", false));
            service.mapperService().merge(metadata, MapperService.MergeReason.MAPPING_RECOVERY);
            if (metadata.equals(metadataUpdate) == false) {
                service.updateMetadata(metadata, metadataUpdate);
            }
        } finally {
            IOUtils.close(closeables);
        }
    }

    @Override
    public IndexShard createShard(
        final ShardRouting shardRouting,
        final PeerRecoveryTargetService recoveryTargetService,
        final PeerRecoveryTargetService.RecoveryListener recoveryListener,
        final RepositoriesService repositoriesService,
        final Consumer<IndexShard.ShardFailure> onShardFailure,
        final GlobalCheckpointSyncer globalCheckpointSyncer,
        final RetentionLeaseSyncer retentionLeaseSyncer,
        final DiscoveryNode targetNode,
        final DiscoveryNode sourceNode
    ) throws IOException {
        Objects.requireNonNull(retentionLeaseSyncer);
        ensureChangesAllowed();
        IndexService indexService = indexService(shardRouting.index());
        assert indexService != null;
        RecoveryState recoveryState = indexService.createRecoveryState(shardRouting, targetNode, sourceNode);
        IndexShard indexShard = indexService.createShard(shardRouting, globalCheckpointSyncer, retentionLeaseSyncer);
        indexShard.addShardFailureCallback(onShardFailure);
        indexShard.startRecovery(recoveryState, recoveryTargetService, recoveryListener, repositoriesService, (mapping, listener) -> {
            assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS
                : "mapping update consumer only required by local shards recovery";
            client.execute(
                clusterService.state().nodes().getMinNodeVersion().onOrAfter(Version.V_8_8_0)
                    ? AutoPutMappingAction.INSTANCE
                    : PutMappingAction.INSTANCE,
                new PutMappingRequest().setConcreteIndex(shardRouting.index())
                    .setConcreteIndex(shardRouting.index()) // concrete index - no name clash, it uses uuid
                    .source(mapping.source().string(), XContentType.JSON)
                    .timeout(TimeValue.MAX_VALUE)
                    .masterNodeTimeout(TimeValue.MAX_VALUE),
                new ThreadedActionListener<>(threadPool.generic(), listener.map(ignored -> null))
            );
        }, this);
        return indexShard;
    }

    @Override
    public void removeIndex(final Index index, final IndexRemovalReason reason, final String extraInfo) {
        final String indexName = index.getName();
        try {
            final IndexService indexService;
            final IndexEventListener listener;
            synchronized (this) {
                if (hasIndex(index) == false) {
                    return;
                }

                logger.debug("[{}] closing ... (reason [{}])", indexName, reason);
                indexService = indices.get(index.getUUID());
                assert indexService != null : "IndexService is null for index: " + index;
                indices = Maps.copyMapWithRemovedEntry(indices, index.getUUID());
                listener = indexService.getIndexEventListener();
            }

            listener.beforeIndexRemoved(indexService, reason);
            logger.debug("{} closing index service (reason [{}][{}])", index, reason, extraInfo);
            indexService.close(extraInfo, reason == IndexRemovalReason.DELETED);
            logger.debug("{} closed... (reason [{}][{}])", index, reason, extraInfo);
            final IndexSettings indexSettings = indexService.getIndexSettings();
            listener.afterIndexRemoved(indexService.index(), indexSettings, reason);
            if (reason == IndexRemovalReason.DELETED) {
                // now we are done - try to wipe data on disk if possible
                deleteIndexStore(extraInfo, indexService.index(), indexSettings);
            }
        } catch (Exception e) {
            logger.warn(() -> format("failed to remove index %s ([%s][%s])", index, reason, extraInfo), e);
        }
    }

    public IndicesFieldDataCache getIndicesFieldDataCache() {
        return indicesFieldDataCache;
    }

    public CircuitBreakerService getCircuitBreakerService() {
        return circuitBreakerService;
    }

    public IndicesQueryCache getIndicesQueryCache() {
        return indicesQueryCache;
    }

    static class OldShardsStats implements IndexEventListener {

        final SearchStats searchStats = new SearchStats();
        final GetStats getStats = new GetStats();
        final IndexingStats indexingStats = new IndexingStats();
        final MergeStats mergeStats = new MergeStats();
        final RefreshStats refreshStats = new RefreshStats();
        final FlushStats flushStats = new FlushStats();
        final RecoveryStats recoveryStats = new RecoveryStats();
        final BulkStats bulkStats = new BulkStats();

        @Override
        public synchronized void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
            if (indexShard != null) {
                getStats.addTotals(indexShard.getStats());
                indexingStats.addTotals(indexShard.indexingStats());
                // if this index was closed or deleted, we should eliminate the effect of the current scroll for this shard
                searchStats.addTotalsForClosingShard(indexShard.searchStats());
                mergeStats.addTotals(indexShard.mergeStats());
                refreshStats.addTotals(indexShard.refreshStats());
                flushStats.addTotals(indexShard.flushStats());
                recoveryStats.addTotals(indexShard.recoveryStats());
                bulkStats.addTotals(indexShard.bulkStats());
            }
        }

        @Override
        public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {

        }
    }

    /**
     * Deletes an index that is not assigned to this node. This method cleans up all disk folders relating to the index
     * but does not deal with in-memory structures. For those call {@link #removeIndex(Index, IndexRemovalReason, String)}
     */
    @Override
    public void deleteUnassignedIndex(String reason, IndexMetadata oldIndexMetadata, ClusterState clusterState) {
        if (nodeEnv.hasNodeFile()) {
            Index index = oldIndexMetadata.getIndex();
            try {
                if (clusterState.metadata().hasIndex(index)) {
                    final IndexMetadata currentMetadata = clusterState.metadata().index(index);
                    throw new IllegalStateException(
                        "Can't delete unassigned index store for ["
                            + index.getName()
                            + "] - it's still part "
                            + "of the cluster state ["
                            + currentMetadata.getIndexUUID()
                            + "] ["
                            + oldIndexMetadata.getIndexUUID()
                            + "]"
                    );
                }
                deleteIndexStore(reason, oldIndexMetadata);
            } catch (Exception e) {
                logger.warn(() -> format("[%s] failed to delete unassigned index (reason [%s])", oldIndexMetadata.getIndex(), reason), e);
            }
        }
    }

    /**
     * Deletes the index store trying to acquire all shards locks for this index.
     * This method will delete the metadata for the index even if the actual shards can't be locked.
     *
     * Package private for testing
     */
    void deleteIndexStore(String reason, IndexMetadata metadata) throws IOException {
        if (nodeEnv.hasNodeFile()) {
            synchronized (this) {
                Index index = metadata.getIndex();
                if (hasIndex(index)) {
                    String localUUid = indexService(index).indexUUID();
                    throw new IllegalStateException(
                        "Can't delete index store for ["
                            + index.getName()
                            + "] - it's still part of the indices service ["
                            + localUUid
                            + "] ["
                            + metadata.getIndexUUID()
                            + "]"
                    );
                }
            }
            final IndexSettings indexSettings = buildIndexSettings(metadata);
            deleteIndexStore(reason, indexSettings.getIndex(), indexSettings);
        }
    }

    private void deleteIndexStore(String reason, Index index, IndexSettings indexSettings) throws IOException {
        deleteIndexStoreIfDeletionAllowed(reason, index, indexSettings, DEFAULT_INDEX_DELETION_PREDICATE);
    }

    private void deleteIndexStoreIfDeletionAllowed(
        final String reason,
        final Index index,
        final IndexSettings indexSettings,
        final IndexDeletionAllowedPredicate predicate
    ) throws IOException {
        boolean success = false;
        try {
            // we are trying to delete the index store here - not a big deal if the lock can't be obtained
            // the store metadata gets wiped anyway even without the lock this is just best effort since
            // every shards deletes its content under the shard lock it owns.
            logger.debug("{} deleting index store reason [{}]", index, reason);
            if (predicate.apply(index, indexSettings)) {
                // its safe to delete all index metadata and shard data
                nodeEnv.deleteIndexDirectorySafe(
                    index,
                    0,
                    indexSettings,
                    paths -> indexFoldersDeletionListeners.beforeIndexFoldersDeleted(index, indexSettings, paths)
                );
            }
            success = true;
        } catch (ShardLockObtainFailedException ex) {
            logger.debug(() -> format("%s failed to delete index store - at least one shards is still locked", index), ex);
        } catch (Exception ex) {
            logger.warn(() -> format("%s failed to delete index", index), ex);
        } finally {
            if (success == false) {
                addPendingDelete(index, indexSettings);
            }
            // this is a pure protection to make sure this index doesn't get re-imported as a dangling index.
            // we should in the future rather write a tombstone rather than wiping the metadata.
            MetadataStateFormat.deleteMetaState(nodeEnv.indexPaths(index));
        }
    }

    /**
     * Deletes the shard with an already acquired shard lock.
     * @param reason the reason for the shard deletion
     * @param lock the lock of the shard to delete
     * @param indexSettings the shards index settings.
     * @throws IOException if an IOException occurs
     */
    @Override
    public void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException {
        ShardId shardId = lock.getShardId();
        logger.trace("{} deleting shard reason [{}]", shardId, reason);
        nodeEnv.deleteShardDirectoryUnderLock(
            lock,
            indexSettings,
            paths -> indexFoldersDeletionListeners.beforeShardFoldersDeleted(shardId, indexSettings, paths)
        );
    }

    /**
     * This method deletes the shard contents on disk for the given shard ID. This method will fail if the shard deleting
     * is prevented by {@link #canDeleteShardContent(ShardId, IndexSettings)}
     * of if the shards lock can not be acquired.
     *
     * On data nodes, if the deleted shard is the last shard folder in its index, the method will attempt to remove
     * the index folder as well.
     *
     * @param reason the reason for the shard deletion
     * @param shardId the shards ID to delete
     * @param clusterState . This is required to access the indexes settings etc.
     * @throws IOException if an IOException occurs
     */
    public void deleteShardStore(String reason, ShardId shardId, ClusterState clusterState) throws IOException,
        ShardLockObtainFailedException {
        final IndexMetadata metadata = clusterState.getMetadata().indices().get(shardId.getIndexName());

        final IndexSettings indexSettings = buildIndexSettings(metadata);
        ShardDeletionCheckResult shardDeletionCheckResult = canDeleteShardContent(shardId, indexSettings);
        if (shardDeletionCheckResult != ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE) {
            throw new IllegalStateException("Can't delete shard " + shardId + " (cause: " + shardDeletionCheckResult + ")");
        }
        nodeEnv.deleteShardDirectorySafe(
            shardId,
            indexSettings,
            paths -> indexFoldersDeletionListeners.beforeShardFoldersDeleted(shardId, indexSettings, paths)
        );
        logger.debug("{} deleted shard reason [{}]", shardId, reason);

        if (canDeleteIndexContents(shardId.getIndex())) {
            if (nodeEnv.findAllShardIds(shardId.getIndex()).isEmpty()) {
                try {
                    // note that deleteIndexStore have more safety checks and may throw an exception if index was concurrently created.
                    deleteIndexStore("no longer used", metadata);
                } catch (Exception e) {
                    // wrap the exception to indicate we already deleted the shard
                    throw new ElasticsearchException("failed to delete unused index after deleting its last shard (" + shardId + ")", e);
                }
            } else {
                logger.trace("[{}] still has shard stores, leaving as is", shardId.getIndex());
            }
        }
    }

    /**
     * This method returns true if the current node is allowed to delete the given index.
     * This is the case if the index is deleted in the metadata or there is no allocation
     * on the local node and the index isn't on a shared file system.
     * @param index {@code Index} to check whether deletion is allowed
     * @return true if the index can be deleted on this node
     */
    public boolean canDeleteIndexContents(Index index) {
        // index contents can be deleted if its an already closed index (so all its resources have
        // already been relinquished)
        final IndexService indexService = indexService(index);
        return indexService == null && nodeEnv.hasNodeFile();
    }

    /**
     * Verify that the contents on disk for the given index is deleted; if not, delete the contents.
     * This method assumes that an index is already deleted in the cluster state and/or explicitly
     * through index tombstones.
     * @param index {@code Index} to make sure its deleted from disk
     * @param clusterState {@code ClusterState} to ensure the index is not part of it
     * @return IndexMetadata for the index loaded from disk
     */
    @Override
    @Nullable
    public IndexMetadata verifyIndexIsDeleted(final Index index, final ClusterState clusterState) {
        // this method should only be called when we know the index (name + uuid) is not part of the cluster state
        if (clusterState.metadata().index(index) != null) {
            throw new IllegalStateException("Cannot delete index [" + index + "], it is still part of the cluster state.");
        }
        if (nodeEnv.hasNodeFile() && FileSystemUtils.exists(nodeEnv.indexPaths(index))) {
            final IndexMetadata metadata;
            try {
                metadata = metaStateService.loadIndexState(index);
                if (metadata == null) {
                    return null;
                }
            } catch (Exception e) {
                logger.warn(
                    () -> format("[%s] failed to load state file from a stale deleted index, " + "folders will be left on disk", index),
                    e
                );
                return null;
            }
            final IndexSettings indexSettings = buildIndexSettings(metadata);
            try {
                deleteIndexStoreIfDeletionAllowed("stale deleted index", index, indexSettings, ALWAYS_TRUE);
            } catch (Exception e) {
                // we just warn about the exception here because if deleteIndexStoreIfDeletionAllowed
                // throws an exception, it gets added to the list of pending deletes to be tried again
                logger.warn(() -> "[" + metadata.getIndex() + "] failed to delete index on disk", e);
            }
            return metadata;
        }
        return null;
    }

    /**
     * result type returned by {@link #canDeleteShardContent signaling different reasons why a shard can / cannot be deleted}
     */
    public enum ShardDeletionCheckResult {
        FOLDER_FOUND_CAN_DELETE, // shard data exists and can be deleted
        STILL_ALLOCATED, // the shard is still allocated / active on this node
        NO_FOLDER_FOUND // the shards data locations do not exist
    }

    /**
     * Returns <code>ShardDeletionCheckResult</code> signaling whether the shards content for the given shard can be deleted.
     *
     * @param shardId the shard to delete.
     * @param indexSettings the shards's relevant {@link IndexSettings}. This is required to access the indexes settings etc.
     */
    public ShardDeletionCheckResult canDeleteShardContent(ShardId shardId, IndexSettings indexSettings) {
        assert shardId.getIndex().equals(indexSettings.getIndex());
        final IndexService indexService = indexService(shardId.getIndex());
        final boolean isAllocated = indexService != null && indexService.hasShard(shardId.id());
        if (isAllocated) {
            return ShardDeletionCheckResult.STILL_ALLOCATED; // we are allocated - can't delete the shard
        } else if (indexSettings.hasCustomDataPath()) {
            // lets see if it's on a custom path (return false if the shard doesn't exist)
            // we don't need to delete anything that is not there
            return Files.exists(nodeEnv.resolveCustomLocation(indexSettings.customDataPath(), shardId))
                ? ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE
                : ShardDeletionCheckResult.NO_FOLDER_FOUND;
        } else {
            // lets see if it's path is available (return false if the shard doesn't exist)
            // we don't need to delete anything that is not there
            return FileSystemUtils.exists(nodeEnv.availableShardPaths(shardId))
                ? ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE
                : ShardDeletionCheckResult.NO_FOLDER_FOUND;
        }
    }

    private IndexSettings buildIndexSettings(IndexMetadata metadata) {
        // play safe here and make sure that we take node level settings into account.
        // we might run on nodes where we use shard FS and then in the future don't delete
        // actual content.
        return new IndexSettings(metadata, settings);
    }

    /**
     * Adds a pending delete for the given index shard.
     */
    @Override
    public void addPendingDelete(ShardId shardId, IndexSettings settings) {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (settings == null) {
            throw new IllegalArgumentException("settings must not be null");
        }
        PendingDelete pendingDelete = new PendingDelete(shardId, settings);
        addPendingDelete(shardId.getIndex(), pendingDelete);
    }

    /**
     * Adds a pending delete for the given index.
     */
    public void addPendingDelete(Index index, IndexSettings settings) {
        PendingDelete pendingDelete = new PendingDelete(index, settings);
        addPendingDelete(index, pendingDelete);
    }

    private void addPendingDelete(Index index, PendingDelete pendingDelete) {
        synchronized (pendingDeletes) {
            pendingDeletes.computeIfAbsent(index, k -> new ArrayList<>()).add(pendingDelete);
            numUncompletedDeletes.incrementAndGet();
        }
    }

    private static final class PendingDelete implements Comparable<PendingDelete> {
        final Index index;
        final int shardId;
        final IndexSettings settings;
        final boolean deleteIndex;

        /**
         * Creates a new pending delete of an index
         */
        PendingDelete(ShardId shardId, IndexSettings settings) {
            this.index = shardId.getIndex();
            this.shardId = shardId.getId();
            this.settings = settings;
            this.deleteIndex = false;
        }

        /**
         * Creates a new pending delete of a shard
         */
        PendingDelete(Index index, IndexSettings settings) {
            this.index = index;
            this.shardId = -1;
            this.settings = settings;
            this.deleteIndex = true;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(index).append("]");
            if (shardId != -1) {
                sb.append("[").append(shardId).append("]");
            }
            return sb.toString();
        }

        @Override
        public int compareTo(PendingDelete o) {
            return Integer.compare(shardId, o.shardId);
        }
    }

    /**
     * Processes all pending deletes for the given index. This method will acquire all locks for the given index and will
     * process all pending deletes for this index. Pending deletes might occur if the OS doesn't allow deletion of files because
     * they are used by a different process ie. on Windows where files might still be open by a virus scanner. On a shared
     * filesystem a replica might not have been closed when the primary is deleted causing problems on delete calls so we
     * schedule there deletes later.
     * @param index the index to process the pending deletes for
     * @param timeout the timeout used for processing pending deletes
     */
    @Override
    public void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeout) throws IOException, InterruptedException,
        ShardLockObtainFailedException {
        logger.debug("{} processing pending deletes", index);
        final long startTimeNS = System.nanoTime();
        final List<ShardLock> shardLocks = nodeEnv.lockAllForIndex(index, indexSettings, "process pending deletes", timeout.millis());
        int numRemoved = 0;
        try {
            Map<ShardId, ShardLock> locks = new HashMap<>();
            for (ShardLock lock : shardLocks) {
                locks.put(lock.getShardId(), lock);
            }
            final List<PendingDelete> remove;
            synchronized (pendingDeletes) {
                remove = pendingDeletes.remove(index);
            }
            if (remove != null && remove.isEmpty() == false) {
                numRemoved = remove.size();
                CollectionUtil.timSort(remove); // make sure we delete indices first
                final long maxSleepTimeMs = 10 * 1000; // ensure we retry after 10 sec
                long sleepTime = 10;
                do {
                    if (remove.isEmpty()) {
                        break;
                    }
                    Iterator<PendingDelete> iterator = remove.iterator();
                    while (iterator.hasNext()) {
                        PendingDelete delete = iterator.next();

                        if (delete.deleteIndex) {
                            assert delete.shardId == -1;
                            logger.debug("{} deleting index store reason [{}]", index, "pending delete");
                            try {
                                nodeEnv.deleteIndexDirectoryUnderLock(
                                    index,
                                    indexSettings,
                                    paths -> indexFoldersDeletionListeners.beforeIndexFoldersDeleted(index, indexSettings, paths)
                                );
                                iterator.remove();
                            } catch (IOException ex) {
                                logger.debug(() -> format("%s retry pending delete", index), ex);
                            }
                        } else {
                            assert delete.shardId != -1;
                            final ShardId shardId = new ShardId(delete.index, delete.shardId);
                            final ShardLock shardLock = locks.get(shardId);
                            if (shardLock != null) {
                                try {
                                    deleteShardStore("pending delete", shardLock, delete.settings);
                                    iterator.remove();
                                } catch (IOException ex) {
                                    logger.debug(() -> format("%s retry pending delete", shardLock.getShardId()), ex);
                                }
                            } else {
                                logger.warn("{} no shard lock for pending delete", delete.shardId);
                                iterator.remove();
                            }
                        }
                    }
                    if (remove.isEmpty() == false) {
                        logger.warn("{} still pending deletes present for shards {} - retrying", index, remove.toString());
                        Thread.sleep(sleepTime);
                        sleepTime = Math.min(maxSleepTimeMs, sleepTime * 2); // increase the sleep time gradually
                        logger.debug("{} schedule pending delete retry after {} ms", index, sleepTime);
                    }
                } while ((System.nanoTime() - startTimeNS) < timeout.nanos());
            }
        } finally {
            IOUtils.close(shardLocks);
            if (numRemoved > 0) {
                int remainingUncompletedDeletes = numUncompletedDeletes.addAndGet(-numRemoved);
                assert remainingUncompletedDeletes >= 0;
            }
        }
    }

    int numPendingDeletes(Index index) {
        synchronized (pendingDeletes) {
            List<PendingDelete> deleteList = pendingDeletes.get(index);
            if (deleteList == null) {
                return 0;
            }
            return deleteList.size();
        }
    }

    /**
     * Checks if all pending deletes have completed. Used by tests to ensure we don't check directory contents
     * while deletion still ongoing. * The reason is that, on Windows, browsing the directory contents can interfere
     * with the deletion process and delay it unnecessarily.
     */
    public boolean hasUncompletedPendingDeletes() {
        return numUncompletedDeletes.get() > 0;
    }

    public AnalysisRegistry getAnalysis() {
        return analysisRegistry;
    }

    /**
     * FieldDataCacheCleaner is a scheduled Runnable used to clean a Guava cache
     * periodically. In this case it is the field data cache, because a cache that
     * has an entry invalidated may not clean up the entry if it is not read from
     * or written to after invalidation.
     */
    private static final class CacheCleaner implements Runnable, Releasable {

        private final IndicesFieldDataCache cache;
        private final Logger logger;
        private final ThreadPool threadPool;
        private final TimeValue interval;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final IndicesRequestCache requestCache;

        CacheCleaner(
            IndicesFieldDataCache cache,
            IndicesRequestCache requestCache,
            Logger logger,
            ThreadPool threadPool,
            TimeValue interval
        ) {
            this.cache = cache;
            this.requestCache = requestCache;
            this.logger = logger;
            this.threadPool = threadPool;
            this.interval = interval;
        }

        @Override
        public void run() {
            long startTimeNS = System.nanoTime();
            if (logger.isTraceEnabled()) {
                logger.trace("running periodic field data cache cleanup");
            }
            try {
                this.cache.getCache().refresh();
            } catch (Exception e) {
                logger.warn("Exception during periodic field data cache cleanup:", e);
            }
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "periodic field data cache cleanup finished in {} milliseconds",
                    TimeValue.nsecToMSec(System.nanoTime() - startTimeNS)
                );
            }

            try {
                this.requestCache.cleanCache();
            } catch (Exception e) {
                logger.warn("Exception during periodic request cache cleanup:", e);
            }
            // Reschedule itself to run again if not closed
            if (closed.get() == false) {
                threadPool.scheduleUnlessShuttingDown(interval, ThreadPool.Names.SAME, this);
            }
        }

        @Override
        public void close() {
            closed.compareAndSet(false, true);
        }
    }

    /**
     * Can the shard request be cached at all?
     */
    public static boolean canCache(ShardSearchRequest request, SearchContext context) {
        // Queries that create a scroll context cannot use the cache.
        // They modify the search context during their execution so using the cache
        // may invalidate the scroll for the next query.
        if (request.scroll() != null) {
            return false;
        }

        // We cannot cache with DFS because results depend not only on the content of the index but also
        // on the overridden statistics. So if you ran two queries on the same index with different stats
        // (because an other shard was updated) you would get wrong results because of the scores
        // (think about top_hits aggs or scripts using the score)
        if (SearchType.QUERY_THEN_FETCH != context.searchType()) {
            return false;
        }

        // Profiled queries should not use the cache
        if (request.source() != null && request.source().profile()) {
            return false;
        }

        IndexSettings settings = context.indexShard().indexSettings();
        // if not explicitly set in the request, use the index setting, if not, use the request
        if (request.requestCache() == null) {
            if (settings.getValue(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING) == false) {
                return false;
            } else if (context.size() != 0) {
                // If no request cache query parameter and shard request cache
                // is enabled in settings don't cache for requests with size > 0
                return false;
            }
        } else if (request.requestCache() == false) {
            return false;
        }
        // We use the cacheKey of the index reader as a part of a key of the IndicesRequestCache.
        assert context.searcher().getIndexReader().getReaderCacheHelper() != null;

        // if now in millis is used (or in the future, a more generic "isDeterministic" flag
        // then we can't cache based on "now" key within the search request, as it is not deterministic
        if (context.getSearchExecutionContext().isCacheable() == false) {
            return false;
        }
        return true;

    }

    /**
     * Loads the cache result, computing it if needed by executing the query phase and otherwise deserializing the cached
     * value into the {@link SearchContext#queryResult() context's query result}. The combination of load + compute allows
     * to have a single load operation that will cause other requests with the same key to wait till its loaded an reuse
     * the same cache.
     */
    public void loadIntoContext(ShardSearchRequest request, SearchContext context) throws Exception {
        assert canCache(request, context);
        final DirectoryReader directoryReader = context.searcher().getDirectoryReader();

        boolean[] loadedFromCache = new boolean[] { true };
        BytesReference cacheKey = request.cacheKey(requestCacheKeyDifferentiator);
        BytesReference bytesReference = cacheShardLevelResult(
            context.indexShard(),
            context.getSearchExecutionContext().mappingCacheKey(),
            directoryReader,
            cacheKey,
            out -> {
                QueryPhase.execute(context);
                context.queryResult().writeToNoId(out);
                loadedFromCache[0] = false;
            }
        );

        if (loadedFromCache[0]) {
            // restore the cached query result into the context
            final QuerySearchResult result = context.queryResult();
            StreamInput in = new NamedWriteableAwareStreamInput(bytesReference.streamInput(), namedWriteableRegistry);
            result.readFromWithId(context.id(), in);
            result.setSearchShardTarget(context.shardTarget());
        } else if (context.queryResult().searchTimedOut()) {
            // we have to invalidate the cache entry if we cached a query result form a request that timed out.
            // we can't really throw exceptions in the loading part to signal a timed out search to the outside world since if there are
            // multiple requests that wait for the cache entry to be calculated they'd fail all with the same exception.
            // instead we all caching such a result for the time being, return the timed out result for all other searches with that cache
            // key invalidate the result in the thread that caused the timeout. This will end up to be simpler and eventually correct since
            // running a search that times out concurrently will likely timeout again if it's run while we have this `stale` result in the
            // cache. One other option is to not cache requests with a timeout at all...
            indicesRequestCache.invalidate(
                new IndexShardCacheEntity(context.indexShard()),
                context.getSearchExecutionContext().mappingCacheKey(),
                directoryReader,
                cacheKey
            );
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "Query timed out, invalidating cache entry for request on shard [{}]:\n {}",
                    request.shardId(),
                    request.source()
                );
            }
        }
    }

    public ByteSizeValue getTotalIndexingBufferBytes() {
        return indexingMemoryController.indexingBufferSize();
    }

    /**
     * Cache something calculated at the shard level.
     * @param shard the shard this item is part of
     * @param reader a reader for this shard. Used to invalidate the cache when there are changes.
     * @param cacheKey key for the thing being cached within this shard
     * @param loader loads the data into the cache if needed
     * @return the contents of the cache or the result of calling the loader
     */
    private BytesReference cacheShardLevelResult(
        IndexShard shard,
        MappingLookup.CacheKey mappingCacheKey,
        DirectoryReader reader,
        BytesReference cacheKey,
        CheckedConsumer<StreamOutput, IOException> loader
    ) throws Exception {
        IndexShardCacheEntity cacheEntity = new IndexShardCacheEntity(shard);
        CheckedSupplier<BytesReference, IOException> supplier = () -> {
            /* BytesStreamOutput allows to pass the expected size but by default uses
             * BigArrays.PAGE_SIZE_IN_BYTES which is 16k. A common cached result ie.
             * a date histogram with 3 buckets is ~100byte so 16k might be very wasteful
             * since we don't shrink to the actual size once we are done serializing.
             * By passing 512 as the expected size we will resize the byte array in the stream
             * slowly until we hit the page size and don't waste too much memory for small query
             * results.*/
            final int expectedSizeInBytes = 512;
            try (BytesStreamOutput out = new BytesStreamOutput(expectedSizeInBytes)) {
                loader.accept(out);
                // for now, keep the paged data structure, which might have unused bytes to fill a page, but better to keep
                // the memory properly paged instead of having varied sized bytes
                return out.bytes();
            }
        };
        return indicesRequestCache.getOrCompute(cacheEntity, supplier, mappingCacheKey, reader, cacheKey);
    }

    static final class IndexShardCacheEntity extends AbstractIndexShardCacheEntity {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IndexShardCacheEntity.class);
        private final IndexShard indexShard;

        protected IndexShardCacheEntity(IndexShard indexShard) {
            this.indexShard = indexShard;
        }

        @Override
        protected ShardRequestCache stats() {
            return indexShard.requestCache();
        }

        @Override
        public boolean isOpen() {
            return indexShard.state() != IndexShardState.CLOSED;
        }

        @Override
        public Object getCacheIdentity() {
            return indexShard;
        }

        @Override
        public long ramBytesUsed() {
            // No need to take the IndexShard into account since it is shared
            // across many entities
            return BASE_RAM_BYTES_USED;
        }
    }

    @FunctionalInterface
    interface IndexDeletionAllowedPredicate {
        boolean apply(Index index, IndexSettings indexSettings);
    }

    private final IndexDeletionAllowedPredicate DEFAULT_INDEX_DELETION_PREDICATE = (
        Index index,
        IndexSettings indexSettings) -> canDeleteIndexContents(index);
    private final IndexDeletionAllowedPredicate ALWAYS_TRUE = (Index index, IndexSettings indexSettings) -> true;

    public AliasFilter buildAliasFilter(ClusterState state, String index, Set<String> resolvedExpressions) {
        /* Being static, parseAliasFilter doesn't have access to whatever guts it needs to parse a query. Instead of passing in a bunch
         * of dependencies we pass in a function that can perform the parsing. */
        CheckedFunction<BytesReference, QueryBuilder, IOException> filterParser = bytes -> {
            try (
                InputStream inputStream = bytes.streamInput();
                XContentParser parser = XContentFactory.xContentType(inputStream).xContent().createParser(parserConfig, inputStream)
            ) {
                return parseTopLevelQuery(parser);
            }
        };
        String[] aliases = indexNameExpressionResolver.filteringAliases(state, index, resolvedExpressions);
        if (aliases == null) {
            return AliasFilter.EMPTY;
        }

        Metadata metadata = state.metadata();
        IndexAbstraction ia = state.metadata().getIndicesLookup().get(index);
        DataStream dataStream = ia.getParentDataStream();
        if (dataStream != null) {
            String dataStreamName = dataStream.getName();
            List<QueryBuilder> filters = Arrays.stream(aliases)
                .map(name -> metadata.dataStreamAliases().get(name))
                .filter(dataStreamAlias -> dataStreamAlias.getFilter(dataStreamName) != null)
                .map(dataStreamAlias -> {
                    try {
                        return filterParser.apply(dataStreamAlias.getFilter(dataStreamName).uncompressed());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .toList();
            if (filters.isEmpty()) {
                return AliasFilter.of(null, aliases);
            } else {
                if (filters.size() == 1) {
                    return AliasFilter.of(filters.get(0), aliases);
                } else {
                    BoolQueryBuilder bool = new BoolQueryBuilder();
                    for (QueryBuilder filter : filters) {
                        bool.should(filter);
                    }
                    return AliasFilter.of(bool, aliases);
                }
            }
        } else {
            IndexMetadata indexMetadata = metadata.index(index);
            return AliasFilter.of(ShardSearchRequest.parseAliasFilter(filterParser, indexMetadata, aliases), aliases);
        }
    }

    /**
     * Returns a new {@link QueryRewriteContext} with the given {@code now} provider
     */
    public QueryRewriteContext getRewriteContext(LongSupplier nowInMillis) {
        return new QueryRewriteContext(parserConfig, client, nowInMillis);
    }

    public DataRewriteContext getDataRewriteContext(LongSupplier nowInMillis) {
        return new DataRewriteContext(parserConfig, client, nowInMillis);
    }

    public CoordinatorRewriteContextProvider getCoordinatorRewriteContextProvider(LongSupplier nowInMillis) {
        return new CoordinatorRewriteContextProvider(parserConfig, client, nowInMillis, clusterService::state, this::getTimestampFieldType);
    }

    /**
     * Clears the caches for the given shard id if the shard is still allocated on this node
     */
    public void clearIndexShardCache(ShardId shardId, boolean queryCache, boolean fieldDataCache, boolean requestCache, String... fields) {
        final IndexService service = indexService(shardId.getIndex());
        if (service != null) {
            IndexShard shard = service.getShardOrNull(shardId.id());
            final boolean clearedAtLeastOne = service.clearCaches(queryCache, fieldDataCache, fields);
            if ((requestCache || (clearedAtLeastOne == false && fields.length == 0)) && shard != null) {
                indicesRequestCache.clear(new IndexShardCacheEntity(shard));
            }
        }
    }

    /**
     * Returns a function which given an index name, returns a predicate which fields must match in order to be returned by get mappings,
     * get index, get field mappings and field capabilities API. Useful to filter the fields that such API return.
     * The predicate receives the field name as input argument. In case multiple plugins register a field filter through
     * {@link org.elasticsearch.plugins.MapperPlugin#getFieldFilter()}, only fields that match all the registered filters will be
     * returned by get mappings, get index, get field mappings and field capabilities API.
     */
    public Function<String, Predicate<String>> getFieldFilter() {
        return mapperRegistry.getFieldFilter();
    }

    private void setIdFieldDataEnabled(boolean value) {
        this.idFieldDataEnabled = value;
    }

    private void updateDanglingIndicesInfo(Index index) {
        assert DiscoveryNode.canContainData(settings) : "dangling indices information should only be persisted on data nodes";
        assert nodeWriteDanglingIndicesInfo : "writing dangling indices info is not enabled";
        assert danglingIndicesThreadPoolExecutor != null : "executor for dangling indices info is not available";
        if (danglingIndicesToWrite.add(index)) {
            logger.trace("triggered dangling indices update for {}", index);
            final long triggeredTimeMillis = threadPool.relativeTimeInMillis();
            try {
                danglingIndicesThreadPoolExecutor.execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(() -> format("failed to write dangling indices state for index %s", index), e);
                    }

                    @Override
                    protected void doRun() {
                        final boolean exists = danglingIndicesToWrite.remove(index);
                        assert exists : "removed non-existing item for " + index;
                        final IndexService indexService = indices.get(index.getUUID());
                        if (indexService != null) {
                            final long executedTimeMillis = threadPool.relativeTimeInMillis();
                            logger.trace(
                                "writing out dangling indices state for index {}, triggered {} ago",
                                index,
                                TimeValue.timeValueMillis(Math.min(0L, executedTimeMillis - triggeredTimeMillis))
                            );
                            indexService.writeDanglingIndicesInfo();
                            final long completedTimeMillis = threadPool.relativeTimeInMillis();
                            logger.trace(
                                "writing out of dangling indices state for index {} completed after {}",
                                index,
                                TimeValue.timeValueMillis(Math.min(0L, completedTimeMillis - executedTimeMillis))
                            );
                        } else {
                            logger.trace("omit writing dangling indices state for index {} as index is deallocated on this node", index);
                        }
                    }
                });
            } catch (EsRejectedExecutionException e) {
                // ignore cases where we are shutting down..., there is really nothing interesting to be done here...
                assert danglingIndicesThreadPoolExecutor.isShutdown();
            }
        } else {
            logger.trace("dangling indices update already pending for {}", index);
        }
    }

    private void setAllowExpensiveQueries(Boolean allowExpensiveQueries) {
        this.allowExpensiveQueries = allowExpensiveQueries;
    }

    // visible for testing
    public boolean allPendingDanglingIndicesWritten() {
        return nodeWriteDanglingIndicesInfo == false
            || (danglingIndicesToWrite.isEmpty() && danglingIndicesThreadPoolExecutor.getActiveCount() == 0);
    }

    /**
     * @return the field type of the {@code @timestamp} field of the given index, or {@code null} if:
     * - the index is not found,
     * - the field is not found, or
     * - the field is not a timestamp field.
     */
    @Nullable
    public DateFieldMapper.DateFieldType getTimestampFieldType(Index index) {
        return timestampFieldMapperService.getTimestampFieldType(index);
    }

    public IndexScopedSettings getIndexScopedSettings() {
        return indexScopedSettings;
    }
}
