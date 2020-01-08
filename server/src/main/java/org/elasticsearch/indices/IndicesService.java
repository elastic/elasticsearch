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

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader.CacheHelper;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.SeqNoStats;
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
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.util.CollectionUtils.arrayAsArrayList;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.index.IndexService.IndexCreationContext.CREATE_INDEX;
import static org.elasticsearch.index.IndexService.IndexCreationContext.META_DATA_VERIFICATION;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class IndicesService extends AbstractLifecycleComponent
    implements IndicesClusterStateService.AllocatedIndices<IndexShard, IndexService>, IndexService.ShardStoreDeleter {
    private static final Logger logger = LogManager.getLogger(IndicesService.class);

    public static final String INDICES_SHARDS_CLOSED_TIMEOUT = "indices.shards_closed_timeout";
    public static final Setting<TimeValue> INDICES_CACHE_CLEAN_INTERVAL_SETTING =
        Setting.positiveTimeSetting("indices.cache.cleanup_interval", TimeValue.timeValueMinutes(1), Property.NodeScope);
    public static final Setting<Boolean> INDICES_ID_FIELD_DATA_ENABLED_SETTING =
        Setting.boolSetting("indices.id_field_data.enabled", false, Property.Dynamic, Property.NodeScope);

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
    private final NamedXContentRegistry xContentRegistry;
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
    private volatile Map<String, IndexService> indices = emptyMap();
    private final Map<Index, List<PendingDelete>> pendingDeletes = new HashMap<>();
    private final AtomicInteger numUncompletedDeletes = new AtomicInteger();
    private final OldShardsStats oldShardsStats = new OldShardsStats();
    private final MapperRegistry mapperRegistry;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final IndexingMemoryController indexingMemoryController;
    private final TimeValue cleanInterval;
    final IndicesRequestCache indicesRequestCache; // pkg-private for testing
    private final IndicesQueryCache indicesQueryCache;
    private final MetaStateService metaStateService;
    private final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders;
    private final Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories;
    final AbstractRefCounted indicesRefCount; // pkg-private for testing
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    private volatile boolean idFieldDataEnabled;

    @Nullable
    private final EsThreadPoolExecutor danglingIndicesThreadPoolExecutor;
    private final Set<Index> danglingIndicesToWrite = Sets.newConcurrentHashSet();
    private final boolean nodeWriteDanglingIndicesInfo;


    @Override
    protected void doStart() {
        // Start thread that will manage cleaning the field data cache periodically
        threadPool.schedule(this.cacheCleaner, this.cleanInterval, ThreadPool.Names.SAME);
    }

    public IndicesService(Settings settings, PluginsService pluginsService, NodeEnvironment nodeEnv, NamedXContentRegistry xContentRegistry,
                          AnalysisRegistry analysisRegistry, IndexNameExpressionResolver indexNameExpressionResolver,
                          MapperRegistry mapperRegistry, NamedWriteableRegistry namedWriteableRegistry, ThreadPool threadPool,
                          IndexScopedSettings indexScopedSettings, CircuitBreakerService circuitBreakerService, BigArrays bigArrays,
                          ScriptService scriptService, ClusterService clusterService, Client client, MetaStateService metaStateService,
                          Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders,
                          Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.pluginsService = pluginsService;
        this.nodeEnv = nodeEnv;
        this.xContentRegistry = xContentRegistry;
        this.shardsClosedTimeout = settings.getAsTime(INDICES_SHARDS_CLOSED_TIMEOUT, new TimeValue(1, TimeUnit.DAYS));
        this.analysisRegistry = analysisRegistry;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indicesRequestCache = new IndicesRequestCache(settings);
        this.indicesQueryCache = new IndicesQueryCache(settings);
        this.mapperRegistry = mapperRegistry;
        this.namedWriteableRegistry = namedWriteableRegistry;
        indexingMemoryController = new IndexingMemoryController(settings, threadPool,
                                                                // ensure we pull an iter with new shards - flatten makes a copy
                                                                () -> Iterables.flatten(this).iterator());
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
                assert sizeInBytes >= 0 : "When reducing circuit breaker, it should be adjusted with a number higher or " +
                    "equal to 0 and not [" + sizeInBytes + "]";
                circuitBreakerService.getBreaker(CircuitBreaker.FIELDDATA).addWithoutBreaking(-sizeInBytes);
            }
        });
        this.cleanInterval = INDICES_CACHE_CLEAN_INTERVAL_SETTING.get(settings);
        this.cacheCleaner = new CacheCleaner(indicesFieldDataCache, indicesRequestCache,  logger, threadPool, this.cleanInterval);
        this.metaStateService = metaStateService;
        this.engineFactoryProviders = engineFactoryProviders;

        // do not allow any plugin-provided index store type to conflict with a built-in type
        for (final String indexStoreType : directoryFactories.keySet()) {
            if (IndexModule.isBuiltinType(indexStoreType)) {
                throw new IllegalStateException("registered index store type [" + indexStoreType + "] conflicts with a built-in type");
            }
        }

        this.directoryFactories = directoryFactories;
        // doClose() is called when shutting down a node, yet there might still be ongoing requests
        // that we need to wait for before closing some resources such as the caches. In order to
        // avoid closing these resources while ongoing requests are still being processed, we use a
        // ref count which will only close them when both this service and all index services are
        // actually closed
        indicesRefCount = new AbstractRefCounted("indices") {
            @Override
            protected void closeInternal() {
                try {
                    IOUtils.close(
                            analysisRegistry,
                            indexingMemoryController,
                            indicesFieldDataCache,
                            cacheCleaner,
                            indicesRequestCache,
                            indicesQueryCache);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    closeLatch.countDown();
                }
            }
        };

        final String nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(settings));
        nodeWriteDanglingIndicesInfo = WRITE_DANGLING_INDICES_INFO_SETTING.get(settings);
        danglingIndicesThreadPoolExecutor = nodeWriteDanglingIndicesInfo ? EsExecutors.newScaling(
            nodeName + "/" + DANGLING_INDICES_UPDATE_THREAD_NAME,
            1, 1,
            0, TimeUnit.MILLISECONDS,
            daemonThreadFactory(nodeName, DANGLING_INDICES_UPDATE_THREAD_NAME),
            threadPool.getThreadContext()) : null;
    }

    private static final String DANGLING_INDICES_UPDATE_THREAD_NAME = "DanglingIndices#updateTask";

    @Override
    protected void doStop() {
        ThreadPool.terminate(danglingIndicesThreadPoolExecutor, 10, TimeUnit.SECONDS);

        ExecutorService indicesStopExecutor =
            Executors.newFixedThreadPool(5, daemonThreadFactory(settings, "indices_shutdown"));

        // Copy indices because we modify it asynchronously in the body of the loop
        final Set<Index> indices = this.indices.values().stream().map(s -> s.index()).collect(Collectors.toSet());
        final CountDownLatch latch = new CountDownLatch(indices.size());
        for (final Index index : indices) {
            indicesStopExecutor.execute(() -> {
                try {
                    removeIndex(index, IndexRemovalReason.NO_LONGER_ASSIGNED, "shutdown");
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
                case Get:
                    commonStats.get.add(oldShardsStats.getStats);
                    break;
                case Indexing:
                    commonStats.indexing.add(oldShardsStats.indexingStats);
                    break;
                case Search:
                    commonStats.search.add(oldShardsStats.searchStats);
                    break;
                case Merge:
                    commonStats.merge.add(oldShardsStats.mergeStats);
                    break;
                case Refresh:
                    commonStats.refresh.add(oldShardsStats.refreshStats);
                    break;
                case Recovery:
                    commonStats.recoveryStats.add(oldShardsStats.recoveryStats);
                    break;
                case Flush:
                    commonStats.flush.add(oldShardsStats.flushStats);
                    break;
            }
        }

        return new NodeIndicesStats(commonStats, statsByShard(this, flags));
    }

    Map<Index, List<IndexShardStats>> statsByShard(final IndicesService indicesService, final CommonStatsFlags flags) {
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
                    logger.trace(() -> new ParameterizedMessage("{} ignoring shard stats", indexShard.shardId()), e);
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
                new ShardStats[]{
                        new ShardStats(
                                indexShard.routingEntry(),
                                indexShard.shardPath(),
                                new CommonStats(indicesService.getIndicesQueryCache(), indexShard, flags),
                                commitStats,
                                seqNoStats,
                                retentionLeaseStats)
                });
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
        assert indexService.indexUUID().equals(index.getUUID()) : "uuid mismatch local: " + indexService.indexUUID() +
            " incoming: " + index.getUUID();
        return indexService;
    }

    /**
     * Creates a new {@link IndexService} for the given metadata.
     *
     * @param indexMetaData          the index metadata to create the index for
     * @param builtInListeners       a list of built-in lifecycle {@link IndexEventListener} that should should be used along side with the
     *                               per-index listeners
     * @throws ResourceAlreadyExistsException if the index already exists.
     */
    @Override
    public synchronized IndexService createIndex(
            final IndexMetaData indexMetaData, final List<IndexEventListener> builtInListeners,
            final boolean writeDanglingIndices) throws IOException {
        ensureChangesAllowed();
        if (indexMetaData.getIndexUUID().equals(IndexMetaData.INDEX_UUID_NA_VALUE)) {
            throw new IllegalArgumentException("index must have a real UUID found value: [" + indexMetaData.getIndexUUID() + "]");
        }
        final Index index = indexMetaData.getIndex();
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
        finalListeners.add(onStoreClose);
        finalListeners.add(oldShardsStats);
        final IndexService indexService =
                createIndexService(
                        CREATE_INDEX,
                        indexMetaData,
                        indicesQueryCache,
                        indicesFieldDataCache,
                        finalListeners,
                        indexingMemoryController);
        boolean success = false;
        try {
            if (writeDanglingIndices && nodeWriteDanglingIndicesInfo) {
                indexService.addMetaDataListener(imd -> updateDanglingIndicesInfo(index));
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

    /**
     * This creates a new IndexService without registering it
     */
    private synchronized IndexService createIndexService(IndexService.IndexCreationContext indexCreationContext,
                                                         IndexMetaData indexMetaData,
                                                         IndicesQueryCache indicesQueryCache,
                                                         IndicesFieldDataCache indicesFieldDataCache,
                                                         List<IndexEventListener> builtInListeners,
                                                         IndexingOperationListener... indexingOperationListeners) throws IOException {
        final IndexSettings idxSettings = new IndexSettings(indexMetaData, settings, indexScopedSettings);
        // we ignore private settings since they are not registered settings
        indexScopedSettings.validate(indexMetaData.getSettings(), true, true, true);
        logger.debug("creating Index [{}], shards [{}]/[{}] - reason [{}]",
            indexMetaData.getIndex(),
            idxSettings.getNumberOfShards(),
            idxSettings.getNumberOfReplicas(),
            indexCreationContext);

        final IndexModule indexModule = new IndexModule(idxSettings, analysisRegistry, getEngineFactory(idxSettings), directoryFactories);
        for (IndexingOperationListener operationListener : indexingOperationListeners) {
            indexModule.addIndexOperationListener(operationListener);
        }
        pluginsService.onIndexModule(indexModule);
        for (IndexEventListener listener : builtInListeners) {
            indexModule.addIndexEventListener(listener);
        }
        return indexModule.newIndexService(
                indexCreationContext,
                nodeEnv,
                xContentRegistry,
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
                this::isIdFieldDataEnabled
        );
    }

    private EngineFactory getEngineFactory(final IndexSettings idxSettings) {
        final IndexMetaData indexMetaData = idxSettings.getIndexMetaData();
        if (indexMetaData != null && indexMetaData.getState() == IndexMetaData.State.CLOSE) {
            // NoOpEngine takes precedence as long as the index is closed
            return NoOpEngine::new;
        }

        final List<Optional<EngineFactory>> engineFactories =
                engineFactoryProviders
                        .stream()
                        .map(engineFactoryProvider -> engineFactoryProvider.apply(idxSettings))
                        .filter(maybe -> Objects.requireNonNull(maybe).isPresent())
                        .collect(Collectors.toList());
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
                    engineFactories
                            .stream()
                            .map(t -> {
                                assert t.isPresent();
                                return "[" + t.get().getClass().getName() + "]";
                            })
                            .collect(Collectors.joining(",")));
            throw new IllegalStateException(message);
        }
    }

    /**
     * creates a new mapper service for the given index, in order to do administrative work like mapping updates.
     * This *should not* be used for document parsing. Doing so will result in an exception.
     *
     * Note: the returned {@link MapperService} should be closed when unneeded.
     */
    public synchronized MapperService createIndexMapperService(IndexMetaData indexMetaData) throws IOException {
        final IndexSettings idxSettings = new IndexSettings(indexMetaData, this.settings, indexScopedSettings);
        final IndexModule indexModule = new IndexModule(idxSettings, analysisRegistry, getEngineFactory(idxSettings), directoryFactories);
        pluginsService.onIndexModule(indexModule);
        return indexModule.newIndexMapperService(xContentRegistry, mapperRegistry, scriptService);
    }

    /**
     * This method verifies that the given {@code metaData} holds sane values to create an {@link IndexService}.
     * This method tries to update the meta data of the created {@link IndexService} if the given {@code metaDataUpdate}
     * is different from the given {@code metaData}.
     * This method will throw an exception if the creation or the update fails.
     * The created {@link IndexService} will not be registered and will be closed immediately.
     */
    public synchronized void verifyIndexMetadata(IndexMetaData metaData, IndexMetaData metaDataUpdate) throws IOException {
        final List<Closeable> closeables = new ArrayList<>();
        try {
            IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(settings, new IndexFieldDataCache.Listener() {});
            closeables.add(indicesFieldDataCache);
            IndicesQueryCache indicesQueryCache = new IndicesQueryCache(settings);
            closeables.add(indicesQueryCache);
            // this will also fail if some plugin fails etc. which is nice since we can verify that early
            final IndexService service =
                createIndexService(META_DATA_VERIFICATION, metaData, indicesQueryCache, indicesFieldDataCache, emptyList());
            closeables.add(() -> service.close("metadata verification", false));
            service.mapperService().merge(metaData, MapperService.MergeReason.MAPPING_RECOVERY);
            if (metaData.equals(metaDataUpdate) == false) {
                service.updateMetaData(metaData, metaDataUpdate);
            }
        } finally {
            IOUtils.close(closeables);
        }
    }

    @Override
    public IndexShard createShard(
            final ShardRouting shardRouting,
            final RecoveryState recoveryState,
            final PeerRecoveryTargetService recoveryTargetService,
            final PeerRecoveryTargetService.RecoveryListener recoveryListener,
            final RepositoriesService repositoriesService,
            final Consumer<IndexShard.ShardFailure> onShardFailure,
            final Consumer<ShardId> globalCheckpointSyncer,
            final RetentionLeaseSyncer retentionLeaseSyncer) throws IOException {
        Objects.requireNonNull(retentionLeaseSyncer);
        ensureChangesAllowed();
        IndexService indexService = indexService(shardRouting.index());
        IndexShard indexShard = indexService.createShard(shardRouting, globalCheckpointSyncer, retentionLeaseSyncer);
        indexShard.addShardFailureCallback(onShardFailure);
        indexShard.startRecovery(recoveryState, recoveryTargetService, recoveryListener, repositoriesService,
            mapping -> {
                assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS:
                    "mapping update consumer only required by local shards recovery";
                client.admin().indices().preparePutMapping()
                    .setConcreteIndex(shardRouting.index()) // concrete index - no name clash, it uses uuid
                    .setSource(mapping.source().string(), XContentType.JSON)
                    .get();
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
                Map<String, IndexService> newIndices = new HashMap<>(indices);
                indexService = newIndices.remove(index.getUUID());
                assert indexService != null : "IndexService is null for index: " + index;
                indices = unmodifiableMap(newIndices);
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
            logger.warn(() -> new ParameterizedMessage("failed to remove index {} ([{}][{}])", index, reason, extraInfo), e);
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
    public void deleteUnassignedIndex(String reason, IndexMetaData metaData, ClusterState clusterState) {
        if (nodeEnv.hasNodeFile()) {
            String indexName = metaData.getIndex().getName();
            try {
                if (clusterState.metaData().hasIndex(indexName)) {
                    final IndexMetaData index = clusterState.metaData().index(indexName);
                    throw new IllegalStateException("Can't delete unassigned index store for [" + indexName + "] - it's still part of " +
                                                    "the cluster state [" + index.getIndexUUID() + "] [" + metaData.getIndexUUID() + "]");
                }
                deleteIndexStore(reason, metaData);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to delete unassigned index (reason [{}])",
                    metaData.getIndex(), reason), e);
            }
        }
    }

    /**
     * Deletes the index store trying to acquire all shards locks for this index.
     * This method will delete the metadata for the index even if the actual shards can't be locked.
     *
     * Package private for testing
     */
    void deleteIndexStore(String reason, IndexMetaData metaData) throws IOException {
        if (nodeEnv.hasNodeFile()) {
            synchronized (this) {
                Index index = metaData.getIndex();
                if (hasIndex(index)) {
                    String localUUid = indexService(index).indexUUID();
                    throw new IllegalStateException("Can't delete index store for [" + index.getName() +
                        "] - it's still part of the indices service [" + localUUid + "] [" + metaData.getIndexUUID() + "]");
                }
            }
            final IndexSettings indexSettings = buildIndexSettings(metaData);
            deleteIndexStore(reason, indexSettings.getIndex(), indexSettings);
        }
    }

    private void deleteIndexStore(String reason, Index index, IndexSettings indexSettings) throws IOException {
       deleteIndexStoreIfDeletionAllowed(reason, index, indexSettings, DEFAULT_INDEX_DELETION_PREDICATE);
    }

    private void deleteIndexStoreIfDeletionAllowed(final String reason, final Index index, final IndexSettings indexSettings,
                                                   final IndexDeletionAllowedPredicate predicate) throws IOException {
        boolean success = false;
        try {
            // we are trying to delete the index store here - not a big deal if the lock can't be obtained
            // the store metadata gets wiped anyway even without the lock this is just best effort since
            // every shards deletes its content under the shard lock it owns.
            logger.debug("{} deleting index store reason [{}]", index, reason);
            if (predicate.apply(index, indexSettings)) {
                // its safe to delete all index metadata and shard data
                nodeEnv.deleteIndexDirectorySafe(index, 0, indexSettings);
            }
            success = true;
        } catch (ShardLockObtainFailedException ex) {
            logger.debug(() -> new ParameterizedMessage("{} failed to delete index store - at least one shards is still locked", index),
                ex);
        } catch (Exception ex) {
            logger.warn(() -> new ParameterizedMessage("{} failed to delete index", index), ex);
        } finally {
            if (success == false) {
                addPendingDelete(index, indexSettings);
            }
            // this is a pure protection to make sure this index doesn't get re-imported as a dangling index.
            // we should in the future rather write a tombstone rather than wiping the metadata.
            MetaDataStateFormat.deleteMetaState(nodeEnv.indexPaths(index));
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
        nodeEnv.deleteShardDirectoryUnderLock(lock, indexSettings);
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
    public void deleteShardStore(String reason, ShardId shardId, ClusterState clusterState)
            throws IOException, ShardLockObtainFailedException {
        final IndexMetaData metaData = clusterState.getMetaData().indices().get(shardId.getIndexName());

        final IndexSettings indexSettings = buildIndexSettings(metaData);
        ShardDeletionCheckResult shardDeletionCheckResult = canDeleteShardContent(shardId, indexSettings);
        if (shardDeletionCheckResult != ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE) {
            throw new IllegalStateException("Can't delete shard " + shardId + " (cause: " + shardDeletionCheckResult + ")");
        }
        nodeEnv.deleteShardDirectorySafe(shardId, indexSettings);
        logger.debug("{} deleted shard reason [{}]", shardId, reason);

        if (canDeleteIndexContents(shardId.getIndex(), indexSettings)) {
            if (nodeEnv.findAllShardIds(shardId.getIndex()).isEmpty()) {
                try {
                    // note that deleteIndexStore have more safety checks and may throw an exception if index was concurrently created.
                    deleteIndexStore("no longer used", metaData);
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
     * @param indexSettings {@code IndexSettings} for the given index
     * @return true if the index can be deleted on this node
     */
    public boolean canDeleteIndexContents(Index index, IndexSettings indexSettings) {
        // index contents can be deleted if its an already closed index (so all its resources have
        // already been relinquished)
        final IndexService indexService = indexService(index);
        if (indexService == null && nodeEnv.hasNodeFile()) {
            return true;
        }
        return false;
    }

    /**
     * Verify that the contents on disk for the given index is deleted; if not, delete the contents.
     * This method assumes that an index is already deleted in the cluster state and/or explicitly
     * through index tombstones.
     * @param index {@code Index} to make sure its deleted from disk
     * @param clusterState {@code ClusterState} to ensure the index is not part of it
     * @return IndexMetaData for the index loaded from disk
     */
    @Override
    @Nullable
    public IndexMetaData verifyIndexIsDeleted(final Index index, final ClusterState clusterState) {
        // this method should only be called when we know the index (name + uuid) is not part of the cluster state
        if (clusterState.metaData().index(index) != null) {
            throw new IllegalStateException("Cannot delete index [" + index + "], it is still part of the cluster state.");
        }
        if (nodeEnv.hasNodeFile() && FileSystemUtils.exists(nodeEnv.indexPaths(index))) {
            final IndexMetaData metaData;
            try {
                metaData = metaStateService.loadIndexState(index);
                if (metaData == null) {
                    return null;
                }
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to load state file from a stale deleted index, " +
                    "folders will be left on disk", index), e);
                return null;
            }
            final IndexSettings indexSettings = buildIndexSettings(metaData);
            try {
                deleteIndexStoreIfDeletionAllowed("stale deleted index", index, indexSettings, ALWAYS_TRUE);
            } catch (Exception e) {
                // we just warn about the exception here because if deleteIndexStoreIfDeletionAllowed
                // throws an exception, it gets added to the list of pending deletes to be tried again
                logger.warn(() -> new ParameterizedMessage("[{}] failed to delete index on disk", metaData.getIndex()), e);
            }
            return metaData;
        }
        return null;
    }

    /**
     * result type returned by {@link #canDeleteShardContent signaling different reasons why a shard can / cannot be deleted}
     */
    public enum ShardDeletionCheckResult {
        FOLDER_FOUND_CAN_DELETE, // shard data exists and can be deleted
        STILL_ALLOCATED, // the shard is still allocated / active on this node
        NO_FOLDER_FOUND, // the shards data locations do not exist
        NO_LOCAL_STORAGE // node does not have local storage (see DiscoveryNode.nodeRequiresLocalStorage)
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
        if (nodeEnv.hasNodeFile()) {
            final boolean isAllocated = indexService != null && indexService.hasShard(shardId.id());
            if (isAllocated) {
                return ShardDeletionCheckResult.STILL_ALLOCATED; // we are allocated - can't delete the shard
            } else if (indexSettings.hasCustomDataPath()) {
                // lets see if it's on a custom path (return false if the shared doesn't exist)
                // we don't need to delete anything that is not there
                return Files.exists(nodeEnv.resolveCustomLocation(indexSettings.customDataPath(), shardId)) ?
                        ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE :
                        ShardDeletionCheckResult.NO_FOLDER_FOUND;
            } else {
                // lets see if it's path is available (return false if the shared doesn't exist)
                // we don't need to delete anything that is not there
                return FileSystemUtils.exists(nodeEnv.availableShardPaths(shardId)) ?
                        ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE :
                        ShardDeletionCheckResult.NO_FOLDER_FOUND;
            }
        } else {
            return ShardDeletionCheckResult.NO_LOCAL_STORAGE;
        }
    }

    private IndexSettings buildIndexSettings(IndexMetaData metaData) {
        // play safe here and make sure that we take node level settings into account.
        // we might run on nodes where we use shard FS and then in the future don't delete
        // actual content.
        return new IndexSettings(metaData, settings);
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
            List<PendingDelete> list = pendingDeletes.get(index);
            if (list == null) {
                list = new ArrayList<>();
                pendingDeletes.put(index, list);
            }
            list.add(pendingDelete);
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
    public void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeout)
            throws IOException, InterruptedException, ShardLockObtainFailedException {
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
                                nodeEnv.deleteIndexDirectoryUnderLock(index, indexSettings);
                                iterator.remove();
                            } catch (IOException ex) {
                                logger.debug(() -> new ParameterizedMessage("{} retry pending delete", index), ex);
                            }
                        } else {
                            assert delete.shardId != -1;
                            ShardLock shardLock = locks.get(new ShardId(delete.index, delete.shardId));
                            if (shardLock != null) {
                                try {
                                    deleteShardStore("pending delete", shardLock, delete.settings);
                                    iterator.remove();
                                } catch (IOException ex) {
                                    logger.debug(() -> new ParameterizedMessage("{} retry pending delete", shardLock.getShardId()), ex);
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

        CacheCleaner(IndicesFieldDataCache cache,
                     IndicesRequestCache requestCache,
                     Logger logger,
                     ThreadPool threadPool,
                     TimeValue interval) {
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
                logger.trace("periodic field data cache cleanup finished in {} milliseconds",
                    TimeValue.nsecToMSec(System.nanoTime() - startTimeNS));
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
    public boolean canCache(ShardSearchRequest request, SearchContext context) {
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
        if (context.getQueryShardContext().isCacheable() == false) {
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
    public void loadIntoContext(ShardSearchRequest request, SearchContext context, QueryPhase queryPhase) throws Exception {
        assert canCache(request, context);
        final DirectoryReader directoryReader = context.searcher().getDirectoryReader();

        boolean[] loadedFromCache = new boolean[] { true };
        BytesReference bytesReference = cacheShardLevelResult(context.indexShard(), directoryReader, request.cacheKey(),
            () -> "Shard: " + request.shardId() + "\nSource:\n" + request.source(),
            out -> {
            queryPhase.execute(context);
            try {
                context.queryResult().writeToNoId(out);

            } catch (IOException e) {
                throw new AssertionError("Could not serialize response", e);
            }
            loadedFromCache[0] = false;
        });

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
            indicesRequestCache.invalidate(new IndexShardCacheEntity(context.indexShard()), directoryReader, request.cacheKey());
            if (logger.isTraceEnabled()) {
                logger.trace("Query timed out, invalidating cache entry for request on shard [{}]:\n {}", request.shardId(),
                        request.source());
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
    private BytesReference cacheShardLevelResult(IndexShard shard, DirectoryReader reader, BytesReference cacheKey,
            Supplier<String> cacheKeyRenderer, Consumer<StreamOutput> loader) throws Exception {
        IndexShardCacheEntity cacheEntity = new IndexShardCacheEntity(shard);
        Supplier<BytesReference> supplier = () -> {
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
        return indicesRequestCache.getOrCompute(cacheEntity, supplier, reader, cacheKey, cacheKeyRenderer);
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

    private final IndexDeletionAllowedPredicate DEFAULT_INDEX_DELETION_PREDICATE =
        (Index index, IndexSettings indexSettings) -> canDeleteIndexContents(index, indexSettings);
    private final IndexDeletionAllowedPredicate ALWAYS_TRUE = (Index index, IndexSettings indexSettings) -> true;

    public AliasFilter buildAliasFilter(ClusterState state, String index, Set<String> resolvedExpressions) {
        /* Being static, parseAliasFilter doesn't have access to whatever guts it needs to parse a query. Instead of passing in a bunch
         * of dependencies we pass in a function that can perform the parsing. */
        CheckedFunction<byte[], QueryBuilder, IOException> filterParser = bytes -> {
            try (XContentParser parser = XContentFactory.xContent(bytes)
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, bytes)) {
                return parseInnerQueryBuilder(parser);
            }
        };
        IndexMetaData indexMetaData = state.metaData().index(index);
        String[] aliases = indexNameExpressionResolver.filteringAliases(state, index, resolvedExpressions);
        return new AliasFilter(ShardSearchRequest.parseAliasFilter(filterParser, indexMetaData, aliases), aliases);
    }

    /**
     * Returns a new {@link QueryRewriteContext} with the given {@code now} provider
     */
    public QueryRewriteContext getRewriteContext(LongSupplier nowInMillis) {
        return new QueryRewriteContext(xContentRegistry, namedWriteableRegistry, client, nowInMillis);
    }

    /**
     * Clears the caches for the given shard id if the shard is still allocated on this node
     */
    public void clearIndexShardCache(ShardId shardId, boolean queryCache, boolean fieldDataCache, boolean requestCache,
                                     String...fields) {
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

    /**
     * Returns true if the provided field is a registered metadata field (including ones registered via plugins), false otherwise.
     */
    public boolean isMetaDataField(Version indexCreatedVersion, String field) {
        return mapperRegistry.isMetaDataField(indexCreatedVersion, field);
    }

    /**
     * Returns <code>true</code> if fielddata is enabled for the {@link IdFieldMapper} field, <code>false</code> otherwise.
     */
    public boolean isIdFieldDataEnabled() {
        return idFieldDataEnabled;
    }

    private void setIdFieldDataEnabled(boolean value) {
        this.idFieldDataEnabled = value;
    }

    /**
     * Checks to see if an operation can be performed without taking the cluster over the cluster-wide shard limit. Adds a deprecation
     * warning or returns an error message as appropriate
     *
     * @param newShards         The number of shards to be added by this operation
     * @param state             The current cluster state
     * @return If present, an error message to be given as the reason for failing
     * an operation. If empty, a sign that the operation is valid.
     */
    public static Optional<String> checkShardLimit(int newShards, ClusterState state) {
        Settings theseSettings = state.metaData().settings();
        int nodeCount = state.getNodes().getDataNodes().size();

        // Only enforce the shard limit if we have at least one data node, so that we don't block
        // index creation during cluster setup
        if (nodeCount == 0 || newShards < 0) {
            return Optional.empty();
        }
        int maxShardsPerNode = MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(theseSettings);
        int maxShardsInCluster = maxShardsPerNode * nodeCount;
        int currentOpenShards = state.getMetaData().getTotalOpenIndexShards();

        if ((currentOpenShards + newShards) > maxShardsInCluster) {
            String errorMessage = "this action would add [" + newShards + "] total shards, but this cluster currently has [" +
                currentOpenShards + "]/[" + maxShardsInCluster + "] maximum shards open";
            return Optional.of(errorMessage);
        }
        return Optional.empty();
    }

    private void updateDanglingIndicesInfo(Index index) {
        assert DiscoveryNode.isDataNode(settings) : "dangling indices information should only be persisted on data nodes";
        assert nodeWriteDanglingIndicesInfo : "writing dangling indices info is not enabled";
        assert danglingIndicesThreadPoolExecutor != null : "executor for dangling indices info is not available";
        if (danglingIndicesToWrite.add(index)) {
            logger.trace("triggered dangling indices update for {}", index);
            final long triggeredTimeMillis = threadPool.relativeTimeInMillis();
            try {
                danglingIndicesThreadPoolExecutor.execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(() -> new ParameterizedMessage("failed to write dangling indices state for index {}", index), e);
                    }

                    @Override
                    protected void doRun() {
                        final boolean exists = danglingIndicesToWrite.remove(index);
                        assert exists : "removed non-existing item for " + index;
                        final IndexService indexService = indices.get(index.getUUID());
                        if (indexService != null) {
                            final long executedTimeMillis = threadPool.relativeTimeInMillis();
                            logger.trace("writing out dangling indices state for index {}, triggered {} ago", index,
                                TimeValue.timeValueMillis(Math.min(0L, executedTimeMillis - triggeredTimeMillis)));
                            indexService.writeDanglingIndicesInfo();
                            final long completedTimeMillis = threadPool.relativeTimeInMillis();
                            logger.trace("writing out of dangling indices state for index {} completed after {}", index,
                                TimeValue.timeValueMillis(Math.min(0L, completedTimeMillis - executedTimeMillis)));
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

    // visible for testing
    public boolean allPendingDanglingIndicesWritten() {
        return nodeWriteDanglingIndicesInfo == false ||
            (danglingIndicesToWrite.isEmpty() && danglingIndicesThreadPoolExecutor.getActiveCount() == 0);
    }
}
