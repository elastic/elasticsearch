/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.gateway.WriteStateException;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ThreadPoolMergeExecutorService;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsAccounting;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.NodeMappingStats;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SearchIndexNameMatcher;
import org.elasticsearch.index.search.stats.SearchStatsSettings;
import org.elasticsearch.index.search.stats.ShardSearchLoadRateProvider;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.GlobalCheckpointSyncer;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.IndexingStatsSettings;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.ShardNotInPrimaryModeException;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndexRemovalReason;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.core.Strings.format;

public class IndexService extends AbstractIndexComponent implements IndicesClusterStateService.AllocatedIndex<IndexShard> {

    private final IndexEventListener eventListener;
    private final IndexFieldDataService indexFieldData;
    private final BitsetFilterCache bitsetFilterCache;
    private final NodeEnvironment nodeEnv;
    private final ShardStoreDeleter shardStoreDeleter;
    private final IndexStorePlugin.IndexFoldersDeletionListener indexFoldersDeletionListener;
    private final IndexStorePlugin.DirectoryFactory directoryFactory;
    private final IndexStorePlugin.RecoveryStateFactory recoveryStateFactory;
    private final IndexStorePlugin.SnapshotCommitSupplier snapshotCommitSupplier;
    private final CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper;
    private final Engine.IndexCommitListener indexCommitListener;
    private final IndexCache indexCache;
    private final MapperService mapperService;
    private final XContentParserConfiguration parserConfiguration;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final SimilarityService similarityService;
    private final EngineFactory engineFactory;
    private final IndexWarmer warmer;
    private volatile Map<Integer, IndexShard> shards = Map.of();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean deleted = new AtomicBoolean(false);
    private final IndexSettings indexSettings;
    private final IndexAnalyzers indexAnalyzers;
    private final List<SearchOperationListener> searchOperationListeners;
    private final List<IndexingOperationListener> indexingOperationListeners;
    private final BooleanSupplier allowExpensiveQueries;
    private volatile AsyncRefreshTask refreshTask;
    private volatile AsyncTranslogFSync fsyncTask;
    private final AsyncGlobalCheckpointTask globalCheckpointTask;
    private final AsyncRetentionLeaseSyncTask retentionLeaseSyncTask;

    // don't convert to Setting<> and register... we only set this in tests and register via a plugin
    private final String INDEX_TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING = "index.translog.retention.check_interval";

    private final AsyncTrimTranslogTask trimTranslogTask;
    private final ThreadPool threadPool;
    @Nullable
    private final ThreadPoolMergeExecutorService threadPoolMergeExecutorService;
    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final Client client;
    private final CircuitBreakerService circuitBreakerService;
    private final IndexNameExpressionResolver expressionResolver;
    private final Supplier<Sort> indexSortSupplier;
    private final ValuesSourceRegistry valuesSourceRegistry;
    private final MapperMetrics mapperMetrics;
    private final QueryRewriteInterceptor queryRewriteInterceptor;
    private final IndexingStatsSettings indexingStatsSettings;
    private final SearchStatsSettings searchStatsSettings;

    @SuppressWarnings("this-escape")
    public IndexService(
        IndexSettings indexSettings,
        IndexCreationContext indexCreationContext,
        NodeEnvironment nodeEnv,
        XContentParserConfiguration parserConfiguration,
        SimilarityService similarityService,
        ShardStoreDeleter shardStoreDeleter,
        IndexAnalyzers indexAnalyzers,
        EngineFactory engineFactory,
        CircuitBreakerService circuitBreakerService,
        BigArrays bigArrays,
        ThreadPool threadPool,
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService,
        ScriptService scriptService,
        ClusterService clusterService,
        Client client,
        QueryCache queryCache,
        IndexStorePlugin.DirectoryFactory directoryFactory,
        IndexEventListener eventListener,
        Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>> wrapperFactory,
        MapperRegistry mapperRegistry,
        IndicesFieldDataCache indicesFieldDataCache,
        List<SearchOperationListener> searchOperationListeners,
        List<IndexingOperationListener> indexingOperationListeners,
        NamedWriteableRegistry namedWriteableRegistry,
        IdFieldMapper idFieldMapper,
        BooleanSupplier allowExpensiveQueries,
        IndexNameExpressionResolver expressionResolver,
        ValuesSourceRegistry valuesSourceRegistry,
        IndexStorePlugin.RecoveryStateFactory recoveryStateFactory,
        IndexStorePlugin.IndexFoldersDeletionListener indexFoldersDeletionListener,
        IndexStorePlugin.SnapshotCommitSupplier snapshotCommitSupplier,
        Engine.IndexCommitListener indexCommitListener,
        MapperMetrics mapperMetrics,
        QueryRewriteInterceptor queryRewriteInterceptor,
        IndexingStatsSettings indexingStatsSettings,
        SearchStatsSettings searchStatsSettings
    ) {
        super(indexSettings);
        assert indexCreationContext != IndexCreationContext.RELOAD_ANALYZERS
            : "IndexCreationContext.RELOAD_ANALYZERS should only be used when reloading analysers";
        this.allowExpensiveQueries = allowExpensiveQueries;
        this.indexSettings = indexSettings;
        this.parserConfiguration = parserConfiguration;
        this.similarityService = similarityService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.circuitBreakerService = circuitBreakerService;
        this.expressionResolver = expressionResolver;
        this.valuesSourceRegistry = valuesSourceRegistry;
        this.snapshotCommitSupplier = snapshotCommitSupplier;
        this.indexAnalyzers = indexAnalyzers;
        if (needsMapperService(indexSettings, indexCreationContext)) {
            assert indexAnalyzers != null;
            this.bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetCacheListener(this));
            this.mapperService = new MapperService(
                clusterService,
                indexSettings,
                indexAnalyzers,
                parserConfiguration,
                similarityService,
                mapperRegistry,
                // we parse all percolator queries as they would be parsed on shard 0
                () -> newSearchExecutionContext(0, 0, null, System::currentTimeMillis, null, emptyMap()),
                idFieldMapper,
                scriptService,
                bitsetFilterCache::getBitSetProducer,
                mapperMetrics
            );
            this.indexFieldData = new IndexFieldDataService(indexSettings, indicesFieldDataCache, circuitBreakerService);
            boolean sourceOnly = Boolean.parseBoolean(indexSettings.getSettings().get("index.source_only"));
            if (indexSettings.getIndexSortConfig().hasIndexSort() && sourceOnly == false) {
                // we delay the actual creation of the sort order for this index because the mapping has not been merged yet.
                // The sort order is validated right after the merge of the mapping later in the process.
                this.indexSortSupplier = () -> indexSettings.getIndexSortConfig()
                    .buildIndexSort(
                        mapperService::fieldType,
                        (fieldType, searchLookup) -> loadFielddata(fieldType, FieldDataContext.noRuntimeFields("index sort"))
                    );
            } else {
                this.indexSortSupplier = () -> null;
            }
            indexFieldData.setListener(new FieldDataCacheListener(this));
            this.warmer = new IndexWarmer(threadPool, indexFieldData, bitsetFilterCache.createListener(threadPool));
            this.indexCache = new IndexCache(queryCache, bitsetFilterCache);
        } else {
            assert indexAnalyzers == null;
            this.mapperService = null;
            this.indexFieldData = null;
            this.indexSortSupplier = () -> null;
            this.bitsetFilterCache = null;
            this.warmer = null;
            this.indexCache = null;
        }

        this.shardStoreDeleter = shardStoreDeleter;
        this.indexFoldersDeletionListener = indexFoldersDeletionListener;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.threadPoolMergeExecutorService = threadPoolMergeExecutorService;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.client = client;
        this.eventListener = eventListener;
        this.nodeEnv = nodeEnv;
        this.directoryFactory = directoryFactory;
        this.recoveryStateFactory = recoveryStateFactory;
        this.engineFactory = Objects.requireNonNull(engineFactory);
        // initialize this last -- otherwise if the wrapper requires any other member to be non-null we fail with an NPE
        this.readerWrapper = wrapperFactory.apply(this);
        this.searchOperationListeners = Collections.unmodifiableList(searchOperationListeners);
        this.indexingOperationListeners = Collections.unmodifiableList(indexingOperationListeners);
        this.indexCommitListener = indexCommitListener;
        this.mapperMetrics = mapperMetrics;
        this.queryRewriteInterceptor = queryRewriteInterceptor;
        try (var ignored = threadPool.getThreadContext().clearTraceContext()) {
            // kick off async ops for the first shard in this index
            this.refreshTask = new AsyncRefreshTask(this);
            this.trimTranslogTask = new AsyncTrimTranslogTask(this);
            this.globalCheckpointTask = new AsyncGlobalCheckpointTask(this);
            this.retentionLeaseSyncTask = new AsyncRetentionLeaseSyncTask(this);
        }
        this.indexingStatsSettings = indexingStatsSettings;
        this.searchStatsSettings = searchStatsSettings;
        updateFsyncTaskIfNecessary();
    }

    static boolean needsMapperService(IndexSettings indexSettings, IndexCreationContext indexCreationContext) {
        return false == (indexSettings.getIndexMetadata().getState() == IndexMetadata.State.CLOSE
            && indexCreationContext == IndexCreationContext.CREATE_INDEX); // metadata verification needs a mapper service
    }

    public enum IndexCreationContext {
        CREATE_INDEX,
        METADATA_VERIFICATION,
        RELOAD_ANALYZERS
    }

    public int numberOfShards() {
        return shards.size();
    }

    public IndexEventListener getIndexEventListener() {
        return this.eventListener;
    }

    @Override
    public Iterator<IndexShard> iterator() {
        return shards.values().iterator();
    }

    public boolean hasShard(int shardId) {
        return shards.containsKey(shardId);
    }

    /**
     * Return the shard with the provided id, or null if there is no such shard.
     */
    @Override
    @Nullable
    public IndexShard getShardOrNull(int shardId) {
        return shards.get(shardId);
    }

    /**
     * Return the shard with the provided id, or throw an exception if it doesn't exist.
     */
    public IndexShard getShard(int shardId) {
        IndexShard indexShard = getShardOrNull(shardId);
        if (indexShard == null) {
            throw new ShardNotFoundException(new ShardId(index(), shardId));
        }
        return indexShard;
    }

    public NodeMappingStats getNodeMappingStats() {
        if (mapperService == null) {
            return null;
        }
        long numFields = mapperService().mappingLookup().getTotalMapperCount();
        long totalEstimatedOverhead = numFields * 1024L; // 1KiB estimated per mapping
        // Assume all segments have the same mapping; otherwise, we need to acquire searchers to count the actual fields.
        int numLeaves = 0;
        for (IndexShard shard : shards.values()) {
            try {
                numLeaves += shard.commitStats().getNumLeaves();
            } catch (AlreadyClosedException ignored) {

            }
        }
        return new NodeMappingStats(numFields, totalEstimatedOverhead, numLeaves, numLeaves * numFields);
    }

    public Set<Integer> shardIds() {
        return shards.keySet();
    }

    public IndexCache cache() {
        return indexCache;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.mapperService.getIndexAnalyzers();
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public SimilarityService similarityService() {
        return similarityService;
    }

    public Supplier<Sort> getIndexSortSupplier() {
        return indexSortSupplier;
    }

    public synchronized void close(final String reason, boolean delete, Executor shardCloseExecutor, ActionListener<Void> closeListener) {
        if (closed.compareAndSet(false, true)) {
            deleted.compareAndSet(false, delete);
            try (var refs = new RefCountingRunnable(() -> ActionListener.run(closeListener, l -> {
                IOUtils.close(
                    bitsetFilterCache,
                    indexCache,
                    indexFieldData,
                    indexAnalyzers,
                    refreshTask,
                    fsyncTask,
                    trimTranslogTask,
                    globalCheckpointTask,
                    retentionLeaseSyncTask
                );
                l.onResponse(null);
            }))) {
                final Set<Integer> shardIds = shardIds();
                for (final int shardId : shardIds) {
                    ActionListener.run(refs.acquireListener().delegateResponse((l, e) -> {
                        logger.warn("failed to close shard", e);
                        l.onResponse(null);
                    }), l -> removeShard(shardId, reason, shardCloseExecutor, l));
                }
            }
        } else {
            closeListener.onResponse(null);
        }
    }

    // method is synchronized so that IndexService can't be closed while we're writing out dangling indices information
    public synchronized void writeDanglingIndicesInfo() {
        if (closed.get()) {
            return;
        }
        try {
            IndexMetadata.FORMAT.writeAndCleanup(getMetadata(), nodeEnv.indexPaths(index()));
        } catch (WriteStateException e) {
            logger.warn(() -> format("failed to write dangling indices state for index %s", index()), e);
        }
    }

    // method is synchronized so that IndexService can't be closed while we're deleting dangling indices information
    public synchronized void deleteDanglingIndicesInfo() {
        if (closed.get()) {
            return;
        }
        try {
            MetadataStateFormat.deleteMetaState(nodeEnv.indexPaths(index()));
        } catch (IOException e) {
            logger.warn(() -> format("failed to delete dangling indices state for index %s", index()), e);
        }
    }

    public String indexUUID() {
        return indexSettings.getUUID();
    }

    // NOTE: O(numShards) cost, but numShards should be smallish?
    private long getAvgShardSizeInBytes() throws IOException {
        long sum = 0;
        int count = 0;
        for (IndexShard indexShard : this) {
            sum += indexShard.store().stats(0L, LongUnaryOperator.identity()).sizeInBytes();
            count++;
        }
        if (count == 0) {
            return -1L;
        } else {
            return sum / count;
        }
    }

    public synchronized IndexShard createShard(
        final ShardRouting routing,
        final GlobalCheckpointSyncer globalCheckpointSyncer,
        final RetentionLeaseSyncer retentionLeaseSyncer,
        final ShardSearchLoadRateProvider shardSearchLoadRateProvider
    ) throws IOException {
        Objects.requireNonNull(retentionLeaseSyncer);
        /*
         * TODO: we execute this in parallel but it's a synced method. Yet, we might
         * be able to serialize the execution via the cluster state in the future. for now we just
         * keep it synced.
         */
        if (closed.get()) {
            throw new IllegalStateException("Can't create shard " + routing.shardId() + ", closed");
        }
        final Settings indexSettings = this.indexSettings.getSettings();
        final ShardId shardId = routing.shardId();
        boolean success = false;
        Store store = null;
        IndexShard indexShard = null;
        ShardLock lock = null;
        eventListener.beforeIndexShardCreated(routing, indexSettings);
        try {
            // Try and acquire the shard lock, but we are on the cluster applier thread so we do not wait if it is unavailable; in that
            // case, the IndicesClusterStateService will try again (in the background)
            lock = nodeEnv.shardLock(shardId, "starting shard");
            ShardPath path;
            try {
                path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings.customDataPath());
            } catch (IllegalStateException ex) {
                logger.warn("{} failed to load shard path, trying to remove leftover", shardId);
                try {
                    ShardPath.deleteLeftoverShardDirectory(
                        logger,
                        nodeEnv,
                        lock,
                        this.indexSettings,
                        shardPaths -> indexFoldersDeletionListener.beforeShardFoldersDeleted(
                            shardId,
                            this.indexSettings,
                            shardPaths,
                            IndexRemovalReason.FAILURE
                        )
                    );
                    path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings.customDataPath());
                } catch (Exception inner) {
                    ex.addSuppressed(inner);
                    throw ex;
                }
            }

            if (path == null) {
                // TODO: we should, instead, hold a "bytes reserved" of how large we anticipate this shard will be, e.g. for a shard
                // that's being relocated/replicated we know how large it will become once it's done copying:
                // Count up how many shards are currently on each data path:
                Map<Path, Integer> dataPathToShardCount = new HashMap<>();
                for (IndexShard shard : this) {
                    Path dataPath = shard.shardPath().getRootStatePath();
                    Integer curCount = dataPathToShardCount.get(dataPath);
                    if (curCount == null) {
                        curCount = 0;
                    }
                    dataPathToShardCount.put(dataPath, curCount + 1);
                }
                path = ShardPath.selectNewPathForShard(
                    nodeEnv,
                    shardId,
                    this.indexSettings,
                    routing.getExpectedShardSize() == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                        ? getAvgShardSizeInBytes()
                        : routing.getExpectedShardSize(),
                    dataPathToShardCount
                );
                logger.debug("{} creating using a new path [{}]", shardId, path);
            } else {
                logger.debug("{} creating using an existing path [{}]", shardId, path);
            }

            if (shards.containsKey(shardId.id())) {
                throw new IllegalStateException(shardId + " already exists");
            }

            logger.debug("creating shard_id {}", shardId);
            // if we are on a shared FS we only own the shard (ie. we can safely delete it) if we are the primary.
            final Engine.Warmer engineWarmer = (reader) -> {
                IndexShard shard = getShardOrNull(shardId.getId());
                if (shard != null) {
                    warmer.warm(reader, shard, IndexService.this.indexSettings);
                }
            };
            final Directory directory = directoryFactory.newDirectory(this.indexSettings, path, routing);
            store = new Store(
                shardId,
                this.indexSettings,
                directory,
                lock,
                new StoreCloseListener(shardId, () -> eventListener.onStoreClosed(shardId)),
                this.indexSettings.getIndexSortConfig().hasIndexSort()
            );
            eventListener.onStoreCreated(shardId);
            indexShard = new IndexShard(
                routing,
                this.indexSettings,
                path,
                store,
                indexSortSupplier,
                indexCache,
                mapperService,
                similarityService,
                engineFactory,
                eventListener,
                readerWrapper,
                threadPool,
                threadPoolMergeExecutorService,
                bigArrays,
                engineWarmer,
                searchOperationListeners,
                indexingOperationListeners,
                globalCheckpointSyncer,
                retentionLeaseSyncer,
                circuitBreakerService,
                snapshotCommitSupplier,
                System::nanoTime,
                indexCommitListener,
                mapperMetrics,
                indexingStatsSettings,
                searchStatsSettings,
                shardSearchLoadRateProvider
            );
            eventListener.indexShardStateChanged(indexShard, null, indexShard.state(), "shard created");
            eventListener.afterIndexShardCreated(indexShard);
            shards = Maps.copyMapWithAddedEntry(shards, shardId.id(), indexShard);
            success = true;
            return indexShard;
        } finally {
            if (success == false) {
                if (lock != null) {
                    IOUtils.closeWhileHandlingException(lock);
                }
                final var finalStore = store;
                final var finalIndexShard = indexShard;
                CloseUtils.executeDirectly(
                    l -> closeShard(
                        "initialization failed",
                        shardId,
                        finalIndexShard,
                        finalStore,
                        eventListener,
                        EsExecutors.DIRECT_EXECUTOR_SERVICE /* closing a shard that failed to start up should be fast enough */,
                        l
                    )
                );
            }
        }
    }

    @Override
    public synchronized void removeShard(int shardId, String reason, Executor closeExecutor, ActionListener<Void> closeListener) {
        final IndexShard indexShard = shards.get(shardId);
        if (indexShard == null) {
            closeListener.onResponse(null);
            return;
        }
        logger.debug("[{}] closing... (reason: [{}])", shardId, reason);
        final var wrappedListener = logger.isDebugEnabled()
            ? ActionListener.runBefore(closeListener, () -> logger.debug("[{}] closed (reason: [{}])", shardId, reason))
            : closeListener;

        shards = Maps.copyMapWithRemovedEntry(shards, shardId);
        closeShard(
            reason,
            indexShard.shardId(),
            indexShard,
            indexShard.store(),
            indexShard.getIndexEventListener(),
            closeExecutor,
            wrappedListener
        );
        logger.debug("[{}] removed (reason: [{}])", shardId, reason);
    }

    private void closeShard(
        String reason,
        ShardId sId,
        IndexShard indexShard,
        Store store,
        IndexEventListener listener,
        Executor closeExecutor,
        ActionListener<Void> closeListener
    ) {
        final int shardId = sId.id();
        final Settings indexSettings = this.getIndexSettings().getSettings();
        if (store != null) {
            store.beforeClose();
        }
        try {
            try {
                listener.beforeIndexShardClosed(sId, indexShard, indexSettings);
            } finally {
                // this logic is tricky, we want to close the engine so we rollback the changes done to it
                // and close the shard so no operations are allowed to it
                if (indexShard == null) {
                    closeListener.onResponse(null);
                } else {
                    // only flush if we are closed (closed index or shutdown) and if we are not deleted
                    final boolean flushEngine = deleted.get() == false && closed.get();
                    // if the store is still open, want to keep it open until afterIndexShardClosed
                    assert store == null || store.hasReferences() : "store exists but already closed";
                    final var hasStoreRef = store != null && store.tryIncRef(); // being cautious
                    ActionListener.run(new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void unused) {
                            try {
                                // call this before we close the store, so we can release resources for it
                                listener.afterIndexShardClosed(sId, indexShard, indexSettings);
                            } finally {
                                try {
                                    if (hasStoreRef) {
                                        store.decRef();
                                    }
                                } finally {
                                    closeListener.onResponse(null);
                                }
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.debug(() -> "[" + shardId + "] failed to close index shard", e);
                            onResponse(null); // otherwise ignore the exception
                        }
                    }, l -> indexShard.close(reason, flushEngine, closeExecutor, l));
                    listener.afterIndexShardClosing(sId, indexShard, indexSettings);
                }
            }
        } finally {
            try {
                if (store != null) {
                    store.close();
                } else {
                    logger.trace("[{}] store not initialized prior to closing shard, nothing to close", shardId);
                }
            } catch (Exception e) {
                logger.warn(() -> format("[%s] failed to close store on shard removal (reason: [%s])", shardId, reason), e);
            }
        }
    }

    private void onShardClose(ShardLock lock) {
        if (deleted.get()) { // we remove that shards content if this index has been deleted
            try {
                try {
                    eventListener.beforeIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                } finally {
                    shardStoreDeleter.deleteShardStore("delete index", lock, indexSettings, IndexRemovalReason.DELETED);
                    eventListener.afterIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                }
            } catch (IOException e) {
                shardStoreDeleter.addPendingDelete(lock.getShardId(), indexSettings, IndexRemovalReason.DELETED);
                logger.debug(() -> "[" + lock.getShardId().id() + "] failed to delete shard content - scheduled a retry", e);
            }
        }
    }

    public RecoveryState createRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, DiscoveryNode sourceNode) {
        return recoveryStateFactory.newRecoveryState(shardRouting, targetNode, sourceNode);
    }

    @Override
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Creates a new {@link SearchExecutionContext}.
     *
     * Passing a {@code null} {@link IndexSearcher} will return a valid context, however it won't be able to make
     * {@link IndexReader}-specific optimizations, such as rewriting containing range queries.
     */
    public SearchExecutionContext newSearchExecutionContext(
        int shardId,
        int shardRequestIndex,
        IndexSearcher searcher,
        LongSupplier nowInMillis,
        String clusterAlias,
        Map<String, Object> runtimeMappings
    ) {
        return newSearchExecutionContext(shardId, shardRequestIndex, searcher, nowInMillis, clusterAlias, runtimeMappings, null);
    }

    public SearchExecutionContext newSearchExecutionContext(
        int shardId,
        int shardRequestIndex,
        IndexSearcher searcher,
        LongSupplier nowInMillis,
        String clusterAlias,
        Map<String, Object> runtimeMappings,
        Integer requestSize
    ) {
        final SearchIndexNameMatcher indexNameMatcher = new SearchIndexNameMatcher(
            index().getName(),
            clusterAlias,
            clusterService,
            expressionResolver
        );
        var mapperService = mapperService();
        return new SearchExecutionContext(
            shardId,
            shardRequestIndex,
            indexSettings,
            indexCache.bitsetFilterCache(),
            this::loadFielddata,
            mapperService,
            mapperService.mappingLookup(),
            similarityService(),
            scriptService,
            parserConfiguration,
            namedWriteableRegistry,
            client,
            searcher,
            nowInMillis,
            clusterAlias,
            indexNameMatcher,
            allowExpensiveQueries,
            valuesSourceRegistry,
            runtimeMappings,
            requestSize,
            mapperMetrics
        );
    }

    /**
     * Creates a new {@link QueryRewriteContext}.
     * This class is used to rewrite queries in case access to the index is not required, since we can
     * decide rewriting based on mappings alone. This saves the cost of pulling an index searcher as
     * well as the associated cost of refreshing idle shards.
     */
    public QueryRewriteContext newQueryRewriteContext(
        final LongSupplier nowInMillis,
        final Map<String, Object> runtimeMappings,
        final String clusterAlias
    ) {
        final SearchIndexNameMatcher indexNameMatcher = new SearchIndexNameMatcher(
            index().getName(),
            clusterAlias,
            clusterService,
            expressionResolver
        );
        final MapperService mapperService = mapperService();
        final MappingLookup mappingLookup = mapperService.mappingLookup();
        return new QueryRewriteContext(
            parserConfiguration,
            client,
            nowInMillis,
            mapperService,
            mappingLookup,
            parseRuntimeMappings(runtimeMappings, mapperService, indexSettings, mappingLookup),
            indexSettings,
            new Index(
                RemoteClusterAware.buildRemoteIndexName(clusterAlias, indexSettings.getIndex().getName()),
                indexSettings.getIndex().getUUID()
            ),
            indexNameMatcher,
            namedWriteableRegistry,
            valuesSourceRegistry,
            allowExpensiveQueries,
            scriptService,
            null,
            null,
            null,
            false
        );
    }

    /**
     * The {@link ThreadPool} to use for this index.
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    public @Nullable ThreadPoolMergeExecutorService getThreadPoolMergeExecutorService() {
        return threadPoolMergeExecutorService;
    }

    /**
     * The {@link BigArrays} to use for this index.
     */
    public BigArrays getBigArrays() {
        return bigArrays;
    }

    /**
     * The {@link ScriptService} to use for this index.
     */
    public ScriptService getScriptService() {
        return scriptService;
    }

    public CircuitBreakerService breakerService() {
        return circuitBreakerService;
    }

    List<IndexingOperationListener> getIndexOperationListeners() { // pkg private for testing
        return indexingOperationListeners;
    }

    List<SearchOperationListener> getSearchOperationListener() { // pkg private for testing
        return searchOperationListeners;
    }

    public void updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) {
        if (mapperService != null) {
            mapperService.updateMapping(currentIndexMetadata, newIndexMetadata);
        }
    }

    private class StoreCloseListener implements Store.OnClose {
        private final ShardId shardId;
        private final Closeable[] toClose;

        StoreCloseListener(ShardId shardId, Closeable... toClose) {
            this.shardId = shardId;
            this.toClose = toClose;
        }

        @Override
        public void accept(ShardLock lock) {
            try {
                assert lock.getShardId().equals(shardId) : "shard id mismatch, expected: " + shardId + " but got: " + lock.getShardId();
                onShardClose(lock);
            } finally {
                try {
                    IOUtils.close(toClose);
                } catch (IOException ex) {
                    logger.debug("failed to close resource", ex);
                }
            }

        }
    }

    private static final class BitsetCacheListener implements BitsetFilterCache.Listener {
        final IndexService indexService;

        private BitsetCacheListener(IndexService indexService) {
            this.indexService = indexService;
        }

        @Override
        public void onCache(ShardId shardId, Accountable accountable) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0L;
                    shard.shardBitsetFilterCache().onCached(ramBytesUsed);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, Accountable accountable) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0L;
                    shard.shardBitsetFilterCache().onRemoval(ramBytesUsed);
                }
            }
        }
    }

    private static final class FieldDataCacheListener implements IndexFieldDataCache.Listener {
        final IndexService indexService;

        FieldDataCacheListener(IndexService indexService) {
            this.indexService = indexService;
        }

        @Override
        public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onCache(shardId, fieldName, ramUsage);
                }
            }
        }

        @Override
        public void onCache(ShardId shardId, String fieldName, GlobalOrdinalsAccounting info) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onCache(shardId, fieldName, info);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onRemoval(shardId, fieldName, wasEvicted, sizeInBytes);
                }
            }
        }
    }

    public IndexMetadata getMetadata() {
        return indexSettings.getIndexMetadata();
    }

    private final CopyOnWriteArrayList<Consumer<IndexMetadata>> metadataListeners = new CopyOnWriteArrayList<>();

    public void addMetadataListener(Consumer<IndexMetadata> listener) {
        metadataListeners.add(listener);
    }

    @Override
    public synchronized void updateMetadata(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) {
        final boolean updateIndexSettings = indexSettings.updateIndexMetadata(newIndexMetadata);

        if (Assertions.ENABLED && currentIndexMetadata != null) {
            final long currentSettingsVersion = currentIndexMetadata.getSettingsVersion();
            final long newSettingsVersion = newIndexMetadata.getSettingsVersion();
            if (currentSettingsVersion == newSettingsVersion) {
                assert updateIndexSettings == false : "No index updates are expected as index settings version has not changed";
            } else {
                assert updateIndexSettings : "Index updates are expected as index settings version has changed";
                assert currentSettingsVersion < newSettingsVersion
                    : "expected current settings version ["
                        + currentSettingsVersion
                        + "] "
                        + "to be less than new settings version ["
                        + newSettingsVersion
                        + "]";
            }
        }

        if (updateIndexSettings) {
            for (final IndexShard shard : this.shards.values()) {
                try {
                    shard.onSettingsChanged();
                } catch (Exception e) {
                    logger.warn(() -> "[" + shard.shardId().id() + "] failed to notify shard about setting change", e);
                }
            }
            if (refreshTask.getInterval().equals(indexSettings.getRefreshInterval()) == false) {
                // once we change the refresh interval we schedule yet another refresh
                // to ensure we are in a clean and predictable state.
                // it doesn't matter if we move from or to <code>-1</code> in both cases we want
                // docs to become visible immediately. This also flushes all pending indexing / search requests
                // that are waiting for a refresh.
                threadPool.executor(ThreadPool.Names.REFRESH).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("forced refresh failed after interval change", e);
                    }

                    @Override
                    protected void doRun() {
                        maybeRefreshEngine(true);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return true;
                    }
                });
                rescheduleRefreshTasks();
            }
            updateFsyncTaskIfNecessary();
        }

        metadataListeners.forEach(c -> c.accept(newIndexMetadata));
    }

    private void updateFsyncTaskIfNecessary() {
        if (indexSettings.getTranslogDurability() == Translog.Durability.REQUEST) {
            try {
                if (fsyncTask != null) {
                    fsyncTask.close();
                }
            } finally {
                fsyncTask = null;
            }
        } else if (fsyncTask == null) {
            fsyncTask = new AsyncTranslogFSync(this);
        } else {
            fsyncTask.updateIfNeeded();
        }
    }

    private void rescheduleRefreshTasks() {
        try {
            refreshTask.close();
        } finally {
            refreshTask = new AsyncRefreshTask(this);
        }
    }

    public static Function<String, String> dateMathExpressionResolverAt() {
        return expression -> IndexNameExpressionResolver.resolveDateMathExpression(expression, System.currentTimeMillis());
    }

    public static Function<String, String> dateMathExpressionResolverAt(long instant) {
        return expression -> IndexNameExpressionResolver.resolveDateMathExpression(expression, instant);
    }

    public interface ShardStoreDeleter {
        void deleteShardStore(String reasonText, ShardLock lock, IndexSettings indexSettings, IndexRemovalReason reason) throws IOException;

        void addPendingDelete(ShardId shardId, IndexSettings indexSettings, IndexRemovalReason reason);
    }

    public final EngineFactory getEngineFactory() {
        return engineFactory;
    }

    final CheckedFunction<DirectoryReader, DirectoryReader, IOException> getReaderWrapper() {
        return readerWrapper;
    } // pkg private for testing

    final IndexStorePlugin.DirectoryFactory getDirectoryFactory() {
        return directoryFactory;
    } // pkg private for testing

    private void maybeFSyncTranslogs() {
        if (indexSettings.getTranslogDurability() == Translog.Durability.ASYNC) {
            for (IndexShard shard : this.shards.values()) {
                try {
                    if (shard.isSyncNeeded()) {
                        shard.sync();
                    }
                } catch (AlreadyClosedException ex) {
                    // fine - continue;
                } catch (IOException e) {
                    logger.warn("failed to sync translog", e);
                }
            }
        }
    }

    private void maybeRefreshEngine(boolean force) {
        if (indexSettings.getRefreshInterval().millis() > 0 || force) {
            for (IndexShard shard : this.shards.values()) {
                shard.scheduledRefresh(new ActionListener<>() {
                    @Override
                    public void onResponse(Boolean ignored) {}

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof IndexShardClosedException == false && e instanceof AlreadyClosedException == false) {
                            logger.warn("unexpected exception while performing scheduled refresh", e);
                        }
                    }
                });
            }
        }
    }

    private void maybeTrimTranslog() {
        for (IndexShard shard : this.shards.values()) {
            switch (shard.state()) {
                case CREATED:
                case RECOVERING:
                case CLOSED:
                    continue;
                case POST_RECOVERY:
                case STARTED:
                    try {
                        shard.trimTranslog();
                    } catch (IndexShardClosedException | AlreadyClosedException ex) {
                        // fine - continue;
                    }
                    continue;
                default:
                    throw new IllegalStateException("unknown state: " + shard.state());
            }
        }
    }

    private void maybeSyncGlobalCheckpoints() {
        sync(is -> is.maybeSyncGlobalCheckpoint("background"), "global checkpoint");
    }

    private void syncRetentionLeases() {
        sync(IndexShard::syncRetentionLeases, "retention lease");
    }

    private void sync(final Consumer<IndexShard> sync, final String source) {
        for (final IndexShard shard : this.shards.values()) {
            if (shard.routingEntry().active() && shard.routingEntry().primary()) {
                switch (shard.state()) {
                    case CLOSED:
                    case CREATED:
                    case RECOVERING:
                        continue;
                    case POST_RECOVERY:
                        assert false : "shard " + shard.shardId() + " is in post-recovery but marked as active";
                        continue;
                    case STARTED:
                        try {
                            shard.runUnderPrimaryPermit(() -> sync.accept(shard), e -> {
                                if (e instanceof AlreadyClosedException == false
                                    && e instanceof IndexShardClosedException == false
                                    && e instanceof ShardNotInPrimaryModeException == false) {
                                    logger.warn(() -> format("%s failed to execute %s sync", shard.shardId(), source), e);
                                }
                            }, EsExecutors.DIRECT_EXECUTOR_SERVICE);
                        } catch (final AlreadyClosedException | IndexShardClosedException e) {
                            // the shard was closed concurrently, continue
                        }
                        continue;
                    default:
                        throw new IllegalStateException("unknown state [" + shard.state() + "]");
                }
            }
        }
    }

    abstract static class BaseAsyncTask extends AbstractAsyncTask {

        protected final IndexService indexService;

        BaseAsyncTask(final IndexService indexService, final Executor executor, final TimeValue interval) {
            super(indexService.logger, indexService.threadPool, executor, interval, true);
            this.indexService = indexService;
            rescheduleIfNecessary();
        }

        @Override
        protected boolean mustReschedule() {
            // don't re-schedule if the IndexService instance is closed or if the index is closed
            return indexService.closed.get() == false
                && indexService.indexSettings.getIndexMetadata().getState() == IndexMetadata.State.OPEN;
        }
    }

    /**
     * FSyncs the translog for all shards of this index in a defined interval.
     */
    static final class AsyncTranslogFSync extends BaseAsyncTask {

        AsyncTranslogFSync(IndexService indexService) {
            super(
                indexService,
                indexService.threadPool.executor(ThreadPool.Names.FLUSH),
                indexService.getIndexSettings().getTranslogSyncInterval()
            );
        }

        @Override
        protected void runInternal() {
            indexService.maybeFSyncTranslogs();
        }

        void updateIfNeeded() {
            final TimeValue newInterval = indexService.getIndexSettings().getTranslogSyncInterval();
            if (newInterval.equals(getInterval()) == false) {
                setInterval(newInterval);
            }
        }

        @Override
        public String toString() {
            return "translog_sync";
        }
    }

    static final class AsyncRefreshTask extends BaseAsyncTask {

        AsyncRefreshTask(IndexService indexService) {
            super(
                indexService,
                indexService.threadPool.executor(ThreadPool.Names.REFRESH),
                indexService.getIndexSettings().getRefreshInterval()
            );
        }

        @Override
        protected void runInternal() {
            indexService.maybeRefreshEngine(false);
        }

        @Override
        public String toString() {
            return "refresh";
        }
    }

    final class AsyncTrimTranslogTask extends BaseAsyncTask {

        AsyncTrimTranslogTask(IndexService indexService) {
            super(
                indexService,
                threadPool.generic(),
                indexService.getIndexSettings()
                    .getSettings()
                    .getAsTime(INDEX_TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING, TimeValue.timeValueMinutes(10))
            );
        }

        @Override
        protected boolean mustReschedule() {
            return indexService.closed.get() == false;
        }

        @Override
        protected void runInternal() {
            indexService.maybeTrimTranslog();
        }

        @Override
        public String toString() {
            return "trim_translog";
        }
    }

    // this setting is intentionally not registered, it is only used in tests
    public static final Setting<TimeValue> GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING = Setting.timeSetting(
        "index.global_checkpoint_sync.interval",
        new TimeValue(30, TimeUnit.SECONDS),
        new TimeValue(0, TimeUnit.MILLISECONDS),
        Property.Dynamic,
        Property.IndexScope
    );

    // this setting is intentionally not registered, it is only used in tests
    public static final Setting<TimeValue> RETENTION_LEASE_SYNC_INTERVAL_SETTING = Setting.timeSetting(
        "index.soft_deletes.retention_lease.sync_interval",
        new TimeValue(30, TimeUnit.SECONDS),
        new TimeValue(0, TimeUnit.MILLISECONDS),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Background task that syncs the global checkpoint to replicas.
     */
    private static final class AsyncGlobalCheckpointTask extends BaseAsyncTask {

        AsyncGlobalCheckpointTask(final IndexService indexService) {
            // index.global_checkpoint_sync_interval is not a real setting, it is only registered in tests
            super(
                indexService,
                indexService.getThreadPool().generic(),
                GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.get(indexService.getIndexSettings().getSettings())
            );
        }

        @Override
        protected void runInternal() {
            indexService.maybeSyncGlobalCheckpoints();
        }

        @Override
        public String toString() {
            return "global_checkpoint_sync";
        }
    }

    private static final class AsyncRetentionLeaseSyncTask extends BaseAsyncTask {

        AsyncRetentionLeaseSyncTask(final IndexService indexService) {
            super(
                indexService,
                indexService.threadPool.executor(ThreadPool.Names.MANAGEMENT),
                RETENTION_LEASE_SYNC_INTERVAL_SETTING.get(indexService.getIndexSettings().getSettings())
            );
        }

        @Override
        protected void runInternal() {
            indexService.syncRetentionLeases();
        }

        @Override
        public String toString() {
            return "retention_lease_sync";
        }

    }

    AsyncRefreshTask getRefreshTask() { // for tests
        return refreshTask;
    }

    AsyncTranslogFSync getFsyncTask() { // for tests
        return fsyncTask;
    }

    AsyncTrimTranslogTask getTrimTranslogTask() { // for tests
        return trimTranslogTask;
    }

    /**
     * Clears the caches for the given shard id if the shard is still allocated on this node
     */
    public boolean clearCaches(boolean queryCache, boolean fieldDataCache, String... fields) {
        boolean clearedAtLeastOne = false;
        if (queryCache) {
            clearedAtLeastOne = true;
            indexCache.query().clear("api");
        }
        if (fieldDataCache) {
            clearedAtLeastOne = true;
            if (fields.length == 0) {
                indexFieldData.clear();
            } else {
                for (String field : fields) {
                    indexFieldData.clearField(field);
                }
            }
        }
        if (clearedAtLeastOne == false) {
            if (fields.length == 0) {
                indexCache.clear("api");
                indexFieldData.clear();
            } else {
                // only clear caches relating to the specified fields
                for (String field : fields) {
                    indexFieldData.clearField(field);
                }
            }
        }
        return clearedAtLeastOne;
    }

    public static Map<String, MappedFieldType> parseRuntimeMappings(
        Map<String, Object> runtimeMappings,
        MapperService mapperService,
        IndexSettings indexSettings,
        MappingLookup lookup
    ) {
        if (runtimeMappings.isEmpty()) {
            return Collections.emptyMap();
        }
        // TODO add specific tests to SearchExecutionTests similar to the ones in FieldTypeLookupTests
        MappingParserContext parserContext = mapperService.parserContext();
        Map<String, RuntimeField> runtimeFields = RuntimeField.parseRuntimeFields(new HashMap<>(runtimeMappings), parserContext, false);
        Map<String, MappedFieldType> runtimeFieldTypes = RuntimeField.collectFieldTypes(runtimeFields.values());
        if (false == indexSettings.getIndexMetadata().getRoutingPaths().isEmpty()) {
            for (String r : runtimeMappings.keySet()) {
                if (Regex.simpleMatch(indexSettings.getIndexMetadata().getRoutingPaths(), r)) {
                    throw new IllegalArgumentException("runtime fields may not match [routing_path] but [" + r + "] matched");
                }
            }
        }
        runtimeFieldTypes.keySet().forEach(lookup::validateDoesNotShadow);
        return runtimeFieldTypes;
    }

    public IndexFieldData<?> loadFielddata(MappedFieldType fieldType, FieldDataContext fieldDataContext) {
        return indexFieldData.getForField(fieldType, fieldDataContext);
    }
}
