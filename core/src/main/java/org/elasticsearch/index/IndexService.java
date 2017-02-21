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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShadowIndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

public class IndexService extends AbstractIndexComponent implements IndicesClusterStateService.AllocatedIndex<IndexShard> {

    private final IndexEventListener eventListener;
    private final IndexFieldDataService indexFieldData;
    private final BitsetFilterCache bitsetFilterCache;
    private final NodeEnvironment nodeEnv;
    private final ShardStoreDeleter shardStoreDeleter;
    private final IndexStore indexStore;
    private final IndexSearcherWrapper searcherWrapper;
    private final IndexCache indexCache;
    private final MapperService mapperService;
    private final NamedXContentRegistry xContentRegistry;
    private final SimilarityService similarityService;
    private final EngineFactory engineFactory;
    private final IndexWarmer warmer;
    private final Consumer<ShardId> globalCheckpointSyncer;
    private volatile Map<Integer, IndexShard> shards = emptyMap();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean deleted = new AtomicBoolean(false);
    private final IndexSettings indexSettings;
    private final List<IndexingOperationListener> indexingOperationListeners;
    private final List<SearchOperationListener> searchOperationListeners;
    private volatile AsyncRefreshTask refreshTask;
    private volatile AsyncTranslogFSync fsyncTask;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private final AsyncGlobalCheckpointTask globalCheckpointTask;
    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final Client client;

    public IndexService(IndexSettings indexSettings, NodeEnvironment nodeEnv,
                        NamedXContentRegistry xContentRegistry,
                        SimilarityService similarityService,
                        ShardStoreDeleter shardStoreDeleter,
                        AnalysisRegistry registry,
                        @Nullable EngineFactory engineFactory,
                        CircuitBreakerService circuitBreakerService,
                        BigArrays bigArrays,
                        ThreadPool threadPool,
                        ScriptService scriptService,
                        ClusterService clusterService,
                        Client client,
                        QueryCache queryCache,
                        IndexStore indexStore,
                        IndexEventListener eventListener,
                        IndexModule.IndexSearcherWrapperFactory wrapperFactory,
                        MapperRegistry mapperRegistry,
                        IndicesFieldDataCache indicesFieldDataCache,
                        Consumer<ShardId> globalCheckpointSyncer,
                        List<SearchOperationListener> searchOperationListeners,
                        List<IndexingOperationListener> indexingOperationListeners) throws IOException {
        super(indexSettings);
        this.indexSettings = indexSettings;
        this.globalCheckpointSyncer = globalCheckpointSyncer;
        this.xContentRegistry = xContentRegistry;
        this.similarityService = similarityService;
        this.mapperService = new MapperService(indexSettings, registry.build(indexSettings), xContentRegistry, similarityService,
            mapperRegistry,
            // we parse all percolator queries as they would be parsed on shard 0
            () -> newQueryShardContext(0, null, () -> {
                throw new IllegalArgumentException("Percolator queries are not allowed to use the current timestamp");
            }));
        this.indexFieldData = new IndexFieldDataService(indexSettings, indicesFieldDataCache, circuitBreakerService, mapperService);
        this.shardStoreDeleter = shardStoreDeleter;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.client = client;
        this.eventListener = eventListener;
        this.nodeEnv = nodeEnv;
        this.indexStore = indexStore;
        indexFieldData.setListener(new FieldDataCacheListener(this));
        this.bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetCacheListener(this));
        this.warmer = new IndexWarmer(indexSettings.getSettings(), threadPool,
            bitsetFilterCache.createListener(threadPool));
        this.indexCache = new IndexCache(indexSettings, queryCache, bitsetFilterCache);
        this.engineFactory = engineFactory;
        // initialize this last -- otherwise if the wrapper requires any other member to be non-null we fail with an NPE
        this.searcherWrapper = wrapperFactory.newWrapper(this);
        this.globalCheckpointTask = new AsyncGlobalCheckpointTask(this);
        this.indexingOperationListeners = Collections.unmodifiableList(indexingOperationListeners);
        this.searchOperationListeners = Collections.unmodifiableList(searchOperationListeners);
        // kick off async ops for the first shard in this index
        this.refreshTask = new AsyncRefreshTask(this);
        rescheduleFsyncTask(indexSettings.getTranslogDurability());
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

    public Set<Integer> shardIds() {
        return shards.keySet();
    }

    public IndexCache cache() {
        return indexCache;
    }

    public IndexFieldDataService fieldData() {
        return indexFieldData;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.mapperService.getIndexAnalyzers();
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public SimilarityService similarityService() {
        return similarityService;
    }

    public synchronized void close(final String reason, boolean delete) throws IOException {
        if (closed.compareAndSet(false, true)) {
            deleted.compareAndSet(false, delete);
            try {
                final Set<Integer> shardIds = shardIds();
                for (final int shardId : shardIds) {
                    try {
                        removeShard(shardId, reason);
                    } catch (Exception e) {
                        logger.warn("failed to close shard", e);
                    }
                }
            } finally {
                IOUtils.close(bitsetFilterCache, indexCache, indexFieldData, mapperService, refreshTask, fsyncTask, globalCheckpointTask);
            }
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
            sum += indexShard.store().stats().sizeInBytes();
            count++;
        }
        if (count == 0) {
            return -1L;
        } else {
            return sum / count;
        }
    }

    public synchronized IndexShard createShard(ShardRouting routing) throws IOException {
        final boolean primary = routing.primary();
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
        try {
            lock = nodeEnv.shardLock(shardId, TimeUnit.SECONDS.toMillis(5));
            eventListener.beforeIndexShardCreated(shardId, indexSettings);
            ShardPath path;
            try {
                path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings);
            } catch (IllegalStateException ex) {
                logger.warn("{} failed to load shard path, trying to remove leftover", shardId);
                try {
                    ShardPath.deleteLeftoverShardDirectory(logger, nodeEnv, lock, this.indexSettings);
                    path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings);
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
                path = ShardPath.selectNewPathForShard(nodeEnv, shardId, this.indexSettings,
                    routing.getExpectedShardSize() == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                        ? getAvgShardSizeInBytes() : routing.getExpectedShardSize(),
                    dataPathToShardCount);
                logger.debug("{} creating using a new path [{}]", shardId, path);
            } else {
                logger.debug("{} creating using an existing path [{}]", shardId, path);
            }

            if (shards.containsKey(shardId.id())) {
                throw new IllegalStateException(shardId + " already exists");
            }

            logger.debug("creating shard_id {}", shardId);
            // if we are on a shared FS we only own the shard (ie. we can safely delete it) if we are the primary.
            final boolean canDeleteShardContent = this.indexSettings.isOnSharedFilesystem() == false ||
                (primary && this.indexSettings.isOnSharedFilesystem());
            final Engine.Warmer engineWarmer = (searcher) -> {
                IndexShard shard =  getShardOrNull(shardId.getId());
                if (shard != null) {
                    warmer.warm(searcher, shard, IndexService.this.indexSettings);
                }
            };
            store = new Store(shardId, this.indexSettings, indexStore.newDirectoryService(path), lock,
                new StoreCloseListener(shardId, canDeleteShardContent, () -> eventListener.onStoreClosed(shardId)));
            if (useShadowEngine(primary, this.indexSettings)) {
                indexShard = new ShadowIndexShard(routing, this.indexSettings, path, store, indexCache, mapperService, similarityService,
                    indexFieldData, engineFactory, eventListener, searcherWrapper, threadPool, bigArrays, engineWarmer,
                    searchOperationListeners);
                // no indexing listeners - shadow  engines don't index
            } else {
                indexShard = new IndexShard(routing, this.indexSettings, path, store, indexCache, mapperService, similarityService,
                    indexFieldData, engineFactory, eventListener, searcherWrapper, threadPool, bigArrays, engineWarmer,
                    () -> globalCheckpointSyncer.accept(shardId),
                    searchOperationListeners, indexingOperationListeners);
            }
            eventListener.indexShardStateChanged(indexShard, null, indexShard.state(), "shard created");
            eventListener.afterIndexShardCreated(indexShard);
            shards = newMapBuilder(shards).put(shardId.id(), indexShard).immutableMap();
            success = true;
            return indexShard;
        } catch (ShardLockObtainFailedException e) {
            throw new IOException("failed to obtain in-memory shard lock", e);
        } finally {
            if (success == false) {
                if (lock != null) {
                    IOUtils.closeWhileHandlingException(lock);
                }
                closeShard("initialization failed", shardId, indexShard, store, eventListener);
            }
        }
    }

    static boolean useShadowEngine(boolean primary, IndexSettings indexSettings) {
        return primary == false && indexSettings.isShadowReplicaIndex();
    }

    @Override
    public synchronized void removeShard(int shardId, String reason) {
        final ShardId sId = new ShardId(index(), shardId);
        final IndexShard indexShard;
        if (shards.containsKey(shardId) == false) {
            return;
        }
        logger.debug("[{}] closing... (reason: [{}])", shardId, reason);
        HashMap<Integer, IndexShard> newShards = new HashMap<>(shards);
        indexShard = newShards.remove(shardId);
        shards = unmodifiableMap(newShards);
        closeShard(reason, sId, indexShard, indexShard.store(), indexShard.getIndexEventListener());
        logger.debug("[{}] closed (reason: [{}])", shardId, reason);
    }

    private void closeShard(String reason, ShardId sId, IndexShard indexShard, Store store, IndexEventListener listener) {
        final int shardId = sId.id();
        final Settings indexSettings = this.getIndexSettings().getSettings();
        try {
            try {
                listener.beforeIndexShardClosed(sId, indexShard, indexSettings);
            } finally {
                // this logic is tricky, we want to close the engine so we rollback the changes done to it
                // and close the shard so no operations are allowed to it
                if (indexShard != null) {
                    try {
                        // only flush we are we closed (closed index or shutdown) and if we are not deleted
                        final boolean flushEngine = deleted.get() == false && closed.get();
                        indexShard.close(reason, flushEngine);
                    } catch (Exception e) {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to close index shard", shardId), e);
                        // ignore
                    }
                }
                // call this before we close the store, so we can release resources for it
                listener.afterIndexShardClosed(sId, indexShard, indexSettings);
            }
        } finally {
            try {
                if (store != null) {
                    store.close();
                } else {
                    logger.trace("[{}] store not initialized prior to closing shard, nothing to close", shardId);
                }
            } catch (Exception e) {
                logger.warn(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "[{}] failed to close store on shard removal (reason: [{}])", shardId, reason), e);
            }
        }
    }


    private void onShardClose(ShardLock lock, boolean ownsShard) {
        if (deleted.get()) { // we remove that shards content if this index has been deleted
            try {
                if (ownsShard) {
                    try {
                        eventListener.beforeIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                    } finally {
                        shardStoreDeleter.deleteShardStore("delete index", lock, indexSettings);
                        eventListener.afterIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                    }
                }
            } catch (IOException e) {
                shardStoreDeleter.addPendingDelete(lock.getShardId(), indexSettings);
                logger.debug(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "[{}] failed to delete shard content - scheduled a retry", lock.getShardId().id()), e);
            }
        }
    }

    @Override
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Creates a new QueryShardContext. The context has not types set yet, if types are required set them via
     * {@link QueryShardContext#setTypes(String...)}.
     *
     * Passing a {@code null} {@link IndexReader} will return a valid context, however it won't be able to make
     * {@link IndexReader}-specific optimizations, such as rewriting containing range queries.
     */
    public QueryShardContext newQueryShardContext(int shardId, IndexReader indexReader, LongSupplier nowInMillis) {
        return new QueryShardContext(
            shardId, indexSettings, indexCache.bitsetFilterCache(), indexFieldData, mapperService(),
                similarityService(), scriptService, xContentRegistry,
                client, indexReader,
            nowInMillis);
    }

    /**
     * The {@link ThreadPool} to use for this index.
     */
    public ThreadPool getThreadPool() {
        return threadPool;
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

    List<IndexingOperationListener> getIndexOperationListeners() { // pkg private for testing
        return indexingOperationListeners;
    }

    List<SearchOperationListener> getSearchOperationListener() { // pkg private for testing
        return searchOperationListeners;
    }

    @Override
    public boolean updateMapping(IndexMetaData indexMetaData) throws IOException {
        return mapperService().updateMapping(indexMetaData);
    }

    private class StoreCloseListener implements Store.OnClose {
        private final ShardId shardId;
        private final boolean ownsShard;
        private final Closeable[] toClose;

        StoreCloseListener(ShardId shardId, boolean ownsShard, Closeable... toClose) {
            this.shardId = shardId;
            this.ownsShard = ownsShard;
            this.toClose = toClose;
        }

        @Override
        public void handle(ShardLock lock) {
            try {
                assert lock.getShardId().equals(shardId) : "shard id mismatch, expected: " + shardId + " but got: " + lock.getShardId();
                onShardClose(lock, ownsShard);
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

    private final class FieldDataCacheListener implements IndexFieldDataCache.Listener {
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
        public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onRemoval(shardId, fieldName, wasEvicted, sizeInBytes);
                }
            }
        }
    }

    public IndexMetaData getMetaData() {
        return indexSettings.getIndexMetaData();
    }

    @Override
    public synchronized void updateMetaData(final IndexMetaData metadata) {
        final Translog.Durability oldTranslogDurability = indexSettings.getTranslogDurability();
        if (indexSettings.updateIndexMetaData(metadata)) {
            for (final IndexShard shard : this.shards.values()) {
                try {
                    shard.onSettingsChanged();
                } catch (Exception e) {
                    logger.warn(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "[{}] failed to notify shard about setting change", shard.shardId().id()), e);
                }
            }
            if (refreshTask.getInterval().equals(indexSettings.getRefreshInterval()) == false) {
                rescheduleRefreshTasks();
            }
            final Translog.Durability durability = indexSettings.getTranslogDurability();
            if (durability != oldTranslogDurability) {
                rescheduleFsyncTask(durability);
            }
        }

        // update primary terms
        for (final IndexShard shard : this.shards.values()) {
            shard.updatePrimaryTerm(metadata.primaryTerm(shard.shardId().id()));
        }
    }

    private void rescheduleFsyncTask(Translog.Durability durability) {
        try {
            if (fsyncTask != null) {
                fsyncTask.close();
            }
        } finally {
            fsyncTask = durability == Translog.Durability.REQUEST ? null : new AsyncTranslogFSync(this);
        }
    }

    private void rescheduleRefreshTasks() {
        try {
            refreshTask.close();
        } finally {
            refreshTask = new AsyncRefreshTask(this);
        }

    }

    public interface ShardStoreDeleter {
        void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException;

        void addPendingDelete(ShardId shardId, IndexSettings indexSettings);
    }

    final EngineFactory getEngineFactory() {
        return engineFactory;
    } // pkg private for testing

    final IndexSearcherWrapper getSearcherWrapper() {
        return searcherWrapper;
    } // pkg private for testing

    final IndexStore getIndexStore() {
        return indexStore;
    } // pkg private for testing

    private void maybeFSyncTranslogs() {
        if (indexSettings.getTranslogDurability() == Translog.Durability.ASYNC) {
            for (IndexShard shard : this.shards.values()) {
                try {
                    Translog translog = shard.getTranslog();
                    if (translog.syncNeeded()) {
                        translog.sync();
                    }
                } catch (AlreadyClosedException ex) {
                    // fine - continue;
                } catch (IOException e) {
                    logger.warn("failed to sync translog", e);
                }
            }
        }
    }

    private void maybeRefreshEngine() {
        if (indexSettings.getRefreshInterval().millis() > 0) {
            for (IndexShard shard : this.shards.values()) {
                switch (shard.state()) {
                    case CREATED:
                    case RECOVERING:
                    case CLOSED:
                        continue;
                    case POST_RECOVERY:
                    case STARTED:
                    case RELOCATED:
                        try {
                            if (shard.isRefreshNeeded()) {
                                shard.refresh("schedule");
                            }
                        } catch (IndexShardClosedException | AlreadyClosedException ex) {
                            // fine - continue;
                        }
                        continue;
                    default:
                        throw new IllegalStateException("unknown state: " + shard.state());
                }
            }
        }
    }

    private void maybeUpdateGlobalCheckpoints() {
        for (IndexShard shard : this.shards.values()) {
            if (shard.routingEntry().primary()) {
                switch (shard.state()) {
                    case CREATED:
                    case RECOVERING:
                    case CLOSED:
                    case RELOCATED:
                        continue;
                    case POST_RECOVERY:
                    case STARTED:
                        try {
                            shard.updateGlobalCheckpointOnPrimary();
                        } catch (AlreadyClosedException ex) {
                            // fine - continue, the shard was concurrently closed on us.
                        }
                        continue;
                    default:
                        throw new IllegalStateException("unknown state: " + shard.state());
                }
            }
        }
    }


    abstract static class BaseAsyncTask implements Runnable, Closeable {
        protected final IndexService indexService;
        protected final ThreadPool threadPool;
        private final TimeValue interval;
        private ScheduledFuture<?> scheduledFuture;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private volatile Exception lastThrownException;

        BaseAsyncTask(IndexService indexService, TimeValue interval) {
            this.indexService = indexService;
            this.threadPool = indexService.getThreadPool();
            this.interval = interval;
            onTaskCompletion();
        }

        boolean mustReschedule() {
            // don't re-schedule if its closed or if we don't have a single shard here..., we are done
            return indexService.closed.get() == false
                && closed.get() == false && interval.millis() > 0;
        }

        private synchronized void onTaskCompletion() {
            if (mustReschedule()) {
                if (indexService.logger.isTraceEnabled()) {
                    indexService.logger.trace("scheduling {} every {}", toString(), interval);
                }
                this.scheduledFuture = threadPool.schedule(interval, getThreadPool(), BaseAsyncTask.this);
            } else {
                indexService.logger.trace("scheduled {} disabled", toString());
                this.scheduledFuture = null;
            }
        }

        boolean isScheduled() {
            return scheduledFuture != null;
        }

        @Override
        public final void run() {
            try {
                runInternal();
            } catch (Exception ex) {
                if (lastThrownException == null || sameException(lastThrownException, ex) == false) {
                    // prevent the annoying fact of logging the same stuff all the time with an interval of 1 sec will spam all your logs
                    indexService.logger.warn(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "failed to run task {} - suppressing re-occurring exceptions unless the exception changes",
                            toString()),
                        ex);
                    lastThrownException = ex;
                }
            } finally {
                onTaskCompletion();
            }
        }

        private static boolean sameException(Exception left, Exception right) {
            if (left.getClass() == right.getClass()) {
                if (Objects.equals(left.getMessage(), right.getMessage())) {
                    StackTraceElement[] stackTraceLeft = left.getStackTrace();
                    StackTraceElement[] stackTraceRight = right.getStackTrace();
                    if (stackTraceLeft.length == stackTraceRight.length) {
                        for (int i = 0; i < stackTraceLeft.length; i++) {
                            if (stackTraceLeft[i].equals(stackTraceRight[i]) == false) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        protected abstract void runInternal();

        protected String getThreadPool() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public synchronized void close() {
            if (closed.compareAndSet(false, true)) {
                FutureUtils.cancel(scheduledFuture);
                scheduledFuture = null;
            }
        }

        TimeValue getInterval() {
            return interval;
        }

        boolean isClosed() {
            return this.closed.get();
        }
    }

    /**
     * FSyncs the translog for all shards of this index in a defined interval.
     */
    static final class AsyncTranslogFSync extends BaseAsyncTask {

        AsyncTranslogFSync(IndexService indexService) {
            super(indexService, indexService.getIndexSettings().getTranslogSyncInterval());
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.FLUSH;
        }

        @Override
        protected void runInternal() {
            indexService.maybeFSyncTranslogs();
        }

        @Override
        public String toString() {
            return "translog_sync";
        }
    }

    final class AsyncRefreshTask extends BaseAsyncTask {

        AsyncRefreshTask(IndexService indexService) {
            super(indexService, indexService.getIndexSettings().getRefreshInterval());
        }

        @Override
        protected void runInternal() {
            indexService.maybeRefreshEngine();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.REFRESH;
        }

        @Override
        public String toString() {
            return "refresh";
        }
    }

    final class AsyncGlobalCheckpointTask extends BaseAsyncTask {

        AsyncGlobalCheckpointTask(IndexService indexService) {
            super(indexService, indexService.getIndexSettings().getGlobalCheckpointInterval());
        }

        @Override
        protected void runInternal() {
            indexService.maybeUpdateGlobalCheckpoints();
        }

        @Override
        public String toString() {
            return "global_checkpoint";
        }
    }

    AsyncRefreshTask getRefreshTask() { // for tests
        return refreshTask;
    }

    AsyncTranslogFSync getFsyncTask() { // for tests
        return fsyncTask;
    }

    AsyncGlobalCheckpointTask getGlobalCheckpointTask() { // for tests
        return globalCheckpointTask;
    }
}
