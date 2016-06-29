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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineClosedException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.IndexShard;
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
import org.elasticsearch.indices.AliasFilterParsingException;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

/**
 *
 */
public class IndexService extends AbstractIndexComponent implements IndicesClusterStateService.AllocatedIndex<IndexShard> {

    private final IndexEventListener eventListener;
    private final AnalysisService analysisService;
    private final IndexFieldDataService indexFieldData;
    private final BitsetFilterCache bitsetFilterCache;
    private final NodeEnvironment nodeEnv;
    private final ShardStoreDeleter shardStoreDeleter;
    private final NodeServicesProvider nodeServicesProvider;
    private final IndexStore indexStore;
    private final IndexSearcherWrapper searcherWrapper;
    private final IndexCache indexCache;
    private final MapperService mapperService;
    private final SimilarityService similarityService;
    private final EngineFactory engineFactory;
    private final IndexWarmer warmer;
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

    public IndexService(IndexSettings indexSettings, NodeEnvironment nodeEnv,
                        SimilarityService similarityService,
                        ShardStoreDeleter shardStoreDeleter,
                        AnalysisRegistry registry,
                        @Nullable EngineFactory engineFactory,
                        NodeServicesProvider nodeServicesProvider,
                        QueryCache queryCache,
                        IndexStore indexStore,
                        IndexEventListener eventListener,
                        IndexModule.IndexSearcherWrapperFactory wrapperFactory,
                        MapperRegistry mapperRegistry,
                        IndicesFieldDataCache indicesFieldDataCache,
                        List<SearchOperationListener> searchOperationListeners,
                        List<IndexingOperationListener> indexingOperationListeners) throws IOException {
        super(indexSettings);
        this.indexSettings = indexSettings;
        this.analysisService = registry.build(indexSettings);
        this.similarityService = similarityService;
        this.mapperService = new MapperService(indexSettings, analysisService, similarityService, mapperRegistry,
            IndexService.this::newQueryShardContext);
        this.indexFieldData = new IndexFieldDataService(indexSettings, indicesFieldDataCache,
            nodeServicesProvider.getCircuitBreakerService(), mapperService);
        this.shardStoreDeleter = shardStoreDeleter;
        this.bigArrays = nodeServicesProvider.getBigArrays();
        this.threadPool = nodeServicesProvider.getThreadPool();
        this.eventListener = eventListener;
        this.nodeEnv = nodeEnv;
        this.nodeServicesProvider = nodeServicesProvider;
        this.indexStore = indexStore;
        indexFieldData.setListener(new FieldDataCacheListener(this));
        this.bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetCacheListener(this));
        this.warmer = new IndexWarmer(indexSettings.getSettings(), threadPool,
            bitsetFilterCache.createListener(threadPool));
        this.indexCache = new IndexCache(indexSettings, queryCache, bitsetFilterCache);
        this.engineFactory = engineFactory;
        // initialize this last -- otherwise if the wrapper requires any other member to be non-null we fail with an NPE
        this.searcherWrapper = wrapperFactory.newWrapper(this);
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
    public @Nullable IndexShard getShardOrNull(int shardId) {
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

    public AnalysisService analysisService() {
        return this.analysisService;
    }

    public MapperService mapperService() {
        return mapperService;
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
                    } catch (Throwable t) {
                        logger.warn("failed to close shard", t);
                    }
                }
            } finally {
                IOUtils.close(bitsetFilterCache, indexCache, indexFieldData, analysisService, refreshTask, fsyncTask);
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
        final ShardLock lock = nodeEnv.shardLock(shardId, TimeUnit.SECONDS.toMillis(5));
        try {
            eventListener.beforeIndexShardCreated(shardId, indexSettings);
            ShardPath path;
            try {
                path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings);
            } catch (IllegalStateException ex) {
                logger.warn("{} failed to load shard path, trying to remove leftover", shardId);
                try {
                    ShardPath.deleteLeftoverShardDirectory(logger, nodeEnv, lock, this.indexSettings);
                    path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings);
                } catch (Throwable t) {
                    t.addSuppressed(ex);
                    throw t;
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
                throw new IndexShardAlreadyExistsException(shardId + " already exists");
            }

            logger.debug("creating shard_id {}", shardId);
            // if we are on a shared FS we only own the shard (ie. we can safely delete it) if we are the primary.
            final boolean canDeleteShardContent = IndexMetaData.isOnSharedFilesystem(indexSettings) == false ||
                (primary && IndexMetaData.isOnSharedFilesystem(indexSettings));
            final Engine.Warmer engineWarmer = (searcher) -> {
                IndexShard shard =  getShardOrNull(shardId.getId());
                if (shard != null) {
                    warmer.warm(searcher, shard, IndexService.this.indexSettings);
                }
            };
            store = new Store(shardId, this.indexSettings, indexStore.newDirectoryService(path), lock,
                new StoreCloseListener(shardId, canDeleteShardContent, () -> eventListener.onStoreClosed(shardId)));
            if (useShadowEngine(primary, indexSettings)) {
                indexShard = new ShadowIndexShard(routing, this.indexSettings, path, store, indexCache, mapperService, similarityService,
                    indexFieldData, engineFactory, eventListener, searcherWrapper, threadPool, bigArrays, engineWarmer,
                    searchOperationListeners);
                // no indexing listeners - shadow  engines don't index
            } else {
                indexShard = new IndexShard(routing, this.indexSettings, path, store, indexCache, mapperService, similarityService,
                    indexFieldData, engineFactory, eventListener, searcherWrapper, threadPool, bigArrays, engineWarmer,
                    searchOperationListeners, indexingOperationListeners);
            }
            eventListener.indexShardStateChanged(indexShard, null, indexShard.state(), "shard created");
            eventListener.afterIndexShardCreated(indexShard);
            shards = newMapBuilder(shards).put(shardId.id(), indexShard).immutableMap();
            success = true;
            return indexShard;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(lock);
                closeShard("initialization failed", shardId, indexShard, store, eventListener);
            }
        }
    }

    static boolean useShadowEngine(boolean primary, Settings indexSettings) {
        return primary == false && IndexMetaData.isIndexUsingShadowReplicas(indexSettings);
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
                    } catch (Throwable e) {
                        logger.debug("[{}] failed to close index shard", e, shardId);
                        // ignore
                    }
                }
                // call this before we close the store, so we can release resources for it
                listener.afterIndexShardClosed(sId, indexShard, indexSettings);
            }
        } finally {
            try {
                store.close();
            } catch (Throwable e) {
                logger.warn("[{}] failed to close store on shard removal (reason: [{}])", e, shardId, reason);
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
                logger.debug("[{}] failed to delete shard content - scheduled a retry", e, lock.getShardId().id());
            }
        }
    }

    public NodeServicesProvider getIndexServices() {
        return nodeServicesProvider;
    }

    @Override
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Creates a new QueryShardContext. The context has not types set yet, if types are required set them via
     * {@link QueryShardContext#setTypes(String...)}
     */
    public QueryShardContext newQueryShardContext(IndexReader indexReader) {
        return new QueryShardContext(
                indexSettings, indexCache.bitsetFilterCache(), indexFieldData, mapperService(),
                similarityService(), nodeServicesProvider.getScriptService(), nodeServicesProvider.getIndicesQueriesRegistry(),
                nodeServicesProvider.getClient(), indexReader,
                nodeServicesProvider.getClusterService().state()
        );
    }

    /**
     * Creates a new QueryShardContext. The context has not types set yet, if types are required set them via
     * {@link QueryShardContext#setTypes(String...)}. This context may be used for query parsing but cannot be
     * used for rewriting since it does not know about the current {@link IndexReader}.
     */
    public QueryShardContext newQueryShardContext() {
        return newQueryShardContext(null);
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    public BigArrays getBigArrays() {
        return bigArrays;
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

        public StoreCloseListener(ShardId shardId, boolean ownsShard, Closeable... toClose) {
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

        public FieldDataCacheListener(IndexService indexService) {
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

    /**
     * Returns the filter associated with listed filtering aliases.
     * <p>
     * The list of filtering aliases should be obtained by calling MetaData.filteringAliases.
     * Returns <tt>null</tt> if no filtering is required.</p>
     */
    public Query aliasFilter(QueryShardContext context, String... aliasNames) {
        if (aliasNames == null || aliasNames.length == 0) {
            return null;
        }
        final ImmutableOpenMap<String, AliasMetaData> aliases = indexSettings.getIndexMetaData().getAliases();
        if (aliasNames.length == 1) {
            AliasMetaData alias = aliases.get(aliasNames[0]);
            if (alias == null) {
                // This shouldn't happen unless alias disappeared after filteringAliases was called.
                throw new InvalidAliasNameException(index(), aliasNames[0], "Unknown alias name was passed to alias Filter");
            }
            return parse(alias, context);
        } else {
            // we need to bench here a bit, to see maybe it makes sense to use OrFilter
            BooleanQuery.Builder combined = new BooleanQuery.Builder();
            for (String aliasName : aliasNames) {
                AliasMetaData alias = aliases.get(aliasName);
                if (alias == null) {
                    // This shouldn't happen unless alias disappeared after filteringAliases was called.
                    throw new InvalidAliasNameException(indexSettings.getIndex(), aliasNames[0],
                        "Unknown alias name was passed to alias Filter");
                }
                Query parsedFilter = parse(alias, context);
                if (parsedFilter != null) {
                    combined.add(parsedFilter, BooleanClause.Occur.SHOULD);
                } else {
                    // The filter might be null only if filter was removed after filteringAliases was called
                    return null;
                }
            }
            return combined.build();
        }
    }

    private Query parse(AliasMetaData alias, QueryShardContext shardContext) {
        if (alias.filter() == null) {
            return null;
        }
        try {
            byte[] filterSource = alias.filter().uncompressed();
            try (XContentParser parser = XContentFactory.xContent(filterSource).createParser(filterSource)) {
                Optional<QueryBuilder> innerQueryBuilder = shardContext.newParseContext(parser).parseInnerQueryBuilder();
                if (innerQueryBuilder.isPresent()) {
                    return shardContext.toFilter(innerQueryBuilder.get()).query();
                }
                return null;
            }
        } catch (IOException ex) {
            throw new AliasFilterParsingException(shardContext.index(), alias.getAlias(), "Invalid alias filter", ex);
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
                    logger.warn("[{}] failed to notify shard about setting change", e, shard.shardId().id());
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
                } catch (EngineClosedException | AlreadyClosedException ex) {
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
                        } catch (EngineClosedException | AlreadyClosedException ex) {
                            // fine - continue;
                        }
                        continue;
                    default:
                        throw new IllegalStateException("unknown state: " + shard.state());
                }
            }
        }
    }

    static abstract class BaseAsyncTask implements Runnable, Closeable {
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
                    indexService.logger.warn("failed to run task {} - suppressing re-occurring exceptions unless the exception changes",
                        ex, toString());
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
    final static class AsyncTranslogFSync extends BaseAsyncTask {

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

    AsyncRefreshTask getRefreshTask() { // for tests
        return refreshTask;
    }

    AsyncTranslogFSync getFsyncTask() { // for tests
        return fsyncTask;
    }
}
