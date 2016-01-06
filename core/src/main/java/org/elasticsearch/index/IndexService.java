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
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.EngineClosedException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.IndexShard;
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
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

/**
 *
 */
public final class IndexService extends AbstractIndexComponent implements IndexComponent, Iterable<IndexShard> {

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
    private volatile Map<Integer, IndexShard> shards = emptyMap();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean deleted = new AtomicBoolean(false);
    private final IndexSettings indexSettings;

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
                        MapperRegistry mapperRegistry) throws IOException {
        super(indexSettings);
        this.indexSettings = indexSettings;
        this.analysisService = registry.build(indexSettings);
        this.similarityService = similarityService;
        this.mapperService = new MapperService(indexSettings, analysisService, similarityService, mapperRegistry);
        this.indexFieldData = new IndexFieldDataService(indexSettings, nodeServicesProvider.getIndicesFieldDataCache(), nodeServicesProvider.getCircuitBreakerService(), mapperService);
        this.shardStoreDeleter = shardStoreDeleter;
        this.eventListener = eventListener;
        this.nodeEnv = nodeEnv;
        this.nodeServicesProvider = nodeServicesProvider;
        this.indexStore = indexStore;
        indexFieldData.setListener(new FieldDataCacheListener(this));
        this.bitsetFilterCache = new BitsetFilterCache(indexSettings, nodeServicesProvider.getWarmer(), new BitsetCacheListener(this));
        this.indexCache = new IndexCache(indexSettings, queryCache, bitsetFilterCache);
        this.engineFactory = engineFactory;
        // initialize this last -- otherwise if the wrapper requires any other member to be non-null we fail with an NPE
        this.searcherWrapper = wrapperFactory.newWrapper(this);
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
                IOUtils.close(bitsetFilterCache, indexCache, mapperService, indexFieldData, analysisService);
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
                path = ShardPath.selectNewPathForShard(nodeEnv, shardId, this.indexSettings, routing.getExpectedShardSize() == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE ? getAvgShardSizeInBytes() : routing.getExpectedShardSize(),
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
            store = new Store(shardId, this.indexSettings, indexStore.newDirectoryService(path), lock, new StoreCloseListener(shardId, canDeleteShardContent, () -> nodeServicesProvider.getIndicesQueryCache().onClose(shardId)));
            if (useShadowEngine(primary, indexSettings)) {
                indexShard = new ShadowIndexShard(shardId, this.indexSettings, path, store, indexCache, mapperService, similarityService, indexFieldData, engineFactory, eventListener, searcherWrapper, nodeServicesProvider);
            } else {
                indexShard = new IndexShard(shardId, this.indexSettings, path, store, indexCache, mapperService, similarityService, indexFieldData, engineFactory, eventListener, searcherWrapper, nodeServicesProvider);
            }

            eventListener.indexShardStateChanged(indexShard, null, indexShard.state(), "shard created");
            eventListener.afterIndexShardCreated(indexShard);
            indexShard.updateRoutingEntry(routing, true);
            if (shards.isEmpty() && this.indexSettings.getTranslogSyncInterval().millis() != 0) {
                ThreadPool threadPool = nodeServicesProvider.getThreadPool();
                new AsyncTranslogFSync(this, threadPool).schedule(); // kick this off if we are the first shard in this service.
            }
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
                        final boolean flushEngine = deleted.get() == false && closed.get(); // only flush we are we closed (closed index or shutdown) and if we are not deleted
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

    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    public QueryShardContext getQueryShardContext() {
        return new QueryShardContext(indexSettings, nodeServicesProvider.getClient(), indexCache.bitsetFilterCache(), indexFieldData, mapperService(), similarityService(), nodeServicesProvider.getScriptService(), nodeServicesProvider.getIndicesQueriesRegistry());
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
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0l;
                    shard.shardBitsetFilterCache().onCached(ramBytesUsed);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, Accountable accountable) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0l;
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
        public void onCache(ShardId shardId, String fieldName, FieldDataType fieldDataType, Accountable ramUsage) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onCache(shardId, fieldName, fieldDataType, ramUsage);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, String fieldName, FieldDataType fieldDataType, boolean wasEvicted, long sizeInBytes) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onRemoval(shardId, fieldName, fieldDataType, wasEvicted, sizeInBytes);
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
                    throw new InvalidAliasNameException(indexSettings.getIndex(), aliasNames[0], "Unknown alias name was passed to alias Filter");
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

    private Query parse(AliasMetaData alias, QueryShardContext parseContext) {
        if (alias.filter() == null) {
            return null;
        }
        try {
            byte[] filterSource = alias.filter().uncompressed();
            try (XContentParser parser = XContentFactory.xContent(filterSource).createParser(filterSource)) {
                ParsedQuery parsedFilter = parseContext.parseInnerFilter(parser);
                return parsedFilter == null ? null : parsedFilter.query();
            }
        } catch (IOException ex) {
            throw new AliasFilterParsingException(parseContext.index(), alias.getAlias(), "Invalid alias filter", ex);
        }
    }

    public IndexMetaData getMetaData() {
        return indexSettings.getIndexMetaData();
    }

    public synchronized void updateMetaData(final IndexMetaData metadata) {
        if (indexSettings.updateIndexMetaData(metadata)) {
            final Settings settings = indexSettings.getSettings();
            for (final IndexShard shard : this.shards.values()) {
                try {
                    shard.onRefreshSettings(settings);
                } catch (Exception e) {
                    logger.warn("[{}] failed to refresh shard settings", e, shard.shardId().id());
                }
            }
            try {
                indexStore.onRefreshSettings(settings);
            } catch (Exception e) {
                logger.warn("failed to refresh index store settings", e);
            }
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


    /**
     * FSyncs the translog for all shards of this index in a defined interval.
     */
    final static class AsyncTranslogFSync implements Runnable {
        private final IndexService indexService;
        private final ThreadPool threadPool;

        AsyncTranslogFSync(IndexService indexService, ThreadPool threadPool) {
            this.indexService = indexService;
            this.threadPool = threadPool;
        }

        boolean mustRun() {
            // don't re-schedule if its closed or if we dont' have a single shard here..., we are done
            return (indexService.closed.get() || indexService.shards.isEmpty()) == false;
        }

        void schedule() {
            threadPool.schedule(indexService.getIndexSettings().getTranslogSyncInterval(), ThreadPool.Names.SAME, AsyncTranslogFSync.this);
        }

        @Override
        public void run() {
            if (mustRun()) {
                threadPool.executor(ThreadPool.Names.FLUSH).execute(() -> {
                    indexService.maybeFSyncTranslogs();
                    if (mustRun()) {
                        schedule();
                    }
                });
            }
        }
    }


}
