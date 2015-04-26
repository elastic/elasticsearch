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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.bitset.ShardBitsetFilterCacheModule;
import org.elasticsearch.index.cache.filter.ShardFilterCacheModule;
import org.elasticsearch.index.cache.query.ShardQueryCacheModule;
import org.elasticsearch.index.deletionpolicy.DeletionPolicyModule;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.ShardFieldDataModule;
import org.elasticsearch.index.gateway.IndexShardGatewayModule;
import org.elasticsearch.index.gateway.IndexShardGatewayService;
import org.elasticsearch.index.get.ShardGetModule;
import org.elasticsearch.index.indexing.ShardIndexingModule;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.policy.MergePolicyModule;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerModule;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.index.percolator.PercolatorShardModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.search.stats.ShardSearchModule;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotModule;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreModule;
import org.elasticsearch.index.suggest.SuggestShardModule;
import org.elasticsearch.index.termvectors.ShardTermVectorsModule;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogModule;
import org.elasticsearch.index.translog.TranslogService;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InternalIndicesLifecycle;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ShardsPluginsModule;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

/**
 *
 */
public class IndexService extends AbstractIndexComponent implements IndexComponent, Iterable<IndexShard> {

    private final Injector injector;

    private final Settings indexSettings;

    private final PluginsService pluginsService;

    private final InternalIndicesLifecycle indicesLifecycle;

    private final AnalysisService analysisService;

    private final MapperService mapperService;

    private final IndexQueryParserService queryParserService;

    private final SimilarityService similarityService;

    private final IndexAliasesService aliasesService;

    private final IndexCache indexCache;

    private final IndexFieldDataService indexFieldData;

    private final BitsetFilterCache bitsetFilterCache;

    private final IndexSettingsService settingsService;

    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesServices;

    private volatile ImmutableMap<Integer, Tuple<IndexShard, Injector>> shards = ImmutableMap.of();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean deleted = new AtomicBoolean(false);

    @Inject
    public IndexService(Injector injector, Index index, @IndexSettings Settings indexSettings, NodeEnvironment nodeEnv,
                        AnalysisService analysisService, MapperService mapperService, IndexQueryParserService queryParserService,
                        SimilarityService similarityService, IndexAliasesService aliasesService, IndexCache indexCache,
                        IndexSettingsService settingsService,
                        IndexFieldDataService indexFieldData, BitsetFilterCache bitSetFilterCache, IndicesService indicesServices) {
        super(index, indexSettings);
        this.injector = injector;
        this.indexSettings = indexSettings;
        this.analysisService = analysisService;
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        this.similarityService = similarityService;
        this.aliasesService = aliasesService;
        this.indexCache = indexCache;
        this.indexFieldData = indexFieldData;
        this.settingsService = settingsService;
        this.bitsetFilterCache = bitSetFilterCache;

        this.pluginsService = injector.getInstance(PluginsService.class);
        this.indicesServices = indicesServices;
        this.indicesLifecycle = (InternalIndicesLifecycle) injector.getInstance(IndicesLifecycle.class);

        // inject workarounds for cyclic dep
        indexCache.filter().setIndexService(this);
        indexFieldData.setIndexService(this);
        bitSetFilterCache.setIndexService(this);
        this.nodeEnv = nodeEnv;
    }

    public int numberOfShards() {
        return shards.size();
    }

    public InternalIndicesLifecycle indicesLifecycle() {
        return this.indicesLifecycle;
    }

    @Override
    public Iterator<IndexShard> iterator() {
        return Iterators.transform(shards.values().iterator(), new Function<Tuple<IndexShard, Injector>, IndexShard>() {
            @Override
            public IndexShard apply(Tuple<IndexShard, Injector> input) {
                return input.v1();
            }
        });
    }

    public boolean hasShard(int shardId) {
        return shards.containsKey(shardId);
    }

    /**
     * Return the shard with the provided id, or null if there is no such shard.
     */
    @Nullable
    public IndexShard shard(int shardId) {
        Tuple<IndexShard, Injector> indexShardInjectorTuple = shards.get(shardId);
        if (indexShardInjectorTuple != null) {
            return indexShardInjectorTuple.v1();
        }
        return null;
    }
    /**
     * Return the shard with the provided id, or throw an exception if it doesn't exist.
     */
    public IndexShard shardSafe(int shardId) throws IndexShardMissingException {
        IndexShard indexShard = shard(shardId);
        if (indexShard == null) {
            throw new IndexShardMissingException(new ShardId(index, shardId));
        }
        return indexShard;
    }

    public Set<Integer> shardIds() {
        return shards.keySet();
    }

    public Injector injector() {
        return injector;
    }

    public IndexSettingsService settingsService() {
        return this.settingsService;
    }

    public IndexCache cache() {
        return indexCache;
    }

    public IndexFieldDataService fieldData() {
        return indexFieldData;
    }

    public BitsetFilterCache bitsetFilterCache() {
        return bitsetFilterCache;
    }

    public AnalysisService analysisService() {
        return this.analysisService;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public IndexQueryParserService queryParserService() {
        return queryParserService;
    }

    public SimilarityService similarityService() {
        return similarityService;
    }

    public IndexAliasesService aliasesService() {
        return aliasesService;
    }

    public synchronized void close(final String reason, boolean delete) {
        if (closed.compareAndSet(false, true)) {
            deleted.compareAndSet(false, delete);
            final Set<Integer> shardIds = shardIds();
            for (final int shardId : shardIds) {
                try {
                    removeShard(shardId, reason);
                } catch (Throwable t) {
                    logger.warn("failed to close shard", t);
                }
            }
        }
    }

    /**
     * Return the shard injector for the provided id, or throw an exception if there is no such shard.
     */
    public Injector shardInjectorSafe(int shardId) throws IndexShardMissingException {
        Tuple<IndexShard, Injector> tuple = shards.get(shardId);
        if (tuple == null) {
            throw new IndexShardMissingException(new ShardId(index, shardId));
        }
        return tuple.v2();
    }

    public String indexUUID() {
        return indexSettings.get(IndexMetaData.SETTING_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
    }

    public synchronized IndexShard createShard(int sShardId, boolean primary) throws ElasticsearchException {
        /*
         * TODO: we execute this in parallel but it's a synced method. Yet, we might
         * be able to serialize the execution via the cluster state in the future. for now we just
         * keep it synced.
         */
        if (closed.get()) {
            throw new ElasticsearchIllegalStateException("Can't create shard [" + index.name() + "][" + sShardId + "], closed");
        }
        final ShardId shardId = new ShardId(index, sShardId);
        ShardLock lock = null;
        boolean success = false;
        Injector shardInjector = null;
        try {

            ShardPath path = ShardPath.loadShardPath(logger, nodeEnv, shardId, indexSettings);
            if (path == null) {
                path = ShardPath.selectNewPathForShard(nodeEnv, shardId, indexSettings);
                logger.debug("{} creating using a new path [{}]", shardId, path);
            } else {
                logger.debug("{} creating using an existing path [{}]", shardId, path);
            }

            lock = nodeEnv.shardLock(shardId, TimeUnit.SECONDS.toMillis(5));
            if (shards.containsKey(shardId.id())) {
                throw new IndexShardAlreadyExistsException(shardId + " already exists");
            }

            indicesLifecycle.beforeIndexShardCreated(shardId, indexSettings);

            logger.debug("creating shard_id {}", shardId);
            // if we are on a shared FS we only own the shard (ie. we can safely delete it) if we are the primary.
            final boolean canDeleteShardContent = IndexMetaData.isOnSharedFilesystem(indexSettings) == false ||
                    (primary && IndexMetaData.isOnSharedFilesystem(indexSettings));
            ModulesBuilder modules = new ModulesBuilder();
            modules.add(new ShardsPluginsModule(indexSettings, pluginsService));
            modules.add(new IndexShardModule(shardId, primary, indexSettings));
            modules.add(new ShardIndexingModule());
            modules.add(new ShardSearchModule());
            modules.add(new ShardGetModule());
            modules.add(new StoreModule(indexSettings, injector.getInstance(IndexStore.class).shardDirectory(), lock,
                    new StoreCloseListener(shardId, canDeleteShardContent), path));
            modules.add(new DeletionPolicyModule(indexSettings));
            modules.add(new MergePolicyModule(indexSettings));
            modules.add(new MergeSchedulerModule(indexSettings));
            modules.add(new ShardFilterCacheModule());
            modules.add(new ShardQueryCacheModule());
            modules.add(new ShardBitsetFilterCacheModule());
            modules.add(new ShardFieldDataModule());
            modules.add(new TranslogModule(indexSettings));
            modules.add(new IndexShardGatewayModule());
            modules.add(new PercolatorShardModule());
            modules.add(new ShardTermVectorsModule());
            modules.add(new IndexShardSnapshotModule());
            modules.add(new SuggestShardModule());
            try {
                shardInjector = modules.createChildInjector(injector);
            } catch (CreationException e) {
                throw new IndexShardCreationException(shardId, Injectors.getFirstErrorFailure(e));
            } catch (Throwable e) {
                throw new IndexShardCreationException(shardId, e);
            }

            IndexShard indexShard = shardInjector.getInstance(IndexShard.class);
            indicesLifecycle.indexShardStateChanged(indexShard, null, "shard created");
            indicesLifecycle.afterIndexShardCreated(indexShard);

            shards = newMapBuilder(shards).put(shardId.id(), new Tuple<>(indexShard, shardInjector)).immutableMap();
            success = true;
            return indexShard;
        } catch (IOException ex) {
            throw new IndexShardCreationException(shardId, ex);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(lock);
                if (shardInjector != null) {
                    IndexShard indexShard = shardInjector.getInstance(IndexShard.class);
                    closeShardInjector("initialization failed", shardId, shardInjector, indexShard);
                }
            }
        }
    }

    public synchronized void removeShard(int shardId, String reason) throws ElasticsearchException {
        final ShardId sId = new ShardId(index, shardId);
        final Injector shardInjector;
        final IndexShard indexShard;
        if (shards.containsKey(shardId) == false) {
            return;
        }
        logger.debug("[{}] closing... (reason: [{}])", shardId, reason);
        HashMap<Integer, Tuple<IndexShard, Injector>> tmpShardsMap = newHashMap(shards);
        Tuple<IndexShard, Injector> tuple = tmpShardsMap.remove(shardId);
        indexShard = tuple.v1();
        shardInjector = tuple.v2();
        shards = ImmutableMap.copyOf(tmpShardsMap);
        closeShardInjector(reason, sId, shardInjector, indexShard);
        logger.debug("[{}] closed (reason: [{}])", shardId, reason);
    }

    private void closeShardInjector(String reason, ShardId sId, Injector shardInjector, IndexShard indexShard) {
        final int shardId = sId.id();
        try {
            try {
                indicesLifecycle.beforeIndexShardClosed(sId, indexShard, indexSettings);
            } finally {
                // close everything else even if the beforeIndexShardClosed threw an exception
                for (Class<? extends Closeable> closeable : pluginsService.shardServices()) {
                    try {
                        shardInjector.getInstance(closeable).close();
                    } catch (Throwable e) {
                        logger.debug("[{}] failed to clean plugin shard service [{}]", e, shardId, closeable);
                    }
                }
                // now we can close the translog service, we need to close it before the we close the shard
                closeInjectorResource(sId, shardInjector, TranslogService.class);
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
                closeInjectorResource(sId, shardInjector,
                        MergeSchedulerProvider.class,
                        MergePolicyProvider.class,
                        IndexShardGatewayService.class,
                        Translog.class,
                        PercolatorQueriesRegistry.class);

                // call this before we close the store, so we can release resources for it
                indicesLifecycle.afterIndexShardClosed(sId, indexShard, indexSettings);
            }
        } finally {
            try {
                shardInjector.getInstance(Store.class).close();
            } catch (Throwable e) {
                logger.warn("[{}] failed to close store on shard removal (reason: [{}])", e, shardId, reason);
            }
        }
    }

    /**
     * This method gets an instance for each of the given classes passed and calls #close() on the returned instance.
     * NOTE: this method swallows all exceptions thrown from the close method of the injector and logs them as debug log
     */
    private void closeInjectorResource(ShardId shardId, Injector shardInjector, Class<? extends Closeable>... toClose) {
        for (Class<? extends Closeable> closeable : toClose) {
            try {
                final Closeable instance = shardInjector.getInstance(closeable);
                if (instance == null) {
                    throw new NullPointerException("No instance available for " + closeable.getName());
                }
                IOUtils.close(instance);
            } catch (Throwable t) {
                logger.debug("{} failed to close {}", t, shardId, Strings.toUnderscoreCase(closeable.getSimpleName()));
            }
        }
    }

    private void onShardClose(ShardLock lock, boolean ownsShard) {
        if (deleted.get()) { // we remove that shards content if this index has been deleted
            try {
                if (ownsShard) {
                    try {
                        indicesLifecycle.beforeIndexShardDeleted(lock.getShardId(), indexSettings);
                    } finally {
                        indicesServices.deleteShardStore("delete index", lock, indexSettings);
                        indicesLifecycle.afterIndexShardDeleted(lock.getShardId(), indexSettings);
                    }
                }
            } catch (IOException e) {
                indicesServices.addPendingDelete(lock.getShardId(), indexSettings);
                logger.debug("{} failed to delete shard content - scheduled a retry", e, lock.getShardId().id());
            }
        }
    }

    private class StoreCloseListener implements Store.OnClose {
        private final ShardId shardId;
        private final boolean ownsShard;

        public StoreCloseListener(ShardId shardId, boolean ownsShard) {
            this.shardId = shardId;
            this.ownsShard = ownsShard;
        }

        @Override
        public void handle(ShardLock lock) {
            assert lock.getShardId().equals(shardId) : "shard Id mismatch, expected: "  + shardId + " but got: " + lock.getShardId();
            onShardClose(lock, ownsShard);
        }
    }

    public Settings getIndexSettings() {
        return indexSettings;
    }
}
