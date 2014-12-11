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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.*;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.filter.ShardFilterCacheModule;
import org.elasticsearch.index.cache.fixedbitset.FixedBitSetFilterCache;
import org.elasticsearch.index.cache.query.ShardQueryCacheModule;
import org.elasticsearch.index.deletionpolicy.DeletionPolicyModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineModule;
import org.elasticsearch.index.engine.IndexEngine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.ShardFieldDataModule;
import org.elasticsearch.index.gateway.IndexGateway;
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
import org.elasticsearch.index.shard.IndexShardCreationException;
import org.elasticsearch.index.shard.IndexShardModule;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotModule;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreModule;
import org.elasticsearch.index.suggest.SuggestShardModule;
import org.elasticsearch.index.termvectors.ShardTermVectorModule;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogModule;
import org.elasticsearch.index.translog.TranslogService;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InternalIndicesLifecycle;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ShardsPluginsModule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
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

    private final FixedBitSetFilterCache fixedBitSetFilterCache;

    private final IndexEngine indexEngine;

    private final IndexGateway indexGateway;

    private final IndexStore indexStore;

    private final IndexSettingsService settingsService;

    private final NodeEnvironment nodeEnv;

    private volatile ImmutableMap<Integer, Injector> shardsInjectors = ImmutableMap.of();

    private volatile ImmutableMap<Integer, IndexShard> shards = ImmutableMap.of();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Inject
    public IndexService(Injector injector, Index index, @IndexSettings Settings indexSettings, NodeEnvironment nodeEnv,
                                AnalysisService analysisService, MapperService mapperService, IndexQueryParserService queryParserService,
                                SimilarityService similarityService, IndexAliasesService aliasesService, IndexCache indexCache, IndexEngine indexEngine,
                                IndexGateway indexGateway, IndexStore indexStore, IndexSettingsService settingsService, IndexFieldDataService indexFieldData,
                                FixedBitSetFilterCache fixedBitSetFilterCache) {
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
        this.indexEngine = indexEngine;
        this.indexGateway = indexGateway;
        this.indexStore = indexStore;
        this.settingsService = settingsService;
        this.fixedBitSetFilterCache = fixedBitSetFilterCache;

        this.pluginsService = injector.getInstance(PluginsService.class);
        this.indicesLifecycle = (InternalIndicesLifecycle) injector.getInstance(IndicesLifecycle.class);

        // inject workarounds for cyclic dep
        indexCache.filter().setIndexService(this);
        indexFieldData.setIndexService(this);
        fixedBitSetFilterCache.setIndexService(this);
        this.nodeEnv = nodeEnv;
    }

    public int numberOfShards() {
        return shards.size();
    }

    @Override
    public UnmodifiableIterator<IndexShard> iterator() {
        return shards.values().iterator();
    }

    public boolean hasShard(int shardId) {
        return shards.containsKey(shardId);
    }

    /**
     * Return the shard with the provided id, or null if there is no such shard.
     */
    @Nullable
    public IndexShard shard(int shardId) {
        return shards.get(shardId);
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

    public ImmutableSet<Integer> shardIds() {
        return shards.keySet();
    }

    public Injector injector() {
        return injector;
    }

    public IndexGateway gateway() {
        return indexGateway;
    }

    public IndexSettingsService settingsService() {
        return this.settingsService;
    }

    public IndexStore store() {
        return indexStore;
    }

    public IndexCache cache() {
        return indexCache;
    }

    public IndexFieldDataService fieldData() {
        return indexFieldData;
    }

    public FixedBitSetFilterCache fixedBitSetFilterCache() {
        return fixedBitSetFilterCache;
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

    public IndexEngine engine() {
        return indexEngine;
    }

    public synchronized void close(final String reason, final IndicesService.IndexCloseListener listener) {
        if (closed.compareAndSet(false, true)) {
            final Set<Integer> shardIds = shardIds();
            final IndicesService.IndexCloseListener innerListener = listener == null ? null :
                    new PerShardIndexCloseListener(shardIds, listener);
            for (final int shardId : shardIds) {
                try {
                    removeShard(shardId, reason, innerListener);
                } catch (Throwable t) {
                    logger.warn("failed to close shard", t);
                }
            }
        }
    }

    /**
     * Return the shard injector for the provided id, or null if there is no such shard.
     */
    @Nullable
    public Injector shardInjector(int shardId) throws ElasticsearchException {
        return shardsInjectors.get(shardId);
    }

    /**
     * Return the shard injector for the provided id, or throw an exception if there is no such shard.
     */
    public Injector shardInjectorSafe(int shardId) throws IndexShardMissingException {
        Injector shardInjector = shardInjector(shardId);
        if (shardInjector == null) {
            throw new IndexShardMissingException(new ShardId(index, shardId));
        }
        return shardInjector;
    }

    public String indexUUID() {
        return indexSettings.get(IndexMetaData.SETTING_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
    }

    public synchronized IndexShard createShard(int sShardId) throws ElasticsearchException {
        /*
         * TODO: we execute this in parallel but it's a synced method. Yet, we might
         * be able to serialize the execution via the cluster state in the future. for now we just
         * keep it synced.
         */
        if (closed.get()) {
            throw new ElasticsearchIllegalStateException("Can't create shard [" + index.name() + "][" + sShardId + "], closed");
        }
        ShardId shardId = new ShardId(index, sShardId);
        ShardLock lock = null;
        boolean success = false;
        try {
            lock = nodeEnv.shardLock(shardId, TimeUnit.SECONDS.toMillis(5));
            if (shardsInjectors.containsKey(shardId.id())) {
                throw new IndexShardAlreadyExistsException(shardId + " already exists");
            }

            indicesLifecycle.beforeIndexShardCreated(shardId);

            logger.debug("creating shard_id {}", shardId);

            ModulesBuilder modules = new ModulesBuilder();
            modules.add(new ShardsPluginsModule(indexSettings, pluginsService));
            modules.add(new IndexShardModule(shardId));
            modules.add(new ShardIndexingModule());
            modules.add(new ShardSearchModule());
            modules.add(new ShardGetModule());
            modules.add(new StoreModule(indexSettings, injector.getInstance(IndexStore.class), lock));
            modules.add(new DeletionPolicyModule(indexSettings));
            modules.add(new MergePolicyModule(indexSettings));
            modules.add(new MergeSchedulerModule(indexSettings));
            modules.add(new ShardFilterCacheModule());
            modules.add(new ShardQueryCacheModule());
            modules.add(new ShardFieldDataModule());
            modules.add(new TranslogModule(indexSettings));
            modules.add(new EngineModule(indexSettings));
            modules.add(new IndexShardGatewayModule(injector.getInstance(IndexGateway.class)));
            modules.add(new PercolatorShardModule());
            modules.add(new ShardTermVectorModule());
            modules.add(new IndexShardSnapshotModule());
            modules.add(new SuggestShardModule());

            Injector shardInjector;
            try {
                shardInjector = modules.createChildInjector(injector);
            } catch (CreationException e) {
                throw new IndexShardCreationException(shardId, Injectors.getFirstErrorFailure(e));
            } catch (Throwable e) {
                throw new IndexShardCreationException(shardId, e);
            }

            shardsInjectors = newMapBuilder(shardsInjectors).put(shardId.id(), shardInjector).immutableMap();

            IndexShard indexShard = shardInjector.getInstance(IndexShard.class);
            indicesLifecycle.indexShardStateChanged(indexShard, null, "shard created");
            indicesLifecycle.afterIndexShardCreated(indexShard);

            shards = newMapBuilder(shards).put(shardId.id(), indexShard).immutableMap();
            success = true;
            return indexShard;
        } catch (IOException ex) {
            throw new IndexShardCreationException(shardId, ex);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(lock);
            }
        }
    }

    public void removeShard(int shardId, String reason) throws ElasticsearchException {
        removeShard(shardId, reason, null);
    }

    public synchronized void removeShard(int shardId, String reason, @Nullable final IndicesService.IndexCloseListener listener) throws ElasticsearchException {
        boolean listenerPassed = false;
        final ShardId sId = new ShardId(index, shardId);
        try {
            final Injector shardInjector;
            final IndexShard indexShard;
            Map<Integer, Injector> tmpShardInjectors = newHashMap(shardsInjectors);
            shardInjector = tmpShardInjectors.remove(shardId);
            if (shardInjector == null) {
                return;
            }

            logger.debug("[{}] closing... (reason: [{}])", shardId, reason);
            shardsInjectors = ImmutableMap.copyOf(tmpShardInjectors);
            Map<Integer, IndexShard> tmpShardsMap = newHashMap(shards);
            indexShard = tmpShardsMap.remove(shardId);
            shards = ImmutableMap.copyOf(tmpShardsMap);
            indicesLifecycle.beforeIndexShardClosed(sId, indexShard);
            for (Class<? extends CloseableIndexComponent> closeable : pluginsService.shardServices()) {
                try {
                    shardInjector.getInstance(closeable).close();
                } catch (Throwable e) {
                    logger.debug("[{}] failed to clean plugin shard service [{}]", e, shardId, closeable);
                }
            }
            try {
                // now we can close the translog service, we need to close it before the we close the shard
                shardInjector.getInstance(TranslogService.class).close();
            } catch (Throwable e) {
                logger.debug("[{}] failed to close translog service", e, shardId);
                // ignore
            }
            // this logic is tricky, we want to close the engine so we rollback the changes done to it
            // and close the shard so no operations are allowed to it
            if (indexShard != null) {
                try {
                    ((IndexShard) indexShard).close(reason);
                } catch (Throwable e) {
                    logger.debug("[{}] failed to close index shard", e, shardId);
                    // ignore
                }
            }
            try {
                shardInjector.getInstance(Engine.class).close();
            } catch (Throwable e) {
                logger.debug("[{}] failed to close engine", e, shardId);
                // ignore
            }
            try {
                shardInjector.getInstance(MergeSchedulerProvider.class).close();
            } catch (Throwable e) {
                logger.debug("[{}] failed to close merge policy scheduler", e, shardId);
                // ignore
            }
            try {
                shardInjector.getInstance(MergePolicyProvider.class).close();
            } catch (Throwable e) {
                logger.debug("[{}] failed to close merge policy provider", e, shardId);
                // ignore
            }
            try {
                shardInjector.getInstance(IndexShardGatewayService.class).close();
            } catch (Throwable e) {
                logger.debug("[{}] failed to close index shard gateway", e, shardId);
                // ignore
            }
            try {
                // now we can close the translog
                shardInjector.getInstance(Translog.class).close();
            } catch (Throwable e) {
                logger.debug("[{}] failed to close translog", e, shardId);
                // ignore
            }
            try {
                // now we can close the translog
                shardInjector.getInstance(PercolatorQueriesRegistry.class).close();
            } catch (Throwable e) {
                logger.debug("[{}] failed to close PercolatorQueriesRegistry", e, shardId);
                // ignore
            }

            // call this before we close the store, so we can release resources for it
            indicesLifecycle.afterIndexShardClosed(sId, indexShard);
            // if we delete or have no gateway or the store is not persistent, clean the store...
            final Store store = shardInjector.getInstance(Store.class);
            // and close it
            try {
                listenerPassed = true;
                if (listener == null) {
                    store.close();
                } else {
                    store.close(new Store.OnCloseListener() {
                        @Override
                        public void onClose(ShardId shardId) {
                            listener.onShardClosed(shardId);
                        }
                    });
                }
            } catch (Throwable e) {
                logger.warn("[{}] failed to close store on shard deletion", e, shardId);
            }
            Injectors.close(injector);

            logger.debug("[{}] closed (reason: [{}])", shardId, reason);
        } catch (Throwable t) {
            if (listenerPassed == false && listener != null) { // only notify if the listener wasn't passed to the store
                listener.onShardCloseFailed(sId, t);
            }
            throw t;
        }

    }

    private static final class PerShardIndexCloseListener implements IndicesService.IndexCloseListener {
        final CountDown countDown;
        final List<Throwable> failures;
        private final Set<Integer> shardIds;
        private final IndicesService.IndexCloseListener listener;

        public PerShardIndexCloseListener(Set<Integer> shardIds, IndicesService.IndexCloseListener listener) {
            this.shardIds = shardIds;
            this.listener = listener;
            countDown = new CountDown(shardIds.size());
            failures = new CopyOnWriteArrayList<>();
        }

        @Override
        public void onAllShardsClosed(Index index, List<Throwable> failures) {
            assert false : "nobody should call this";
        }

        @Override
        public void onShardClosed(ShardId shardId) {
            assert countDown.isCountedDown() == false;
            assert shardIds.contains(shardId.getId()) : "Unknown shard id";
            listener.onShardClosed(shardId);
            if (countDown.countDown()) {
                listener.onAllShardsClosed(shardId.index(), failures);
            }
        }

        @Override
        public void onShardCloseFailed(ShardId shardId, Throwable t) {
            assert countDown.isCountedDown() == false;
            assert shardIds.contains(shardId.getId()) : "Unkown shard id";
            listener.onShardCloseFailed(shardId, t);
            failures.add(t);
            if (countDown.countDown()) {
                listener.onAllShardsClosed(shardId.index(), failures);
            }
        }
    }
}
