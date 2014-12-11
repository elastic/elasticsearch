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

import com.google.common.base.Function;
import com.google.common.collect.*;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.*;
import org.elasticsearch.index.aliases.IndexAliasesServiceModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.codec.CodecModule;
import org.elasticsearch.index.engine.IndexEngine;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.fielddata.IndexFieldDataModule;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.IndexGatewayModule;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.IndexStoreModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.plugins.IndexPluginsModule;
import org.elasticsearch.plugins.PluginsService;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 *
 */
public class IndicesService extends AbstractLifecycleComponent<IndicesService> implements Iterable<IndexService> {

    private final InternalIndicesLifecycle indicesLifecycle;

    private final IndicesAnalysisService indicesAnalysisService;

    private IndicesStore indicesStore;

    private final Injector injector;

    private final PluginsService pluginsService;

    private final NodeEnvironment nodeEnv;

    private final Map<String, Injector> indicesInjectors = new HashMap<>();

    private volatile ImmutableMap<String, IndexService> indices = ImmutableMap.of();

    private final OldShardsStats oldShardsStats = new OldShardsStats();

    @Inject
    public IndicesService(Settings settings, IndicesLifecycle indicesLifecycle, IndicesAnalysisService indicesAnalysisService, Injector injector, NodeEnvironment nodeEnv) {
        super(settings);
        this.indicesLifecycle = (InternalIndicesLifecycle) indicesLifecycle;
        this.indicesAnalysisService = indicesAnalysisService;
        this.injector = injector;

        this.pluginsService = injector.getInstance(PluginsService.class);

        this.indicesLifecycle.addListener(oldShardsStats);
        this.nodeEnv = nodeEnv;
    }

    @Inject
    void setIndicesStore(IndicesStore store) {
        this.indicesStore = store;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        ImmutableSet<String> indices = ImmutableSet.copyOf(this.indices.keySet());
        final CountDownLatch latch = new CountDownLatch(indices.size());

        final ExecutorService indicesStopExecutor = Executors.newFixedThreadPool(5, EsExecutors.daemonThreadFactory("indices_shutdown"));

        for (final String index : indices) {
            indicesStopExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        removeIndex(index, "shutdown", false, new IndexCloseListener() {
                            @Override
                            public void onAllShardsClosed(Index index, List<Throwable> failures) {
                                latch.countDown();
                            }

                            @Override
                            public void onShardClosed(ShardId shardId) {
                            }

                            @Override
                            public void onShardCloseFailed(ShardId shardId, Throwable t) {
                            }
                        });
                    } catch (Throwable e) {
                        latch.countDown();
                        logger.warn("failed to delete index on stop [" + index + "]", e);
                    }
                }
            });
        }
        try {
            if (latch.await(30, TimeUnit.SECONDS) == false) {
              logger.warn("Not all shards are closed yet, waited 30sec - stopping service");
            }
        } catch (InterruptedException e) {
            // ignore
        } finally {
            indicesStopExecutor.shutdown();
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        injector.getInstance(RecoverySettings.class).close();
        indicesStore.close();
        indicesAnalysisService.close();
    }

    public IndicesLifecycle indicesLifecycle() {
        return this.indicesLifecycle;
    }

    /**
     * Returns the node stats indices stats. The <tt>includePrevious</tt> flag controls
     * if old shards stats will be aggregated as well (only for relevant stats, such as
     * refresh and indexing, not for docs/store).
     */
    public NodeIndicesStats stats(boolean includePrevious) {
        return stats(true, new CommonStatsFlags().all());
    }

    public NodeIndicesStats stats(boolean includePrevious, CommonStatsFlags flags) {
        CommonStats oldStats = new CommonStats(flags);

        if (includePrevious) {
            Flag[] setFlags = flags.getFlags();
            for (Flag flag : setFlags) {
                switch (flag) {
                    case Get:
                        oldStats.get.add(oldShardsStats.getStats);
                        break;
                    case Indexing:
                        oldStats.indexing.add(oldShardsStats.indexingStats);
                        break;
                    case Search:
                        oldStats.search.add(oldShardsStats.searchStats);
                        break;
                    case Merge:
                        oldStats.merge.add(oldShardsStats.mergeStats);
                        break;
                    case Refresh:
                        oldStats.refresh.add(oldShardsStats.refreshStats);
                        break;
                    case Flush:
                        oldStats.flush.add(oldShardsStats.flushStats);
                        break;
                }
            }
        }

        Map<Index, List<IndexShardStats>> statsByShard = Maps.newHashMap();
        for (IndexService indexService : indices.values()) {
            for (IndexShard indexShard : indexService) {
                try {
                    if (indexShard.routingEntry() == null) {
                        continue;
                    }
                    IndexShardStats indexShardStats = new IndexShardStats(indexShard.shardId(), new ShardStats[] { new ShardStats(indexShard, indexShard.routingEntry(), flags) });
                    if (!statsByShard.containsKey(indexService.index())) {
                        statsByShard.put(indexService.index(), Lists.<IndexShardStats>newArrayList(indexShardStats));
                    } else {
                        statsByShard.get(indexService.index()).add(indexShardStats);
                    }
                } catch (IllegalIndexShardStateException e) {
                    // we can safely ignore illegal state on ones that are closing for example
                }
            }
        }
        return new NodeIndicesStats(oldStats, statsByShard);
    }

    /**
     * Returns <tt>true</tt> if changes (adding / removing) indices, shards and so on are allowed.
     */
    public boolean changesAllowed() {
        // we check on stop here since we defined stop when we delete the indices
        return lifecycle.started();
    }

    @Override
    public UnmodifiableIterator<IndexService> iterator() {
        return indices.values().iterator();
    }

    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    /**
     * Returns a snapshot of the started indices and the associated {@link IndexService} instances.
     *
     * The map being returned is not a live view and subsequent calls can return a different view.
     */
    public ImmutableMap<String, IndexService> indices() {
        return indices;
    }

    /**
     * Returns an IndexService for the specified index if exists otherwise returns <code>null</code>.
     *
     * Even if the index name appeared in {@link #indices()} <code>null</code> can still be returned as an
     * index maybe removed in the meantime, so preferable use the associated {@link IndexService} in order to prevent NPE.
     */
    @Nullable
    public IndexService indexService(String index) {
        return indices.get(index);
    }

    /**
     * Returns an IndexService for the specified index if exists otherwise a {@link IndexMissingException} is thrown.
     */
    public IndexService indexServiceSafe(String index) throws IndexMissingException {
        IndexService indexService = indexService(index);
        if (indexService == null) {
            throw new IndexMissingException(new Index(index));
        }
        return indexService;
    }

    public synchronized IndexService createIndex(String sIndexName, Settings settings, String localNodeId) throws ElasticsearchException {
        if (!lifecycle.started()) {
            throw new ElasticsearchIllegalStateException("Can't create an index [" + sIndexName + "], node is closed");
        }
        Index index = new Index(sIndexName);
        if (indicesInjectors.containsKey(index.name())) {
            throw new IndexAlreadyExistsException(index);
        }

        indicesLifecycle.beforeIndexCreated(index);

        logger.debug("creating Index [{}], shards [{}]/[{}]", sIndexName, settings.get(SETTING_NUMBER_OF_SHARDS), settings.get(SETTING_NUMBER_OF_REPLICAS));

        Settings indexSettings = settingsBuilder()
                .put(this.settings)
                .put(settings)
                .classLoader(settings.getClassLoader())
                .build();

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new IndexNameModule(index));
        modules.add(new LocalNodeIdModule(localNodeId));
        modules.add(new IndexSettingsModule(index, indexSettings));
        modules.add(new IndexPluginsModule(indexSettings, pluginsService));
        modules.add(new IndexStoreModule(indexSettings));
        modules.add(new IndexEngineModule(indexSettings));
        modules.add(new AnalysisModule(indexSettings, indicesAnalysisService));
        modules.add(new SimilarityModule(indexSettings));
        modules.add(new IndexCacheModule(indexSettings));
        modules.add(new IndexFieldDataModule(indexSettings));
        modules.add(new CodecModule(indexSettings));
        modules.add(new MapperServiceModule());
        modules.add(new IndexQueryParserModule(indexSettings));
        modules.add(new IndexAliasesServiceModule());
        modules.add(new IndexGatewayModule(indexSettings, injector.getInstance(Gateway.class)));
        modules.add(new IndexModule(indexSettings));

        Injector indexInjector;
        try {
            indexInjector = modules.createChildInjector(injector);
        } catch (CreationException e) {
            throw new IndexCreationException(index, Injectors.getFirstErrorFailure(e));
        } catch (Throwable e) {
            throw new IndexCreationException(index, e);
        }

        indicesInjectors.put(index.name(), indexInjector);

        IndexService indexService = indexInjector.getInstance(IndexService.class);

        indicesLifecycle.afterIndexCreated(indexService);

        indices = newMapBuilder(indices).put(index.name(), indexService).immutableMap();

        return indexService;
    }

    /**
     * Removes the given index from this service and releases all associated resources. Persistent parts of the index
     * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
     * @param index the index to remove
     * @param reason  the high level reason causing this removal
     */
    public void removeIndex(String index, String reason) throws ElasticsearchException {
        removeIndex(index, reason, false, null);
    }

    /**
     * Deletes the given index. Persistent parts of the index
     * like the shards files, state and transaction logs are removed once all resources are released.
     *
     * Equivalent to {@link #removeIndex(String, String)} but fires
     * different lifecycle events to ensure pending resources of this index are immediately removed.
     * @param index the index to delete
     * @param reason the high level reason causing this delete
     */
    public void deleteIndex(String index, String reason) throws ElasticsearchException {
        removeIndex(index, reason, true, new IndexCloseListener() {

            @Override
            public void onAllShardsClosed(Index index, List<Throwable> failures) {
                try {
                    nodeEnv.deleteIndexDirectorySafe(index);
                    logger.debug("deleted index [{}] from filesystem - failures {}", index, failures);
                } catch (Exception e) {
                    for (Throwable t : failures) {
                        e.addSuppressed(t);
                    }
                    logger.debug("failed to deleted index [{}] from filesystem", e, index);
                    // ignore - still some shards locked here
                }
            }

            @Override
            public void onShardClosed(ShardId shardId) {
                try {
                    // this is called under the shard lock - we can safely delete it
                    IOUtils.rm(nodeEnv.shardPaths(shardId));
                    logger.debug("deleted shard [{}] from filesystem", shardId);
                } catch (IOException e) {
                    logger.warn("Can't delete shard {} ", e, shardId);
                }
            }

            @Override
            public void onShardCloseFailed(ShardId shardId, Throwable t) {
            }
        });
    }

    private void removeIndex(String index, String reason, boolean delete, @Nullable  IndexCloseListener listener) throws ElasticsearchException {
        try {
            final IndexService indexService;
            final Injector indexInjector;
            synchronized (this) {
                indexInjector = indicesInjectors.remove(index);
                if (indexInjector == null) {
                    return;
                }

                logger.debug("[{}] closing ... (reason [{}])", index, reason);
                Map<String, IndexService> tmpMap = newHashMap(indices);
                indexService = tmpMap.remove(index);
                indices = ImmutableMap.copyOf(tmpMap);
            }

            indicesLifecycle.beforeIndexClosed(indexService);
            if (delete) {
                indicesLifecycle.beforeIndexDeleted(indexService);
            }
            IOUtils.close(Iterables.transform(pluginsService.indexServices(), new Function<Class<? extends Closeable>, Closeable>() {
                @Override
                public Closeable apply(Class<? extends Closeable> input) {
                    return indexInjector.getInstance(input);
                }
            }));

            logger.debug("[{}] closing index service (reason [{}])", index, reason);
            ((IndexService) indexService).close(reason, listener);

            logger.debug("[{}] closing index cache (reason [{}])", index, reason);
            indexInjector.getInstance(IndexCache.class).close();
            logger.debug("[{}] clearing index field data (reason [{}])", index, reason);
            indexInjector.getInstance(IndexFieldDataService.class).clear();
            logger.debug("[{}] closing analysis service (reason [{}])", index, reason);
            indexInjector.getInstance(AnalysisService.class).close();
            logger.debug("[{}] closing index engine (reason [{}])", index, reason);
            indexInjector.getInstance(IndexEngine.class).close();

            logger.debug("[{}] closing index gateway (reason [{}])", index, reason);
            indexInjector.getInstance(IndexGateway.class).close();
            logger.debug("[{}] closing mapper service (reason [{}])", index, reason);
            indexInjector.getInstance(MapperService.class).close();
            logger.debug("[{}] closing index query parser service (reason [{}])", index, reason);
            indexInjector.getInstance(IndexQueryParserService.class).close();

            logger.debug("[{}] closing index service (reason [{}])", index, reason);
            indexInjector.getInstance(IndexStore.class).close();

            Injectors.close(injector);

            logger.debug("[{}] closed... (reason [{}])", index, reason);
            indicesLifecycle.afterIndexClosed(indexService.index());
            if (delete) {
                indicesLifecycle.afterIndexDeleted(indexService.index());
            }
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to remove index " + index, ex);
        }
    }

    static class OldShardsStats extends IndicesLifecycle.Listener {

        final SearchStats searchStats = new SearchStats();
        final GetStats getStats = new GetStats();
        final IndexingStats indexingStats = new IndexingStats();
        final MergeStats mergeStats = new MergeStats();
        final RefreshStats refreshStats = new RefreshStats();
        final FlushStats flushStats = new FlushStats();

        @Override
        public synchronized void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard) {
            if (indexShard != null) {
                getStats.add(indexShard.getStats());
                indexingStats.add(indexShard.indexingStats(), false);
                searchStats.add(indexShard.searchStats(), false);
                mergeStats.add(indexShard.mergeStats());
                refreshStats.add(indexShard.refreshStats());
                flushStats.add(indexShard.flushStats());
            }
        }
    }

    /**
     * A listener interface that can be used to get notification once a shard or all shards
     * of an certain index that are allocated on a node are actually closed. The listener methods
     * are invoked once the actual low level instance modifying or reading a shard are closed in contrast to
     * removal methods that might return earlier.
     */
    public static interface IndexCloseListener {

        /**
         * Invoked once all shards are closed or their closing failed.
         * @param index the index that got closed
         * @param failures the recorded shard closing failures
         */
        public void onAllShardsClosed(Index index, List<Throwable> failures);

        /**
         * Invoked once the last resource using the given shard ID is released.
         * Yet, this method is called while still holding the shards lock such that
         * operations on the shards data can safely be executed in this callback.
         */
        public void onShardClosed(ShardId shardId);

        /**
         * Invoked if closing the given shard failed.
         */
        public void onShardCloseFailed(ShardId shardId, Throwable t);

    }
}