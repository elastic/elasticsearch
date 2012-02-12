/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.*;
import org.elasticsearch.index.aliases.IndexAliasesServiceModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.CacheStats;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.engine.IndexEngine;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.IndexGatewayModule;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.percolator.PercolatorModule;
import org.elasticsearch.index.percolator.PercolatorService;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.index.store.IndexStoreModule;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.plugins.IndexPluginsModule;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 *
 */
public class InternalIndicesService extends AbstractLifecycleComponent<IndicesService> implements IndicesService {

    private final NodeEnvironment nodeEnv;

    private final ThreadPool threadPool;

    private final InternalIndicesLifecycle indicesLifecycle;

    private final IndicesAnalysisService indicesAnalysisService;

    private final IndicesStore indicesStore;

    private final Injector injector;

    private final PluginsService pluginsService;

    private final Map<String, Injector> indicesInjectors = new HashMap<String, Injector>();

    private volatile ImmutableMap<String, IndexService> indices = ImmutableMap.of();

    private final OldShardsStats oldShardsStats = new OldShardsStats();

    @Inject
    public InternalIndicesService(Settings settings, NodeEnvironment nodeEnv, ThreadPool threadPool, IndicesLifecycle indicesLifecycle, IndicesAnalysisService indicesAnalysisService, IndicesStore indicesStore, Injector injector) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.threadPool = threadPool;
        this.indicesLifecycle = (InternalIndicesLifecycle) indicesLifecycle;
        this.indicesAnalysisService = indicesAnalysisService;
        this.indicesStore = indicesStore;
        this.injector = injector;

        this.pluginsService = injector.getInstance(PluginsService.class);

        this.indicesLifecycle.addListener(oldShardsStats);
    }

    @Override
    protected void doStart() throws ElasticSearchException {
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        ImmutableSet<String> indices = ImmutableSet.copyOf(this.indices.keySet());
        final CountDownLatch latch = new CountDownLatch(indices.size());

        final ExecutorService indicesStopExecutor = Executors.newFixedThreadPool(5, EsExecutors.daemonThreadFactory("indices_shutdown"));
        final ExecutorService shardsStopExecutor = Executors.newFixedThreadPool(5, EsExecutors.daemonThreadFactory("shards_shutdown"));

        for (final String index : indices) {
            indicesStopExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        deleteIndex(index, false, "shutdown", shardsStopExecutor);
                    } catch (Exception e) {
                        logger.warn("failed to delete index on stop [" + index + "]", e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            // ignore
        } finally {
            shardsStopExecutor.shutdown();
            indicesStopExecutor.shutdown();
        }
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        injector.getInstance(RecoverySettings.class).close();
        indicesStore.close();
        indicesAnalysisService.close();
    }

    @Override
    public IndicesLifecycle indicesLifecycle() {
        return this.indicesLifecycle;
    }

    @Override
    public NodeIndicesStats stats(boolean includePrevious) {
        DocsStats docsStats = new DocsStats();
        StoreStats storeStats = new StoreStats();
        IndexingStats indexingStats = new IndexingStats();
        GetStats getStats = new GetStats();
        SearchStats searchStats = new SearchStats();
        CacheStats cacheStats = new CacheStats();
        MergeStats mergeStats = new MergeStats();
        RefreshStats refreshStats = new RefreshStats();
        FlushStats flushStats = new FlushStats();

        if (includePrevious) {
            getStats.add(oldShardsStats.getStats);
            indexingStats.add(oldShardsStats.indexingStats);
            searchStats.add(oldShardsStats.searchStats);
            mergeStats.add(oldShardsStats.mergeStats);
            refreshStats.add(oldShardsStats.refreshStats);
            flushStats.add(oldShardsStats.flushStats);
        }

        for (IndexService indexService : indices.values()) {
            for (IndexShard indexShard : indexService) {
                storeStats.add(indexShard.storeStats());
                docsStats.add(indexShard.docStats());
                getStats.add(indexShard.getStats());
                indexingStats.add(indexShard.indexingStats());
                searchStats.add(indexShard.searchStats());
                mergeStats.add(indexShard.mergeStats());
                refreshStats.add(indexShard.refreshStats());
                flushStats.add(indexShard.flushStats());
            }
            cacheStats.add(indexService.cache().stats());
        }
        return new NodeIndicesStats(storeStats, docsStats, indexingStats, getStats, searchStats, cacheStats, mergeStats, refreshStats, flushStats);
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

    public Set<String> indices() {
        return newHashSet(indices.keySet());
    }

    public IndexService indexService(String index) {
        return indices.get(index);
    }

    @Override
    public IndexService indexServiceSafe(String index) throws IndexMissingException {
        IndexService indexService = indexService(index);
        if (indexService == null) {
            throw new IndexMissingException(new Index(index));
        }
        return indexService;
    }

    public synchronized IndexService createIndex(String sIndexName, Settings settings, String localNodeId) throws ElasticSearchException {
        if (!lifecycle.started()) {
            throw new ElasticSearchIllegalStateException("Can't create an index [" + sIndexName + "], node is closed");
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
        modules.add(new IndexQueryParserModule(indexSettings));
        modules.add(new MapperServiceModule());
        modules.add(new IndexAliasesServiceModule());
        modules.add(new IndexGatewayModule(indexSettings, injector.getInstance(Gateway.class)));
        modules.add(new IndexModule(indexSettings));
        modules.add(new PercolatorModule());

        Injector indexInjector;
        try {
            indexInjector = modules.createChildInjector(injector);
        } catch (CreationException e) {
            throw new IndexCreationException(index, Injectors.getFirstErrorFailure(e));
        }

        indicesInjectors.put(index.name(), indexInjector);

        IndexService indexService = indexInjector.getInstance(IndexService.class);

        indicesLifecycle.afterIndexCreated(indexService);

        indices = newMapBuilder(indices).put(index.name(), indexService).immutableMap();

        return indexService;
    }

    @Override
    public synchronized void cleanIndex(String index, String reason) throws ElasticSearchException {
        deleteIndex(index, false, reason, null);
    }

    @Override
    public synchronized void deleteIndex(String index, String reason) throws ElasticSearchException {
        deleteIndex(index, true, reason, null);
    }

    private void deleteIndex(String index, boolean delete, String reason, @Nullable Executor executor) throws ElasticSearchException {
        Injector indexInjector;
        IndexService indexService;
        synchronized (this) {
            indexInjector = indicesInjectors.remove(index);
            if (indexInjector == null) {
                if (!delete) {
                    return;
                }
                throw new IndexMissingException(new Index(index));
            }
            if (delete) {
                logger.debug("deleting Index [{}]", index);
            }

            Map<String, IndexService> tmpMap = newHashMap(indices);
            indexService = tmpMap.remove(index);
            indices = ImmutableMap.copyOf(tmpMap);
        }

        indicesLifecycle.beforeIndexClosed(indexService, delete);

        for (Class<? extends CloseableIndexComponent> closeable : pluginsService.indexServices()) {
            indexInjector.getInstance(closeable).close(delete);
        }

        ((InternalIndexService) indexService).close(delete, reason, executor);

        indexInjector.getInstance(PercolatorService.class).close();
        indexInjector.getInstance(IndexCache.class).close();
        indexInjector.getInstance(AnalysisService.class).close();
        indexInjector.getInstance(IndexEngine.class).close();
        indexInjector.getInstance(IndexServiceManagement.class).close();

        indexInjector.getInstance(IndexGateway.class).close(delete);
        indexInjector.getInstance(MapperService.class).close();
        indexInjector.getInstance(IndexQueryParserService.class).close();

        Injectors.close(injector);

        indicesLifecycle.afterIndexClosed(indexService.index(), delete);

        if (delete) {
            FileSystemUtils.deleteRecursively(nodeEnv.indexLocations(new Index(index)));
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
        public synchronized void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, boolean delete) {
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
}