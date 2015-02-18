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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Injectors;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.LocalNodeIdModule;
import org.elasticsearch.index.aliases.IndexAliasesServiceModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.fielddata.IndexFieldDataModule;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.IndexStoreModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.IndexPluginsModule;
import org.elasticsearch.plugins.PluginsService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
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

    private final Injector injector;

    private final PluginsService pluginsService;
    private final NodeEnvironment nodeEnv;
    private final ClusterService clusterService;

    private volatile Map<String, Tuple<IndexService, Injector>> indices = ImmutableMap.of();

    private final OldShardsStats oldShardsStats = new OldShardsStats();

    @Inject
    public IndicesService(Settings settings, IndicesLifecycle indicesLifecycle, IndicesAnalysisService indicesAnalysisService, Injector injector, NodeEnvironment nodeEnv, ClusterService clusterService) {
        super(settings);
        this.indicesLifecycle = (InternalIndicesLifecycle) indicesLifecycle;
        this.clusterService = clusterService;
        this.indicesAnalysisService = indicesAnalysisService;
        this.injector = injector;
        this.pluginsService = injector.getInstance(PluginsService.class);
        this.indicesLifecycle.addListener(oldShardsStats);
        this.nodeEnv = nodeEnv;
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
                        removeIndex(index, "shutdown", false);
                    } catch (Throwable e) {
                        logger.warn("failed to remove index on stop [" + index + "]", e);
                    } finally {
                        latch.countDown();
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
        IOUtils.closeWhileHandlingException(injector.getInstance(RecoverySettings.class),
            indicesAnalysisService);
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
        return stats(includePrevious, new CommonStatsFlags().all());
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
        for (Tuple<IndexService, Injector> value : indices.values()) {
            IndexService indexService = value.v1();
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
    public Iterator<IndexService> iterator() {
        return Iterators.transform(indices.values().iterator(), new Function<Tuple<IndexService, Injector>, IndexService>() {
            @Override
            public IndexService apply(Tuple<IndexService, Injector> input) {
                return input.v1();
            }
        });
    }

    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    /**
     * Returns an IndexService for the specified index if exists otherwise returns <code>null</code>.
     *
     */
    @Nullable
    public IndexService indexService(String index) {
        Tuple<IndexService, Injector> indexServiceInjectorTuple = indices.get(index);
        if (indexServiceInjectorTuple == null) {
            return null;
        } else {
            return indexServiceInjectorTuple.v1();
        }
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

    public synchronized IndexService createIndex(String sIndexName, @IndexSettings Settings settings, String localNodeId) throws ElasticsearchException {
        if (!lifecycle.started()) {
            throw new ElasticsearchIllegalStateException("Can't create an index [" + sIndexName + "], node is closed");
        }
        Index index = new Index(sIndexName);
        if (indices.containsKey(index.name())) {
            throw new IndexAlreadyExistsException(index);
        }

        indicesLifecycle.beforeIndexCreated(index, settings);

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
        modules.add(new AnalysisModule(indexSettings, indicesAnalysisService));
        modules.add(new SimilarityModule(indexSettings));
        modules.add(new IndexCacheModule(indexSettings));
        modules.add(new IndexFieldDataModule(indexSettings));
        modules.add(new MapperServiceModule());
        modules.add(new IndexQueryParserModule(indexSettings));
        modules.add(new IndexAliasesServiceModule());
        modules.add(new IndexModule(indexSettings));

        Injector indexInjector;
        try {
            indexInjector = modules.createChildInjector(injector);
        } catch (CreationException e) {
            throw new IndexCreationException(index, Injectors.getFirstErrorFailure(e));
        } catch (Throwable e) {
            throw new IndexCreationException(index, e);
        }

        IndexService indexService = indexInjector.getInstance(IndexService.class);

        indicesLifecycle.afterIndexCreated(indexService);

        indices = newMapBuilder(indices).put(index.name(), new Tuple<>(indexService, indexInjector)).immutableMap();

        return indexService;
    }

    /**
     * Removes the given index from this service and releases all associated resources. Persistent parts of the index
     * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
     * @param index the index to remove
     * @param reason  the high level reason causing this removal
     */
    public void removeIndex(String index, String reason) throws ElasticsearchException {
        removeIndex(index, reason, false);
    }

    private void removeIndex(String index, String reason, boolean delete) throws ElasticsearchException {
        try {
            final IndexService indexService;
            final Injector indexInjector;
            synchronized (this) {
                if (indices.containsKey(index) == false) {
                    return;
                }

                logger.debug("[{}] closing ... (reason [{}])", index, reason);
                Map<String, Tuple<IndexService, Injector>> tmpMap = newHashMap(indices);
                Tuple<IndexService, Injector> remove = tmpMap.remove(index);
                indexService = remove.v1();
                indexInjector = remove.v2();
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
            indexService.close(reason, delete);

            logger.debug("[{}] closing index cache (reason [{}])", index, reason);
            indexInjector.getInstance(IndexCache.class).close();
            logger.debug("[{}] clearing index field data (reason [{}])", index, reason);
            indexInjector.getInstance(IndexFieldDataService.class).clear();
            logger.debug("[{}] closing analysis service (reason [{}])", index, reason);
            indexInjector.getInstance(AnalysisService.class).close();

            logger.debug("[{}] closing mapper service (reason [{}])", index, reason);
            indexInjector.getInstance(MapperService.class).close();
            logger.debug("[{}] closing index query parser service (reason [{}])", index, reason);
            indexInjector.getInstance(IndexQueryParserService.class).close();

            logger.debug("[{}] closing index service (reason [{}])", index, reason);
            indexInjector.getInstance(IndexStore.class).close();

            logger.debug("[{}] closed... (reason [{}])", index, reason);
            indicesLifecycle.afterIndexClosed(indexService.index(), indexService.settingsService().getSettings());
            if (delete) {
                final Settings indexSettings = indexService.getIndexSettings();
                indicesLifecycle.afterIndexDeleted(indexService.index(), indexSettings);
                // now we are done - try to wipe data on disk if possible
                deleteIndexStore(reason, indexService.index(), indexSettings);
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
        public synchronized void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                                        @IndexSettings Settings indexSettings) {
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
     * Deletes the given index. Persistent parts of the index
     * like the shards files, state and transaction logs are removed once all resources are released.
     *
     * Equivalent to {@link #removeIndex(String, String)} but fires
     * different lifecycle events to ensure pending resources of this index are immediately removed.
     * @param index the index to delete
     * @param reason the high level reason causing this delete
     */
    public void deleteIndex(String index, String reason) throws IOException {
        removeIndex(index, reason, true);
    }

    public void deleteClosedIndex(String reason, IndexMetaData metaData) {
        if (nodeEnv.hasNodeFile()) {
            String indexName = metaData.getIndex();
            try {
                ClusterState clusterState = clusterService.state();
                if (clusterState.metaData().hasIndex(indexName)) {
                    final IndexMetaData index = clusterState.metaData().index(indexName);
                    throw new ElasticsearchIllegalStateException("Can't delete closed index store for [" + indexName + "] - it's still part of the cluster state [" + index.getUUID() + "] [" + metaData.getUUID() + "]");
                }
                deleteIndexStore(reason, metaData);
            } catch (IOException e) {
                logger.warn("[{}] failed to delete closed index", e, metaData.index());
            }
        }
    }

    /**
     * Deletes the index store trying to acquire all shards locks for this index.
     * This method will delete the metadata for the index even if the actual shards can't be locked.
     */
    public void deleteIndexStore(String reason, IndexMetaData metaData) throws IOException {
        if (nodeEnv.hasNodeFile()) {
            synchronized (this) {
                String indexName = metaData.index();
                if (indices.containsKey(metaData.index())) {
                    String localUUid = indices.get(metaData.index()).v1().indexUUID();
                    throw new ElasticsearchIllegalStateException("Can't delete index store for [" + metaData.getIndex() + "] - it's still part of the indices service [" + localUUid+ "] [" + metaData.getUUID() + "]");
                }
                ClusterState clusterState = clusterService.state();
                if (clusterState.metaData().hasIndex(indexName)) {
                    final IndexMetaData index = clusterState.metaData().index(indexName);
                    throw new ElasticsearchIllegalStateException("Can't delete closed index store for [" + indexName + "] - it's still part of the cluster state [" + index.getUUID() + "] [" + metaData.getUUID() + "]");
                }
            }
            Index index = new Index(metaData.index());
            final Settings indexSettings = buildIndexSettings(metaData);
            deleteIndexStore(reason, index, indexSettings);
        }
    }

    private void deleteIndexStore(String reason, Index index, Settings indexSettings) throws IOException {
        try {
            // we are trying to delete the index store here - not a big deal if the lock can't be obtained
            // the store metadata gets wiped anyway even without the lock this is just best effort since
            // every shards deletes its content under the shard lock it owns.
            logger.debug("{} deleting index store reason [{}]", index, reason);
            if (canDeleteIndexContents(index, indexSettings)) {
                nodeEnv.deleteIndexDirectorySafe(index, 0, indexSettings);
            }
        } catch (LockObtainFailedException ex) {
            logger.debug("{} failed to delete index store - at least one shards is still locked", ex, index);
        } catch (Exception ex) {
            logger.warn("{} failed to delete index", ex, index);
        } finally {
            // this is a pure protection to make sure this index doesn't get re-imported as a dangeling index.
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
    public void deleteShardStore(String reason, ShardLock lock, Settings indexSettings) throws IOException {
        ShardId shardId = lock.getShardId();
        logger.trace("{} deleting shard reason [{}]", shardId, reason);
        nodeEnv.deleteShardDirectoryUnderLock(lock, indexSettings);
    }

    /**
     * This method deletes the shard contents on disk for the given shard ID. This method will fail if the shard deleting
     * is prevented by {@link #canDeleteShardContent(org.elasticsearch.index.shard.ShardId, org.elasticsearch.cluster.metadata.IndexMetaData)}
     * of if the shards lock can not be acquired.
     * @param reason the reason for the shard deletion
     * @param shardId the shards ID to delete
     * @param metaData the shards index metadata. This is required to access the indexes settings etc.
     * @throws IOException if an IOException occurs
     */
    public void deleteShardStore(String reason, ShardId shardId, IndexMetaData metaData) throws IOException {
        final Settings indexSettings = buildIndexSettings(metaData);
        if (canDeleteShardContent(shardId, indexSettings) == false) {
            throw new ElasticsearchIllegalStateException("Can't delete shard " + shardId);
        }
        nodeEnv.deleteShardDirectorySafe(shardId, indexSettings);
        logger.trace("{} deleting shard reason [{}]", shardId, reason);
    }

    /**
     * This method returns true if the current node is allowed to delete the
     * given index. If the index uses a shared filesystem this method always
     * returns false.
     * @param index {@code Index} to check whether deletion is allowed
     * @param indexSettings {@code Settings} for the given index
     * @return true if the index can be deleted on this node
     */
    public boolean canDeleteIndexContents(Index index, Settings indexSettings) {
        final Tuple<IndexService, Injector> indexServiceInjectorTuple = this.indices.get(index.name());
        if (IndexMetaData.isOnSharedFilesystem(indexSettings) == false) {
            if (indexServiceInjectorTuple == null && nodeEnv.hasNodeFile()) {
                return true;
            }
        } else {
            logger.trace("{} skipping index directory deletion due to shadow replicas", index);
        }
        return false;
    }

    /**
     * Returns <code>true</code> iff the shards content for the given shard can be deleted.
     * This method will return <code>false</code> if:
     * <ul>
     *     <li>if the shard is still allocated / active on this node</li>
     *     <li>if for instance if the shard is located on shared and should not be deleted</li>
     *     <li>if the shards data locations do not exists</li>
     * </ul>
     *
     * @param shardId the shard to delete.
     * @param metaData the shards index metadata. This is required to access the indexes settings etc.
     */
    public boolean canDeleteShardContent(ShardId shardId, IndexMetaData metaData) {
        // we need the metadata here since we have to build the complete settings
        // to decide where the shard content lives. In the future we might even need more info here ie. for shadow replicas
        // The plan was to make it harder to miss-use and ask for metadata instead of simple settings
        assert shardId.getIndex().equals(metaData.getIndex());
        final Settings indexSettings = buildIndexSettings(metaData);
        return canDeleteShardContent(shardId, indexSettings);
    }

    private boolean canDeleteShardContent(ShardId shardId, @IndexSettings Settings indexSettings) {
        final Tuple<IndexService, Injector> indexServiceInjectorTuple = this.indices.get(shardId.getIndex());
        if (IndexMetaData.isOnSharedFilesystem(indexSettings) == false) {
            if (indexServiceInjectorTuple != null && nodeEnv.hasNodeFile()) {
                final IndexService indexService = indexServiceInjectorTuple.v1();
                return indexService.hasShard(shardId.id()) == false;
            } else if (nodeEnv.hasNodeFile()) {
                final Path[] shardLocations = nodeEnv.shardDataPaths(shardId, indexSettings);
                return FileSystemUtils.exists(shardLocations);
            }
        } else {
            logger.trace("{} skipping shard directory deletion due to shadow replicas", shardId);
        }
        return false;
    }

    private Settings buildIndexSettings(IndexMetaData metaData) {
        // play safe here and make sure that we take node level settings into account.
        // we might run on nodes where we use shard FS and then in the future don't delete
        // actual content.
        ImmutableSettings.Builder builder = settingsBuilder();
        builder.put(settings);
        builder.put(metaData.getSettings());
        return builder.build();
    }
}