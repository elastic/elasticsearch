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

import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.*;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.plugins.PluginsService;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;
import static org.elasticsearch.common.util.CollectionUtils.arrayAsArrayList;

/**
 *
 */
public class IndicesService extends AbstractLifecycleComponent<IndicesService> implements Iterable<IndexService>, IndexService.ShardStoreDeleter {

    public static final String INDICES_SHARDS_CLOSED_TIMEOUT = "indices.shards_closed_timeout";
    private final PluginsService pluginsService;
    private final NodeEnvironment nodeEnv;
    private final TimeValue shardsClosedTimeout;
    private final AnalysisRegistry analysisRegistry;
    private final IndicesQueriesRegistry indicesQueriesRegistry;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private volatile Map<String, IndexService> indices = emptyMap();
    private final Map<Index, List<PendingDelete>> pendingDeletes = new HashMap<>();
    private final OldShardsStats oldShardsStats = new OldShardsStats();
    private final IndexStoreConfig indexStoreConfig;

    @Override
    protected void doStart() {
    }

    @Inject
    public IndicesService(Settings settings, PluginsService pluginsService, NodeEnvironment nodeEnv, NodeSettingsService nodeSettingsService, AnalysisRegistry analysisRegistry, IndicesQueriesRegistry indicesQueriesRegistry, IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService) {
        super(settings);
        this.pluginsService = pluginsService;
        this.nodeEnv = nodeEnv;
        this.shardsClosedTimeout = settings.getAsTime(INDICES_SHARDS_CLOSED_TIMEOUT, new TimeValue(1, TimeUnit.DAYS));
        this.indexStoreConfig = new IndexStoreConfig(settings);
        this.analysisRegistry = analysisRegistry;
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        nodeSettingsService.addListener(indexStoreConfig);
    }

    @Override
    protected void doStop() {
        ExecutorService indicesStopExecutor = Executors.newFixedThreadPool(5, EsExecutors.daemonThreadFactory("indices_shutdown"));

        // Copy indices because we modify it asynchronously in the body of the loop
        Set<String> indices = new HashSet<>(this.indices.keySet());
        final CountDownLatch latch = new CountDownLatch(indices.size());
        for (final String index : indices) {
            indicesStopExecutor.execute(() -> {
                try {
                    removeIndex(index, "shutdown", false);
                } catch (Throwable e) {
                    logger.warn("failed to remove index on stop [" + index + "]", e);
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
    protected void doClose() {
        IOUtils.closeWhileHandlingException(analysisRegistry);
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
                    case Recovery:
                        oldStats.recoveryStats.add(oldShardsStats.recoveryStats);
                        break;
                    case Flush:
                        oldStats.flush.add(oldShardsStats.flushStats);
                        break;
                }
            }
        }

        Map<Index, List<IndexShardStats>> statsByShard = new HashMap<>();
        for (IndexService indexService : indices.values()) {
            for (IndexShard indexShard : indexService) {
                try {
                    if (indexShard.routingEntry() == null) {
                        continue;
                    }
                    IndexShardStats indexShardStats = new IndexShardStats(indexShard.shardId(), new ShardStats[] { new ShardStats(indexShard.routingEntry(), indexShard.shardPath(), new CommonStats(indexShard, flags), indexShard.commitStats()) });
                    if (!statsByShard.containsKey(indexService.index())) {
                        statsByShard.put(indexService.index(), arrayAsArrayList(indexShardStats));
                    } else {
                        statsByShard.get(indexService.index()).add(indexShardStats);
                    }
                } catch (IllegalIndexShardStateException e) {
                    // we can safely ignore illegal state on ones that are closing for example
                    logger.trace("{} ignoring shard stats", e, indexShard.shardId());
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
        return indices.values().iterator();
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
        return indices.get(index);
    }

    /**
     * Returns an IndexService for the specified index if exists otherwise a {@link IndexNotFoundException} is thrown.
     */
    public IndexService indexServiceSafe(String index) {
        IndexService indexService = indexService(index);
        if (indexService == null) {
            throw new IndexNotFoundException(index);
        }
        return indexService;
    }



    /**
     * Creates a new {@link IndexService} for the given metadata.
     * @param indexMetaData the index metadata to create the index for
     * @param builtInListeners a list of built-in lifecycle {@link IndexEventListener} that should should be used along side with the per-index listeners
     * @throws IndexAlreadyExistsException if the index already exists.
     */
    public synchronized IndexService createIndex(final NodeServicesProvider nodeServicesProvider, IndexMetaData indexMetaData, List<IndexEventListener> builtInListeners) throws IOException {
        if (!lifecycle.started()) {
            throw new IllegalStateException("Can't create an index [" + indexMetaData.getIndex() + "], node is closed");
        }
        final String indexName = indexMetaData.getIndex();
        final Predicate<String> indexNameMatcher = (indexExpression) -> indexNameExpressionResolver.matchesIndex(indexName, indexExpression, clusterService.state());
        final IndexSettings idxSettings = new IndexSettings(indexMetaData, this.settings, Collections.EMPTY_LIST, indexNameMatcher);
        Index index = new Index(indexMetaData.getIndex());
        if (indices.containsKey(index.name())) {
            throw new IndexAlreadyExistsException(index);
        }
        logger.debug("creating Index [{}], shards [{}]/[{}{}]",
                indexMetaData.getIndex(),
                idxSettings.getNumberOfShards(),
                idxSettings.getNumberOfReplicas(),
                idxSettings.isShadowReplicaIndex() ? "s" : "");

        final IndexModule indexModule = new IndexModule(idxSettings, indexStoreConfig, analysisRegistry);
        pluginsService.onIndexModule(indexModule);
        for (IndexEventListener listener : builtInListeners) {
            indexModule.addIndexEventListener(listener);
        }
        indexModule.addIndexEventListener(oldShardsStats);
        final IndexEventListener listener = indexModule.freeze();
        listener.beforeIndexCreated(index, idxSettings.getSettings());
        final IndexService indexService = indexModule.newIndexService(nodeEnv, this, nodeServicesProvider);
        boolean success = false;
        try {
            assert indexService.getIndexEventListener() == listener;
            listener.afterIndexCreated(indexService);
            indices = newMapBuilder(indices).put(index.name(), indexService).immutableMap();
            success = true;
            return indexService;
        } finally {
            if (success == false) {
                indexService.close("plugins_failed", true);
            }
        }

    }

    /**
     * Removes the given index from this service and releases all associated resources. Persistent parts of the index
     * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
     * @param index the index to remove
     * @param reason  the high level reason causing this removal
     */
    public void removeIndex(String index, String reason) {
        removeIndex(index, reason, false);
    }

    private void removeIndex(String index, String reason, boolean delete) {
        try {
            final IndexService indexService;
            final IndexEventListener listener;
            synchronized (this) {
                if (indices.containsKey(index) == false) {
                    return;
                }

                logger.debug("[{}] closing ... (reason [{}])", index, reason);
                Map<String, IndexService> newIndices = new HashMap<>(indices);
                indexService = newIndices.remove(index);
                indices = unmodifiableMap(newIndices);
                listener = indexService.getIndexEventListener();
            }

            listener.beforeIndexClosed(indexService);
            if (delete) {
                listener.beforeIndexDeleted(indexService);
            }
            logger.debug("[{}] closing index service (reason [{}])", index, reason);
            indexService.close(reason, delete);
            logger.debug("[{}] closed... (reason [{}])", index, reason);
            listener.afterIndexClosed(indexService.index(), indexService.getIndexSettings().getSettings());
            if (delete) {
                final IndexSettings indexSettings = indexService.getIndexSettings();
                listener.afterIndexDeleted(indexService.index(), indexSettings.getSettings());
                // now we are done - try to wipe data on disk if possible
                deleteIndexStore(reason, indexService.index(), indexSettings, false);
            }
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to remove index " + index, ex);
        }
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
                searchStats.addTotals(indexShard.searchStats());
                mergeStats.addTotals(indexShard.mergeStats());
                refreshStats.addTotals(indexShard.refreshStats());
                flushStats.addTotals(indexShard.flushStats());
                recoveryStats.addTotals(indexShard.recoveryStats());
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

    public void deleteClosedIndex(String reason, IndexMetaData metaData, ClusterState clusterState) {
        if (nodeEnv.hasNodeFile()) {
            String indexName = metaData.getIndex();
            try {
                if (clusterState.metaData().hasIndex(indexName)) {
                    final IndexMetaData index = clusterState.metaData().index(indexName);
                    throw new IllegalStateException("Can't delete closed index store for [" + indexName + "] - it's still part of the cluster state [" + index.getIndexUUID() + "] [" + metaData.getIndexUUID() + "]");
                }
                deleteIndexStore(reason, metaData, clusterState, true);
            } catch (IOException e) {
                logger.warn("[{}] failed to delete closed index", e, metaData.getIndex());
            }
        }
    }

    /**
     * Deletes the index store trying to acquire all shards locks for this index.
     * This method will delete the metadata for the index even if the actual shards can't be locked.
     */
    public void deleteIndexStore(String reason, IndexMetaData metaData, ClusterState clusterState, boolean closed) throws IOException {
        if (nodeEnv.hasNodeFile()) {
            synchronized (this) {
                String indexName = metaData.getIndex();
                if (indices.containsKey(indexName)) {
                    String localUUid = indices.get(indexName).indexUUID();
                    throw new IllegalStateException("Can't delete index store for [" + indexName + "] - it's still part of the indices service [" + localUUid + "] [" + metaData.getIndexUUID() + "]");
                }
                if (clusterState.metaData().hasIndex(indexName) && (clusterState.nodes().localNode().masterNode() == true)) {
                    // we do not delete the store if it is a master eligible node and the index is still in the cluster state
                    // because we want to keep the meta data for indices around even if no shards are left here
                    final IndexMetaData index = clusterState.metaData().index(indexName);
                    throw new IllegalStateException("Can't delete closed index store for [" + indexName + "] - it's still part of the cluster state [" + index.getIndexUUID() + "] [" + metaData.getIndexUUID() + "]");
                }
            }
            Index index = new Index(metaData.getIndex());
            final IndexSettings indexSettings = buildIndexSettings(metaData);
            deleteIndexStore(reason, index, indexSettings, closed);
        }
    }

    private void deleteIndexStore(String reason, Index index, IndexSettings indexSettings, boolean closed) throws IOException {
        boolean success = false;
        try {
            // we are trying to delete the index store here - not a big deal if the lock can't be obtained
            // the store metadata gets wiped anyway even without the lock this is just best effort since
            // every shards deletes its content under the shard lock it owns.
            logger.debug("{} deleting index store reason [{}]", index, reason);
            if (canDeleteIndexContents(index, indexSettings, closed)) {
                nodeEnv.deleteIndexDirectorySafe(index, 0, indexSettings);
            }
            success = true;
        } catch (LockObtainFailedException ex) {
            logger.debug("{} failed to delete index store - at least one shards is still locked", ex, index);
        } catch (Exception ex) {
            logger.warn("{} failed to delete index", ex, index);
        } finally {
            if (success == false) {
                addPendingDelete(index, indexSettings);
            }
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
    public void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException {
        ShardId shardId = lock.getShardId();
        logger.trace("{} deleting shard reason [{}]", shardId, reason);
        nodeEnv.deleteShardDirectoryUnderLock(lock, indexSettings);
    }

    /**
     * This method deletes the shard contents on disk for the given shard ID. This method will fail if the shard deleting
     * is prevented by {@link #canDeleteShardContent(org.elasticsearch.index.shard.ShardId, org.elasticsearch.cluster.metadata.IndexMetaData)}
     * of if the shards lock can not be acquired.
     *
     * On data nodes, if the deleted shard is the last shard folder in its index, the method will attempt to remove the index folder as well.
     *
     * @param reason the reason for the shard deletion
     * @param shardId the shards ID to delete
     * @param clusterState . This is required to access the indexes settings etc.
     * @throws IOException if an IOException occurs
     */
    public void deleteShardStore(String reason, ShardId shardId, ClusterState clusterState) throws IOException {
        final IndexMetaData metaData = clusterState.getMetaData().indices().get(shardId.getIndex());

        final IndexSettings indexSettings = buildIndexSettings(metaData);
        if (canDeleteShardContent(shardId, indexSettings) == false) {
            throw new IllegalStateException("Can't delete shard " + shardId);
        }
        nodeEnv.deleteShardDirectorySafe(shardId, indexSettings);
        logger.debug("{} deleted shard reason [{}]", shardId, reason);

        if (clusterState.nodes().localNode().isMasterNode() == false && // master nodes keep the index meta data, even if having no shards..
                canDeleteIndexContents(shardId.index(), indexSettings, false)) {
            if (nodeEnv.findAllShardIds(shardId.index()).isEmpty()) {
                try {
                    // note that deleteIndexStore have more safety checks and may throw an exception if index was concurrently created.
                    deleteIndexStore("no longer used", metaData, clusterState, false);
                } catch (Exception e) {
                    // wrap the exception to indicate we already deleted the shard
                    throw new ElasticsearchException("failed to delete unused index after deleting its last shard (" + shardId + ")", e);
                }
            } else {
                logger.trace("[{}] still has shard stores, leaving as is", shardId.index());
            }
        }
    }

    /**
     * This method returns true if the current node is allowed to delete the
     * given index. If the index uses a shared filesystem this method always
     * returns false.
     * @param index {@code Index} to check whether deletion is allowed
     * @param indexSettings {@code IndexSettings} for the given index
     * @return true if the index can be deleted on this node
     */
    public boolean canDeleteIndexContents(Index index, IndexSettings indexSettings, boolean closed) {
        final IndexService indexService = this.indices.get(index.name());
        // Closed indices may be deleted, even if they are on a shared
        // filesystem. Since it is closed we aren't deleting it for relocation
        if (indexSettings.isOnSharedFilesystem() == false || closed) {
            if (indexService == null && nodeEnv.hasNodeFile()) {
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
        final IndexSettings indexSettings = buildIndexSettings(metaData);
        return canDeleteShardContent(shardId, indexSettings);
    }

    private boolean canDeleteShardContent(ShardId shardId, IndexSettings indexSettings) {
        final IndexService indexService = this.indices.get(shardId.getIndex());
        if (indexSettings.isOnSharedFilesystem() == false) {
            if (indexService != null && nodeEnv.hasNodeFile()) {
                return indexService.hasShard(shardId.id()) == false;
            } else if (nodeEnv.hasNodeFile()) {
                if (indexSettings.hasCustomDataPath()) {
                    return Files.exists(nodeEnv.resolveCustomLocation(indexSettings, shardId));
                } else {
                    return FileSystemUtils.exists(nodeEnv.availableShardPaths(shardId));
                }
            }
        } else {
            logger.trace("{} skipping shard directory deletion due to shadow replicas", shardId);
        }
        return false;
    }

    private IndexSettings buildIndexSettings(IndexMetaData metaData) {
        // play safe here and make sure that we take node level settings into account.
        // we might run on nodes where we use shard FS and then in the future don't delete
        // actual content.
        return new IndexSettings(metaData, settings, Collections.EMPTY_LIST);
    }

    /**
     * Adds a pending delete for the given index shard.
     */
    public void addPendingDelete(ShardId shardId, IndexSettings settings) {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (settings == null) {
            throw new IllegalArgumentException("settings must not be null");
        }
        PendingDelete pendingDelete = new PendingDelete(shardId, settings);
        addPendingDelete(shardId.index(), pendingDelete);
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
        }
    }

    private static final class PendingDelete implements Comparable<PendingDelete> {
        final String index;
        final int shardId;
        final IndexSettings settings;
        final boolean deleteIndex;

        /**
         * Creates a new pending delete of an index
         */
        public PendingDelete(ShardId shardId, IndexSettings settings) {
            this.index = shardId.getIndex();
            this.shardId = shardId.getId();
            this.settings = settings;
            this.deleteIndex = false;
        }

        /**
         * Creates a new pending delete of a shard
         */
        public PendingDelete(Index index, IndexSettings settings) {
            this.index = index.getName();
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
    public void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeout) throws IOException, InterruptedException {
        logger.debug("{} processing pending deletes", index);
        final long startTimeNS = System.nanoTime();
        final List<ShardLock> shardLocks = nodeEnv.lockAllForIndex(index, indexSettings, timeout.millis());
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
                                logger.debug("{} retry pending delete", ex, index);
                            }
                        } else {
                            assert delete.shardId != -1;
                            ShardLock shardLock = locks.get(new ShardId(delete.index, delete.shardId));
                            if (shardLock != null) {
                                try {
                                    deleteShardStore("pending delete", shardLock, delete.settings);
                                    iterator.remove();
                                } catch (IOException ex) {
                                    logger.debug("{} retry pending delete", ex, shardLock.getShardId());
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
     * Returns this nodes {@link IndicesQueriesRegistry}
     */
    public IndicesQueriesRegistry getIndicesQueryRegistry() {
        return indicesQueriesRegistry;
    }

    public AnalysisRegistry getAnalysis() {
        return analysisRegistry;
    }

}
