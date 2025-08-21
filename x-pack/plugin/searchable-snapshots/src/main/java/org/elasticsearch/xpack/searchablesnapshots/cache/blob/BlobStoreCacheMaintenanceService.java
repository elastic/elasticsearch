/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.blob;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.xpack.core.ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;

/**
 * A service that delete documents in the snapshot blob cache index when they are not required anymore.
 * <p>
 * This service runs on the data node that contains the snapshot blob cache primary shard. It listens to cluster state updates to find
 * searchable snapshot indices that are deleted and checks if the index snapshot is still used by other searchable snapshot indices. If the
 * index snapshot is not used anymore then it triggers the deletion of corresponding cached blobs in the snapshot blob cache index using a
 * delete-by-query.
 */
public class BlobStoreCacheMaintenanceService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(BlobStoreCacheMaintenanceService.class);

    /**
     * The interval at which the periodic cleanup of the blob store cache index is scheduled.
     */
    public static final Setting<TimeValue> SNAPSHOT_SNAPSHOT_CLEANUP_INTERVAL_SETTING = Setting.timeSetting(
        "searchable_snapshots.blob_cache.periodic_cleanup.interval",
        TimeValue.timeValueHours(1),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    /**
     * The keep alive value for the internal point-in-time requests executed during the periodic cleanup.
     */
    public static final Setting<TimeValue> SNAPSHOT_SNAPSHOT_CLEANUP_KEEP_ALIVE_SETTING = Setting.timeSetting(
        "searchable_snapshots.blob_cache.periodic_cleanup.pit_keep_alive",
        TimeValue.timeValueMinutes(10L),
        TimeValue.timeValueSeconds(30L),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    /**
     * The number of documents that are searched for and bulk-deleted at once during the periodic cleanup.
     */
    public static final Setting<Integer> SNAPSHOT_SNAPSHOT_CLEANUP_BATCH_SIZE_SETTING = Setting.intSetting(
        "searchable_snapshots.blob_cache.periodic_cleanup.batch_size",
        100,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The retention period to keep obsolete documents in the blob store cache index. This duration is used during the periodic cleanup in
     * order to avoid deleting documents belonging to concurrently mounted searchable snapshots. Defaults to 1h.
     */
    public static final Setting<TimeValue> SNAPSHOT_SNAPSHOT_CLEANUP_RETENTION_PERIOD = Setting.timeSetting(
        "searchable_snapshots.blob_cache.periodic_cleanup.retention_period",
        TimeValue.timeValueHours(1L),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final ClusterService clusterService;
    private final Client clientWithOrigin;
    private final String systemIndexName;
    private final ThreadPool threadPool;
    private final SystemIndexDescriptor systemIndexDescriptor;

    private volatile Scheduler.Cancellable periodicTask;
    private volatile TimeValue periodicTaskInterval;
    private volatile TimeValue periodicTaskKeepAlive;
    private volatile TimeValue periodicTaskRetention;
    private volatile int periodicTaskBatchSize;
    private volatile boolean schedulePeriodic;

    public BlobStoreCacheMaintenanceService(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SystemIndices systemIndices,
        String systemIndexName
    ) {
        this.clientWithOrigin = new OriginSettingClient(Objects.requireNonNull(client), SEARCHABLE_SNAPSHOTS_ORIGIN);
        this.systemIndexName = Objects.requireNonNull(systemIndexName);
        this.systemIndexDescriptor = Objects.requireNonNull(systemIndices.findMatchingDescriptor(systemIndexName));
        this.clusterService = Objects.requireNonNull(clusterService);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.periodicTaskInterval = SNAPSHOT_SNAPSHOT_CLEANUP_INTERVAL_SETTING.get(settings);
        this.periodicTaskKeepAlive = SNAPSHOT_SNAPSHOT_CLEANUP_KEEP_ALIVE_SETTING.get(settings);
        this.periodicTaskBatchSize = SNAPSHOT_SNAPSHOT_CLEANUP_BATCH_SIZE_SETTING.get(settings);
        this.periodicTaskRetention = SNAPSHOT_SNAPSHOT_CLEANUP_RETENTION_PERIOD.get(settings);
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(SNAPSHOT_SNAPSHOT_CLEANUP_INTERVAL_SETTING, this::setPeriodicTaskInterval);
        clusterSettings.addSettingsUpdateConsumer(SNAPSHOT_SNAPSHOT_CLEANUP_KEEP_ALIVE_SETTING, this::setPeriodicTaskKeepAlive);
        clusterSettings.addSettingsUpdateConsumer(SNAPSHOT_SNAPSHOT_CLEANUP_BATCH_SIZE_SETTING, this::setPeriodicTaskBatchSize);
        clusterSettings.addSettingsUpdateConsumer(SNAPSHOT_SNAPSHOT_CLEANUP_RETENTION_PERIOD, this::setPeriodicTaskRetention);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (state.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
            return; // state not fully recovered
        }
        if (systemIndexPrimaryShardActiveAndAssignedToLocalNode(state) == false) {
            // system index primary shard does not exist or is not assigned to this data node
            stopPeriodicTask();
            return;
        }
        if (event.indicesDeleted().isEmpty() == false) {
            threadPool.generic().execute(new DeletedIndicesMaintenanceTask(event));
        }
        if (periodicTask == null || periodicTask.isCancelled()) {
            schedulePeriodic = true;
            startPeriodicTask();
        }
    }

    private synchronized void setPeriodicTaskInterval(TimeValue interval) {
        this.periodicTaskInterval = interval;
    }

    private void setPeriodicTaskKeepAlive(TimeValue keepAlive) {
        this.periodicTaskKeepAlive = keepAlive;
    }

    public void setPeriodicTaskRetention(TimeValue retention) {
        this.periodicTaskRetention = retention;
    }

    public void setPeriodicTaskBatchSize(int batchSize) {
        this.periodicTaskBatchSize = batchSize;
    }

    private synchronized void startPeriodicTask() {
        if (schedulePeriodic) {
            try {
                final TimeValue delay = periodicTaskInterval;
                if (delay.getMillis() > 0L) {
                    final PeriodicMaintenanceTask task = new PeriodicMaintenanceTask(periodicTaskKeepAlive, periodicTaskBatchSize);
                    periodicTask = threadPool.schedule(task, delay, threadPool.generic());
                } else {
                    periodicTask = null;
                }
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    logger.debug("failed to schedule next periodic maintenance task for blob store cache, node is shutting down", e);
                } else {
                    throw e;
                }
            }
        }
    }

    private synchronized void stopPeriodicTask() {
        schedulePeriodic = false;
        if (periodicTask != null && periodicTask.isCancelled() == false) {
            periodicTask.cancel();
            periodicTask = null;
        }
    }

    private boolean systemIndexPrimaryShardActiveAndAssignedToLocalNode(final ClusterState state) {
        for (IndexMetadata indexMetadata : state.metadata().getProject()) {
            if (indexMetadata.isSystem() && systemIndexDescriptor.matchesIndexPattern(indexMetadata.getIndex().getName())) {
                final IndexRoutingTable indexRoutingTable = state.routingTable().index(indexMetadata.getIndex());
                if (indexRoutingTable == null || indexRoutingTable.shard(0) == null) {
                    continue;
                }
                final var primary = indexRoutingTable.shard(0).primaryShard();
                if (primary != null && primary.active() && Objects.equals(state.nodes().getLocalNodeId(), primary.currentNodeId())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean hasSearchableSnapshotWith(final ClusterState state, final String snapshotId, final String indexId) {
        for (IndexMetadata indexMetadata : state.metadata().getProject()) {
            if (indexMetadata.isSearchableSnapshot()) {
                final Settings indexSettings = indexMetadata.getSettings();
                if (Objects.equals(snapshotId, SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings))
                    && Objects.equals(indexId, SNAPSHOT_INDEX_ID_SETTING.get(indexSettings))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Instant getExpirationTime(TimeValue retention, ThreadPool threadPool) {
        return Instant.ofEpochMilli(threadPool.absoluteTimeInMillis()).minus(retention.duration(), retention.timeUnit().toChronoUnit());
    }

    private static Map<String, Set<String>> listSearchableSnapshots(final ClusterState state) {
        Map<String, Set<String>> snapshots = null;
        for (IndexMetadata indexMetadata : state.metadata().getProject()) {
            if (indexMetadata.isSearchableSnapshot()) {
                final Settings indexSettings = indexMetadata.getSettings();
                if (snapshots == null) {
                    snapshots = new HashMap<>();
                }
                snapshots.computeIfAbsent(SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings), s -> new HashSet<>())
                    .add(SNAPSHOT_INDEX_ID_SETTING.get(indexSettings));
            }
        }
        return snapshots != null ? Collections.unmodifiableMap(snapshots) : Collections.emptyMap();
    }

    static QueryBuilder buildDeleteByQuery(int numberOfShards, String snapshotUuid, String indexUuid) {
        final Set<String> paths = IntStream.range(0, numberOfShards)
            .mapToObj(shard -> String.join("/", snapshotUuid, indexUuid, String.valueOf(shard)))
            .collect(Collectors.toSet());
        assert paths.isEmpty() == false;
        return QueryBuilders.termsQuery("blob.path", paths);
    }

    /**
     * A maintenance task that cleans up the blob store cache index after searchable snapshot indices are deleted
     */
    private class DeletedIndicesMaintenanceTask extends AbstractRunnable {

        private final ClusterChangedEvent event;

        DeletedIndicesMaintenanceTask(ClusterChangedEvent event) {
            assert event.indicesDeleted().isEmpty() == false;
            this.event = Objects.requireNonNull(event);
        }

        @Override
        protected void doRun() {
            final Queue<Tuple<DeleteByQueryRequest, ActionListener<BulkByScrollResponse>>> queue = new LinkedList<>();
            final ClusterState state = event.state();

            for (Index deletedIndex : event.indicesDeleted()) {
                final IndexMetadata indexMetadata = event.previousState().metadata().getProject().index(deletedIndex);
                assert indexMetadata != null || state.metadata().getProject().indexGraveyard().containsIndex(deletedIndex)
                    : "no previous metadata found for " + deletedIndex;
                if (indexMetadata != null) {
                    if (indexMetadata.isSearchableSnapshot()) {
                        assert state.metadata().getProject().hasIndex(deletedIndex) == false;

                        final Settings indexSetting = indexMetadata.getSettings();
                        final String snapshotId = SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSetting);
                        final String indexId = SNAPSHOT_INDEX_ID_SETTING.get(indexSetting);

                        // we should do nothing if the current cluster state contains another
                        // searchable snapshot index that uses the same index snapshot
                        if (hasSearchableSnapshotWith(state, snapshotId, indexId)) {
                            logger.debug(
                                "snapshot [{}] of index {} is in use, skipping maintenance of snapshot blob cache entries",
                                snapshotId,
                                indexId
                            );
                            continue;
                        }

                        final DeleteByQueryRequest request = new DeleteByQueryRequest(systemIndexName);
                        request.setQuery(buildDeleteByQuery(indexMetadata.getNumberOfShards(), snapshotId, indexId));
                        request.setRefresh(queue.isEmpty());

                        queue.add(Tuple.tuple(request, new ActionListener<>() {
                            @Override
                            public void onResponse(BulkByScrollResponse response) {
                                logger.debug(
                                    "blob cache maintenance task deleted [{}] entries after deletion of {} (snapshot:{}, index:{})",
                                    response.getDeleted(),
                                    deletedIndex,
                                    snapshotId,
                                    indexId
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.debug(
                                    () -> format(
                                        "exception when executing blob cache maintenance task after deletion of %s (snapshot:%s, index:%s)",
                                        deletedIndex,
                                        snapshotId,
                                        indexId
                                    ),
                                    e
                                );
                            }
                        }));
                    }
                }
            }

            if (queue.isEmpty() == false) {
                executeNextCleanUp(queue);
            }
        }

        void executeNextCleanUp(final Queue<Tuple<DeleteByQueryRequest, ActionListener<BulkByScrollResponse>>> queue) {
            assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
            final Tuple<DeleteByQueryRequest, ActionListener<BulkByScrollResponse>> next = queue.poll();
            if (next != null) {
                cleanUp(next.v1(), next.v2(), queue);
            }
        }

        void cleanUp(
            final DeleteByQueryRequest request,
            final ActionListener<BulkByScrollResponse> listener,
            final Queue<Tuple<DeleteByQueryRequest, ActionListener<BulkByScrollResponse>>> queue
        ) {
            assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
            clientWithOrigin.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.runAfter(listener, () -> {
                if (queue.isEmpty() == false) {
                    threadPool.generic().execute(() -> executeNextCleanUp(queue));
                }
            }));
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(() -> "snapshot blob cache maintenance task failed for cluster state update [" + event.source() + "]", e);
        }
    }

    /**
     * A maintenance task that periodically cleans up unused cache entries from the blob store cache index.
     * <p>
     * This task first opens a point-in-time context on the blob store cache system index and uses it to search all documents. For each
     * document found the task verifies if it belongs to an existing searchable snapshot index. If the doc does not belong to any
     * index then it is deleted as part of a bulk request. Once the bulk is executed the next batch of documents is searched for. Once
     * all documents from the PIT have been verified the task closes the PIT and completes itself.
     * <p>
     * The task executes every step (PIT opening, searches, bulk deletes, PIT closing) using the generic thread pool.
     * The same task instance is used for all the steps and makes sure that a closed instance is not executed again.
     */
    private class PeriodicMaintenanceTask implements Runnable {
        private final TimeValue keepAlive;
        private final int batchSize;

        private final ThrottledTaskRunner taskRunner;
        private final AtomicLong deletes = new AtomicLong();
        private final AtomicLong total = new AtomicLong();

        PeriodicMaintenanceTask(TimeValue keepAlive, int batchSize) {
            this.keepAlive = keepAlive;
            this.batchSize = batchSize;
            this.taskRunner = new ThrottledTaskRunner(this.getClass().getCanonicalName(), 2, threadPool.generic());
        }

        @Override
        public void run() {
            ActionListener.run(ActionListener.runAfter(new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    logger.info(
                        () -> format(
                            "periodic maintenance task completed (%s deleted documents out of a total of %s)",
                            deletes.get(),
                            total.get()
                        )
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(
                        () -> format(
                            "periodic maintenance task completed with failure (%s deleted documents out of a total of %s)",
                            deletes.get(),
                            total.get()
                        ),
                        e
                    );
                }
            }, BlobStoreCacheMaintenanceService.this::startPeriodicTask), listener -> {
                final OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(SNAPSHOT_BLOB_CACHE_INDEX);
                openRequest.keepAlive(keepAlive);
                clientWithOrigin.execute(TransportOpenPointInTimeAction.TYPE, openRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(OpenPointInTimeResponse response) {
                        logger.trace("periodic maintenance task initialized with point-in-time id [{}]", response.getPointInTimeId());
                        threadPool.generic().execute(ActionRunnable.wrap(listener, l -> {
                            final ClusterState state = clusterService.state();
                            new RunningPeriodicMaintenanceTask(
                                response.getPointInTimeId(),
                                closingPitBefore(clientWithOrigin, response.getPointInTimeId(), l),
                                getExpirationTime(periodicTaskRetention, threadPool),
                                // compute the list of existing searchable snapshots and repositories up-front
                                listSearchableSnapshots(state),
                                RepositoriesMetadata.get(state)
                                    .repositories()
                                    .stream()
                                    .map(RepositoryMetadata::name)
                                    .collect(Collectors.toSet())
                            ).run();
                        }));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (TransportActions.isShardNotAvailableException(e)) {
                            listener.onResponse(null);
                        } else {
                            listener.onFailure(e);
                        }
                    }
                });
            });
        }

        private static ActionListener<Void> closingPitBefore(Client client, BytesReference pointInTimeId, ActionListener<Void> listener) {
            return new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    closePit(client, pointInTimeId, () -> listener.onResponse(null));
                }

                @Override
                public void onFailure(Exception e) {
                    closePit(client, pointInTimeId, () -> listener.onFailure(e));
                }
            };
        }

        private static void closePit(Client client, BytesReference pointInTimeId, Runnable onCompletion) {
            client.execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pointInTimeId), new ActionListener<>() {
                @Override
                public void onResponse(ClosePointInTimeResponse response) {
                    if (response.isSucceeded()) {
                        logger.debug("periodic maintenance task successfully closed point-in-time id [{}]", pointInTimeId);
                    } else {
                        logger.debug("point-in-time id [{}] not found", pointInTimeId);
                    }
                    onCompletion.run();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(() -> "failed to close point-in-time id [" + pointInTimeId + "]", e);
                    onCompletion.run();
                }
            });
        }

        /**
         * The maintenance task, once it has opened its PIT and started running so that it has all the state it needs to do its job.
         */
        private class RunningPeriodicMaintenanceTask implements Runnable {
            private final BytesReference pointInTimeId;
            private final RefCountingListener listeners;
            private final Instant expirationTime;
            private final Map<String, Set<String>> existingSnapshots;
            private final Set<String> existingRepositories;

            RunningPeriodicMaintenanceTask(
                BytesReference pointInTimeId,
                ActionListener<Void> listener,
                Instant expirationTime,
                Map<String, Set<String>> existingSnapshots,
                Set<String> existingRepositories
            ) {
                this.pointInTimeId = pointInTimeId;
                this.listeners = new RefCountingListener(listener);
                this.expirationTime = expirationTime;
                this.existingSnapshots = existingSnapshots;
                this.existingRepositories = existingRepositories;
            }

            @Override
            public void run() {
                assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
                try (listeners) {
                    executeSearch(new SearchRequest().source(getSearchSourceBuilder().trackTotalHits(true)), (searchResponse, refs) -> {
                        assert total.get() == 0L;
                        total.set(searchResponse.getHits().getTotalHits().value());
                        handleSearchResponse(searchResponse, refs);
                    });
                }
            }

            private void executeSearch(SearchRequest searchRequest, BiConsumer<SearchResponse, RefCounted> responseConsumer) {
                clientWithOrigin.execute(TransportSearchAction.TYPE, searchRequest, listeners.acquire(searchResponse -> {
                    searchResponse.mustIncRef();
                    taskRunner.enqueueTask(ActionListener.runAfter(listeners.acquire(ref -> {
                        final var refs = AbstractRefCounted.of(ref::close);
                        try {
                            responseConsumer.accept(searchResponse, refs);
                        } finally {
                            refs.decRef();
                        }
                    }), searchResponse::decRef));
                }));
            }

            private SearchSourceBuilder getSearchSourceBuilder() {
                return new SearchSourceBuilder().fetchField(new FieldAndFormat(CachedBlob.CREATION_TIME_FIELD, "epoch_millis"))
                    .fetchSource(false)
                    .trackScores(false)
                    .sort(ShardDocSortField.NAME)
                    .size(batchSize)
                    .pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId).setKeepAlive(keepAlive));
            }

            private void handleSearchResponse(SearchResponse searchResponse, RefCounted refs) {
                assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);

                if (listeners.isFailing()) {
                    return;
                }

                final var searchHits = searchResponse.getHits().getHits();
                if (searchHits == null || searchHits.length == 0) {
                    return;
                }

                final BulkRequest bulkRequest = new BulkRequest();

                Object[] lastSortValues = null;
                for (SearchHit searchHit : searchHits) {
                    lastSortValues = searchHit.getSortValues();
                    assert searchHit.getId() != null;
                    try {
                        boolean delete = false;

                        // See {@link BlobStoreCacheService#generateId}
                        // doc id = {repository name}/{snapshot id}/{snapshot index id}/{shard id}/{file name}/@{file offset}
                        final String[] parts = Objects.requireNonNull(searchHit.getId()).split("/");
                        assert parts.length == 6 : Arrays.toString(parts) + " vs " + searchHit.getId();

                        final String repositoryName = parts[0];
                        if (existingRepositories.contains(repositoryName) == false) {
                            logger.trace("deleting blob store cache entry with id [{}]: repository does not exist", searchHit.getId());
                            delete = true;
                        } else {
                            final Set<String> knownIndexIds = existingSnapshots.get(parts[1]);
                            if (knownIndexIds == null || knownIndexIds.contains(parts[2]) == false) {
                                logger.trace("deleting blob store cache entry with id [{}]: not used", searchHit.getId());
                                delete = true;
                            }
                        }
                        if (delete) {
                            final Instant creationTime = getCreationTime(searchHit);
                            if (creationTime.isAfter(expirationTime)) {
                                logger.trace(
                                    "blob store cache entry with id [{}] was created recently, skipping deletion",
                                    searchHit.getId()
                                );
                                continue;
                            }
                            bulkRequest.add(new DeleteRequest().index(searchHit.getIndex()).id(searchHit.getId()));
                        }
                    } catch (Exception e) {
                        logger.warn(
                            () -> format("exception when parsing blob store cache entry with id [%s], skipping", searchHit.getId()),
                            e
                        );
                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    refs.mustIncRef();
                    clientWithOrigin.execute(
                        TransportBulkAction.TYPE,
                        bulkRequest,
                        ActionListener.releaseAfter(listeners.acquire(bulkResponse -> {
                            for (BulkItemResponse itemResponse : bulkResponse.getItems()) {
                                if (itemResponse.isFailed() == false) {
                                    assert itemResponse.getResponse() instanceof DeleteResponse;
                                    deletes.incrementAndGet();
                                }
                            }
                        }), refs::decRef)
                    );
                }

                assert lastSortValues != null;
                executeSearch(
                    new SearchRequest().source(getSearchSourceBuilder().trackTotalHits(false).searchAfter(lastSortValues)),
                    this::handleSearchResponse
                );
            }
        }
    }

    private static Instant getCreationTime(SearchHit searchHit) {
        final DocumentField creationTimeField = searchHit.field(CachedBlob.CREATION_TIME_FIELD);
        assert creationTimeField != null;
        final Object creationTimeValue = creationTimeField.getValue();
        assert creationTimeValue != null;
        assert creationTimeValue instanceof String : "expect a java.lang.String but got " + creationTimeValue.getClass();
        return Instant.ofEpochMilli(Long.parseLong(creationTimeField.getValue()));
    }
}
