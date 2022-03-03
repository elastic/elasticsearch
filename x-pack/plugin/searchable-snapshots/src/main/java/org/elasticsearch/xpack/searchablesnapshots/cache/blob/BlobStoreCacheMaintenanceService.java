/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.blob;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.ClosePointInTimeAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeAction;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.xpack.core.ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;

/**
 * A service that delete documents in the snapshot blob cache index when they are not required anymore.
 *
 * This service runs on the data node that contains the snapshot blob cache primary shard. It listens to cluster state updates to find
 * searchable snapshot indices that are deleted and checks if the index snapshot is still used by other searchable snapshot indices. If the
 * index snapshot is not used anymore then i triggers the deletion of corresponding cached blobs in the snapshot blob cache index using a
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
        String systemIndexName
    ) {
        this.clientWithOrigin = new OriginSettingClient(Objects.requireNonNull(client), SEARCHABLE_SNAPSHOTS_ORIGIN);
        this.systemIndexName = Objects.requireNonNull(systemIndexName);
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
        final ShardRouting primary = systemIndexPrimaryShard(state);
        if (primary == null
            || primary.active() == false
            || Objects.equals(state.nodes().getLocalNodeId(), primary.currentNodeId()) == false) {
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
                    periodicTask = threadPool.schedule(task, delay, ThreadPool.Names.GENERIC);
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

    @Nullable
    private ShardRouting systemIndexPrimaryShard(final ClusterState state) {
        final IndexMetadata indexMetadata = state.metadata().index(systemIndexName);
        if (indexMetadata != null) {
            final IndexRoutingTable indexRoutingTable = state.routingTable().index(indexMetadata.getIndex());
            if (indexRoutingTable != null) {
                return indexRoutingTable.shard(0).primaryShard();
            }
        }
        return null;
    }

    private static boolean hasSearchableSnapshotWith(final ClusterState state, final String snapshotId, final String indexId) {
        for (IndexMetadata indexMetadata : state.metadata()) {
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

    private static Map<String, Set<String>> listSearchableSnapshots(final ClusterState state) {
        Map<String, Set<String>> snapshots = null;
        for (IndexMetadata indexMetadata : state.metadata()) {
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
                final IndexMetadata indexMetadata = event.previousState().metadata().index(deletedIndex);
                assert indexMetadata != null || state.metadata().indexGraveyard().containsIndex(deletedIndex)
                    : "no previous metadata found for " + deletedIndex;
                if (indexMetadata != null) {
                    if (indexMetadata.isSearchableSnapshot()) {
                        assert state.metadata().hasIndex(deletedIndex) == false;

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
                                    () -> new ParameterizedMessage(
                                        "exception when executing blob cache maintenance task after deletion of {} (snapshot:{}, index:{})",
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
            assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);
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
            assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);
            clientWithOrigin.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.runAfter(listener, () -> {
                if (queue.isEmpty() == false) {
                    threadPool.generic().execute(() -> executeNextCleanUp(queue));
                }
            }));
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(
                () -> new ParameterizedMessage("snapshot blob cache maintenance task failed for cluster state update [{}]", event.source()),
                e
            );
        }
    }

    /**
     * A maintenance task that periodically cleans up unused cache entries from the blob store cache index.
     *
     * This task first opens a point-in-time context on the blob store cache system index and uses it to search all documents. For each
     * document found the task verifies if it belongs to an existing searchable snapshot index. If the doc does not belong to any
     * index then it is deleted as part of a bulk request. Once the bulk is executed the next batch of documents is searched for. Once
     * all documents from the PIT have been verified the task closes the PIT and completes itself.
     *
     * The task executes every step (PIT opening, searches, bulk deletes, PIT closing) using the generic thread pool.
     * The same task instance is used for all the steps and makes sure that a closed instance is not executed again.
     */
    private class PeriodicMaintenanceTask implements Runnable, Releasable {

        private final TimeValue keepAlive;
        private final int batchSize;

        private final AtomicReference<Exception> error = new AtomicReference<>();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicLong deletes = new AtomicLong();
        private final AtomicLong total = new AtomicLong();

        private volatile Map<String, Set<String>> existingSnapshots;
        private volatile Set<String> existingRepositories;
        private volatile SearchResponse searchResponse;
        private volatile Instant expirationTime;
        private volatile String pointIntTimeId;
        private volatile Object[] searchAfter;

        PeriodicMaintenanceTask(TimeValue keepAlive, int batchSize) {
            this.keepAlive = keepAlive;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            assert assertGenericThread();
            try {
                ensureOpen();
                if (pointIntTimeId == null) {
                    final OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(SNAPSHOT_BLOB_CACHE_INDEX);
                    openRequest.keepAlive(keepAlive);
                    clientWithOrigin.execute(OpenPointInTimeAction.INSTANCE, openRequest, new ActionListener<>() {
                        @Override
                        public void onResponse(OpenPointInTimeResponse response) {
                            logger.trace("periodic maintenance task initialized with point-in-time id [{}]", response.getPointInTimeId());
                            PeriodicMaintenanceTask.this.pointIntTimeId = response.getPointInTimeId();
                            executeNext(PeriodicMaintenanceTask.this);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (TransportActions.isShardNotAvailableException(e)) {
                                complete(null);
                            } else {
                                complete(e);
                            }
                        }
                    });
                    return;
                }

                final String pitId = pointIntTimeId;
                assert Strings.hasLength(pitId);

                if (searchResponse == null) {
                    final SearchSourceBuilder searchSource = new SearchSourceBuilder();
                    searchSource.fetchField(new FieldAndFormat(CachedBlob.CREATION_TIME_FIELD, "epoch_millis"));
                    searchSource.fetchSource(false);
                    searchSource.trackScores(false);
                    searchSource.sort(ShardDocSortField.NAME);
                    searchSource.size(batchSize);
                    if (searchAfter != null) {
                        searchSource.searchAfter(searchAfter);
                        searchSource.trackTotalHits(false);
                    } else {
                        searchSource.trackTotalHits(true);
                    }
                    final PointInTimeBuilder pointInTime = new PointInTimeBuilder(pitId);
                    pointInTime.setKeepAlive(keepAlive);
                    searchSource.pointInTimeBuilder(pointInTime);
                    final SearchRequest searchRequest = new SearchRequest();
                    searchRequest.source(searchSource);
                    clientWithOrigin.execute(SearchAction.INSTANCE, searchRequest, new ActionListener<>() {
                        @Override
                        public void onResponse(SearchResponse response) {
                            if (searchAfter == null) {
                                assert PeriodicMaintenanceTask.this.total.get() == 0L;
                                PeriodicMaintenanceTask.this.total.set(response.getHits().getTotalHits().value);
                            }
                            PeriodicMaintenanceTask.this.searchResponse = response;
                            PeriodicMaintenanceTask.this.searchAfter = null;
                            executeNext(PeriodicMaintenanceTask.this);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            complete(e);
                        }
                    });
                    return;
                }

                final SearchHit[] searchHits = searchResponse.getHits().getHits();
                if (searchHits != null && searchHits.length > 0) {
                    if (expirationTime == null) {
                        final TimeValue retention = periodicTaskRetention;
                        expirationTime = Instant.ofEpochMilli(threadPool.absoluteTimeInMillis())
                            .minus(retention.duration(), retention.timeUnit().toChronoUnit());

                        final ClusterState state = clusterService.state();
                        // compute the list of existing searchable snapshots and repositories once
                        existingSnapshots = listSearchableSnapshots(state);
                        existingRepositories = state.metadata()
                            .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY)
                            .repositories()
                            .stream()
                            .map(RepositoryMetadata::name)
                            .collect(Collectors.toSet());
                    }

                    final BulkRequest bulkRequest = new BulkRequest();
                    final Map<String, Set<String>> knownSnapshots = existingSnapshots;
                    assert knownSnapshots != null;
                    final Set<String> knownRepositories = existingRepositories;
                    assert knownRepositories != null;
                    final Instant expirationTimeCopy = this.expirationTime;
                    assert expirationTimeCopy != null;

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
                            if (knownRepositories.contains(repositoryName) == false) {
                                logger.trace("deleting blob store cache entry with id [{}]: repository does not exist", searchHit.getId());
                                delete = true;
                            } else {
                                final Set<String> knownIndexIds = knownSnapshots.get(parts[1]);
                                if (knownIndexIds == null || knownIndexIds.contains(parts[2]) == false) {
                                    logger.trace("deleting blob store cache entry with id [{}]: not used", searchHit.getId());
                                    delete = true;
                                }
                            }
                            if (delete) {
                                final Instant creationTime = getCreationTime(searchHit);
                                if (creationTime.isAfter(expirationTimeCopy)) {
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
                                () -> new ParameterizedMessage(
                                    "exception when parsing blob store cache entry with id [{}], skipping",
                                    searchHit.getId()
                                ),
                                e
                            );
                        }
                    }

                    assert lastSortValues != null;
                    if (bulkRequest.numberOfActions() == 0) {
                        this.searchResponse = null;
                        this.searchAfter = lastSortValues;
                        executeNext(this);
                        return;
                    }

                    final Object[] finalSearchAfter = lastSortValues;
                    clientWithOrigin.execute(BulkAction.INSTANCE, bulkRequest, new ActionListener<>() {
                        @Override
                        public void onResponse(BulkResponse response) {
                            for (BulkItemResponse itemResponse : response.getItems()) {
                                if (itemResponse.isFailed() == false) {
                                    assert itemResponse.getResponse() instanceof DeleteResponse;
                                    PeriodicMaintenanceTask.this.deletes.incrementAndGet();
                                }
                            }
                            PeriodicMaintenanceTask.this.searchResponse = null;
                            PeriodicMaintenanceTask.this.searchAfter = finalSearchAfter;
                            executeNext(PeriodicMaintenanceTask.this);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            complete(e);
                        }
                    });
                    return;
                }
                // we're done, complete the task
                complete(null);
            } catch (Exception e) {
                complete(e);
            }
        }

        public boolean isClosed() {
            return closed.get();
        }

        private void ensureOpen() {
            if (isClosed()) {
                assert false : "should not use periodic task after close";
                throw new IllegalStateException("Periodic maintenance task is closed");
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                final Exception e = error.get();
                if (e != null) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "periodic maintenance task completed with failure ({} deleted documents out of a total of {})",
                            deletes.get(),
                            total.get()
                        ),
                        e
                    );
                } else {
                    logger.info(
                        () -> new ParameterizedMessage(
                            "periodic maintenance task completed ({} deleted documents out of a total of {})",
                            deletes.get(),
                            total.get()
                        )
                    );
                }
            }
        }

        private void complete(@Nullable Exception failure) {
            assert isClosed() == false;
            final Releasable releasable = () -> {
                try {
                    final Exception previous = error.getAndSet(failure);
                    assert previous == null : "periodic maintenance task already failed: " + previous;
                    close();
                } finally {
                    startPeriodicTask();
                }
            };
            boolean waitForRelease = false;
            try {
                final String pitId = pointIntTimeId;
                if (Strings.hasLength(pitId)) {
                    final ClosePointInTimeRequest closeRequest = new ClosePointInTimeRequest(pitId);
                    clientWithOrigin.execute(ClosePointInTimeAction.INSTANCE, closeRequest, ActionListener.runAfter(new ActionListener<>() {
                        @Override
                        public void onResponse(ClosePointInTimeResponse response) {
                            if (response.isSucceeded()) {
                                logger.debug("periodic maintenance task successfully closed point-in-time id [{}]", pitId);
                            } else {
                                logger.debug("point-in-time id [{}] not found", pitId);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn(() -> new ParameterizedMessage("failed to close point-in-time id [{}]", pitId), e);
                        }
                    }, () -> Releasables.close(releasable)));
                    waitForRelease = true;
                }
            } finally {
                if (waitForRelease == false) {
                    Releasables.close(releasable);
                }
            }
        }
    }

    private void executeNext(PeriodicMaintenanceTask maintenanceTask) {
        threadPool.generic().execute(maintenanceTask);
    }

    private static boolean assertGenericThread() {
        final String threadName = Thread.currentThread().getName();
        assert threadName.contains(ThreadPool.Names.GENERIC) : threadName;
        return true;
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
