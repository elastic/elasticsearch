/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.recovery.RegisterCommitResponse;
import co.elastic.elasticsearch.stateless.utils.WaitForVersion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.GlobalCheckpointListeners;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryCommitTooNewException;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.COMMIT_NOTIFICATION_TRANSPORT_ACTION_SPLIT;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Handles uploading new storage commits to the blob store, and tracks the lifetime of old commits until they can be safely deleted.
 * Old commits are safe to delete when search shards are no longer using them, in favor of newer commits.
 */
public class StatelessCommitService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(StatelessCommitService.class);

    /** How long an indexing shard should not have sent new commit notifications in order to be deemed as inactive. */
    public static final Setting<TimeValue> SHARD_INACTIVITY_DURATION_TIME_SETTING = Setting.positiveTimeSetting(
        "shard.inactivity.duration",
        TimeValue.timeValueMinutes(10),
        Setting.Property.NodeScope
    );

    /** How frequently we check for inactive indexing shards, and potentially send requests for in-use commits to the search shards. */
    public static final Setting<TimeValue> SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING = Setting.positiveTimeSetting(
        "shard.inactivity.monitor.interval",
        TimeValue.timeValueMinutes(30),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> STATELESS_UPLOAD_MAX_AMOUNT_COMMITS = Setting.intSetting(
        "stateless.upload.max_commits",
        100, // 100 means the 300s below should always kick in - this is then just a safety net.
        0,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> STATELESS_UPLOAD_MAX_SIZE = Setting.byteSizeSetting(
        "stateless.upload.max_size",
        ByteSizeValue.ofMb(15),
        ByteSizeValue.ZERO,
        ByteSizeValue.ofGb(1),
        Setting.Property.NodeScope
    );

    /**
     * How long can a VBCC exists before the upload monitor task can trigger its upload
     */
    public static final Setting<TimeValue> STATELESS_UPLOAD_VBCC_MAX_AGE = Setting.positiveTimeSetting(
        "stateless.upload.max_age",
        TimeValue.timeValueSeconds(300),
        Setting.Property.NodeScope
    );

    /**
     * How frequently the upload monitor should check the age of current VBCC and potentially trigger its upload
     */
    public static final Setting<TimeValue> STATELESS_UPLOAD_MONITOR_INTERVAL = Setting.positiveTimeSetting(
        "stateless.upload.monitor.interval",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    /**
     * Enables the lucene files headers/footers replication feature in order to speedup recovery.
     *
     * NOTE: once this is enabled, the project produces CC files in a new format.
     * This format can not be interpreted by the old readers.
     * Hence, all nodes must be upgraded to a new enough version understanding the new format prior to enabling this flag.
     */
    public static final Setting<Boolean> STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT = Setting.boolSetting(
        "stateless.commit.use_internal_files_replicated_content",
        true,
        Setting.Property.NodeScope
    );

    /**
     * Enables the super thin indexing shards feature in order to hollow inactive indexing shards and decrease their memory footprint.
     */
    public static final Setting<Boolean> STATELESS_HOLLOW_INDEX_SHARDS_ENABLED = Setting.boolSetting(
        "stateless.commit.hollow_index_shards.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * How long to wait for a global checkpoint listener to be notified before triggering a translog sync and retrying (without timeout).
     */
    public static final Setting<TimeValue> STATELESS_GCP_LISTENER_TRANSLOG_SYNC_TIMEOUT = Setting.positiveTimeSetting(
        "stateless.gcp.listener.translog.sync.timeout",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    /**
     * Maximum number of retries to attempt when an IO error is encountered during upload, before failing the shard.
     */
    public static final Setting<Integer> STATELESS_UPLOAD_MAX_IO_ERROR_RETRIES = Setting.intSetting(
        "stateless.upload.max_io_error_retries",
        5,
        Setting.Property.NodeScope
    );

    public static final String BCC_TOTAL_SIZE_HISTOGRAM_METRIC = "es.bcc.total_size_in_megabytes.histogram";
    public static final String BCC_NUMBER_COMMITS_HISTOGRAM_METRIC = "es.bcc.number_of_commits.histogram";
    public static final String BCC_ELAPSED_TIME_BEFORE_FREEZE_HISTOGRAM_METRIC = "es.bcc.elapsed_time_before_freeze.histogram";

    private final ClusterService clusterService;
    private final ObjectStoreService objectStoreService;
    private final IndicesService indicesService;
    private final Supplier<String> ephemeralNodeIdSupplier;
    private final Function<ShardId, Optional<IndexShardRoutingTable>> shardRoutingFinder;
    private final ThreadPool threadPool;
    private final StatelessCommitNotificationPublisher statelessCommitNotificationPublisher;
    // We don't do null checks when reading from this sub-map because we hold a commit reference while files are being uploaded. This will
    // prevent commit deletion in the interim.
    private final ConcurrentHashMap<ShardId, ShardCommitState> shardsCommitsStates = new ConcurrentHashMap<>();
    private final ConcurrentMap<ShardId, Consumer<Long>> commitNotificationSuccessListeners = new ConcurrentHashMap<>();
    private final StatelessCommitCleaner commitCleaner;

    private final WaitForVersion waitForClusterStateVersion = new WaitForVersion();

    private final TimeValue shardInactivityDuration;
    private final TimeValue shardInactivityMonitorInterval;
    private final ShardInactivityMonitor shardInactivityMonitor;
    private final SharedBlobCacheWarmingService cacheWarmingService;
    private Scheduler.Cancellable scheduledShardInactivityMonitorFuture;
    private final TimeValue virtualBccUploadMaxAge;
    private final TimeValue gcpListenerTranslogSyncTimeout;
    private final ScheduledUploadMonitor scheduledUploadMonitor;
    private final int bccMaxAmountOfCommits;
    private final long bccUploadMaxSizeInBytes;
    private final int bccUploadMaxIoRetries;
    private final boolean useInternalFilesReplicatedContent;
    private final boolean areHollowIndexShardsEnabled;
    private final int cacheRegionSizeInBytes;
    private final LongHistogram bccSizeInMegabytesHistogram;
    private final LongHistogram bccNumberCommitsHistogram;
    private final LongHistogram bccAgeHistogram;

    /**
     * An estimate of the maximum size in bytes that the header and replicated contents are likely to fill in a region. This is used when a
     * commit is appended to a virtual batched compound commit in order to determine if an internal file is contained within the same region
     * as the header and replicated content, in which case there is no need to replicate content for that file.
     */
    private final int estimatedMaxHeaderSizeInBytes;

    public StatelessCommitService(
        Settings settings,
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        IndicesService indicesService,
        Client client,
        StatelessCommitCleaner commitCleaner,
        StatelessSharedBlobCacheService cacheService,
        SharedBlobCacheWarmingService cacheWarmingService,
        TelemetryProvider telemetryProvider
    ) {
        this(
            settings,
            clusterService,
            objectStoreService,
            indicesService,
            () -> clusterService.localNode().getEphemeralId(),
            (shardId) -> shardRoutingTableFunction(clusterService, shardId),
            clusterService.threadPool(),
            client,
            commitCleaner,
            cacheService,
            cacheWarmingService,
            telemetryProvider
        );
    }

    public StatelessCommitService(
        Settings settings,
        ClusterService clusterService,
        ObjectStoreService objectStoreService,
        IndicesService indicesService,
        Supplier<String> ephemeralNodeIdSupplier,
        Function<ShardId, Optional<IndexShardRoutingTable>> shardRouting,
        ThreadPool threadPool,
        Client client,
        StatelessCommitCleaner commitCleaner,
        StatelessSharedBlobCacheService cacheService,
        SharedBlobCacheWarmingService cacheWarmingService,
        TelemetryProvider telemetryProvider
    ) {
        this.clusterService = clusterService;
        this.objectStoreService = objectStoreService;
        this.indicesService = indicesService;
        this.ephemeralNodeIdSupplier = ephemeralNodeIdSupplier;
        this.shardRoutingFinder = shardRouting;
        this.threadPool = threadPool;
        this.statelessCommitNotificationPublisher = new StatelessCommitNotificationPublisher(client);
        this.commitCleaner = commitCleaner;
        this.shardInactivityDuration = SHARD_INACTIVITY_DURATION_TIME_SETTING.get(settings);
        this.shardInactivityMonitorInterval = SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING.get(settings);
        this.cacheWarmingService = cacheWarmingService;
        this.shardInactivityMonitor = new ShardInactivityMonitor();
        this.virtualBccUploadMaxAge = STATELESS_UPLOAD_VBCC_MAX_AGE.get(settings);
        this.gcpListenerTranslogSyncTimeout = STATELESS_GCP_LISTENER_TRANSLOG_SYNC_TIMEOUT.get(settings);
        this.scheduledUploadMonitor = new ScheduledUploadMonitor(
            threadPool,
            threadPool.generic(),
            STATELESS_UPLOAD_MONITOR_INTERVAL.get(settings)
        );
        this.bccMaxAmountOfCommits = STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.get(settings);
        this.bccUploadMaxSizeInBytes = STATELESS_UPLOAD_MAX_SIZE.get(settings).getBytes();
        this.bccUploadMaxIoRetries = STATELESS_UPLOAD_MAX_IO_ERROR_RETRIES.get(settings).intValue();
        this.useInternalFilesReplicatedContent = STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT.get(settings);
        this.areHollowIndexShardsEnabled = STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.get(settings);
        this.cacheRegionSizeInBytes = cacheService.getRegionSize();
        this.estimatedMaxHeaderSizeInBytes = BlobCacheUtils.toIntBytes(Math.max(0L, cacheRegionSizeInBytes - bccUploadMaxSizeInBytes));
        logger.info(
            "Lucene files headers/footers replication feature is {} with max. header size of [{}] bytes",
            useInternalFilesReplicatedContent ? "enabled" : "disabled",
            estimatedMaxHeaderSizeInBytes
        );
        this.bccSizeInMegabytesHistogram = telemetryProvider.getMeterRegistry()
            .registerLongHistogram(
                BCC_TOTAL_SIZE_HISTOGRAM_METRIC,
                "Histogram for total size in megabytes of batched compound commits",
                "megabytes"
            );
        this.bccNumberCommitsHistogram = telemetryProvider.getMeterRegistry()
            .registerLongHistogram(
                BCC_NUMBER_COMMITS_HISTOGRAM_METRIC,
                "Histogram for number of commits per batched compound commit",
                "unit"
            );
        this.bccAgeHistogram = telemetryProvider.getMeterRegistry()
            .registerLongHistogram(
                BCC_ELAPSED_TIME_BEFORE_FREEZE_HISTOGRAM_METRIC,
                "Histogram for elapsed time in milliseconds of batched compound commits before freezing",
                "ms"
            );
    }

    public boolean useReplicatedRanges() {
        return useInternalFilesReplicatedContent;
    }

    public boolean areHollowIndexShardsEnabled() {
        return areHollowIndexShardsEnabled;
    }

    private static Optional<IndexShardRoutingTable> shardRoutingTableFunction(ClusterService clusterService, ShardId shardId) {
        RoutingTable routingTable = clusterService.state().routingTable();
        return routingTable.hasIndex(shardId.getIndex()) ? Optional.of(routingTable.shardRoutingTable(shardId)) : Optional.empty();
    }

    public void markRecoveredBcc(ShardId shardId, BatchedCompoundCommit recoveredBcc, Set<BlobFile> otherBlobs) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        assert recoveredBcc != null;
        assert recoveredBcc.shardId().equals(shardId) : recoveredBcc.shardId() + " vs " + shardId;
        commitState.markBccRecovered(recoveredBcc, otherBlobs);
    }

    /**
     * This method will mark the shard as relocating. It will calculate the max(minRelocatedGeneration, all pending uploads) and wait
     * for that generation to be uploaded after which it will trigger the provided listener. Additionally, this method will then block
     * the upload of any generations greater than the calculated max(minRelocatedGeneration, all pending uploads).
     *
     * We have implemented this mechanism opposed to using operation permits as we must push a flush after blocking all operations. If we
     * used operation permits, the final flush would not be able to proceed.
     *
     * This method returns an ActionListener with must be triggered when the relocation either fails or succeeds.
     */
    public ActionListener<Void> markRelocating(ShardId shardId, long minRelocatedGeneration, ActionListener<Void> listener) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.markRelocating(minRelocatedGeneration, listener);

        return new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                commitState.markRelocated();
            }

            @Override
            public void onFailure(Exception e) {
                commitState.markRelocationFailed();
            }
        };
    }

    public @Nullable VirtualBatchedCompoundCommit getVirtualBatchedCompoundCommit(
        ShardId shardId,
        PrimaryTermAndGeneration primaryTermAndGeneration
    ) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        return commitState.getVirtualBatchedCompoundCommit(primaryTermAndGeneration);
    }

    public long getRecoveredGeneration(ShardId shardId) {
        return getSafe(shardsCommitsStates, shardId).recoveredGeneration;
    }

    public void markCommitDeleted(ShardId shardId, long generation) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.markCommitDeleted(generation);
    }

    @Override
    protected void doStart() {
        scheduledShardInactivityMonitorFuture = threadPool.scheduleWithFixedDelay(
            shardInactivityMonitor,
            shardInactivityMonitorInterval,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
        scheduledUploadMonitor.rescheduleIfNecessary();
    }

    @Override
    protected void doStop() {
        scheduledShardInactivityMonitorFuture.cancel();
        scheduledUploadMonitor.close();
    }

    @Override
    protected void doClose() throws IOException {}

    /**
     * A runnable that polls search shards that have not received indexing for a while.
     * A request is sent to search shards asking what commits are still in use by readers on the search shard.
     */
    private class ShardInactivityMonitor implements Runnable {

        @Override
        public void run() {
            if (lifecycleState() != Lifecycle.State.STARTED) {
                return;
            }
            updateCommitUseTrackingForInactiveShards(threadPool::relativeTimeInMillis);
        }
    }

    /**
     * Fetches what commits are still in-use by the set of search shards for any shard that hasn't written anything in
     * {@link #shardInactivityDuration} millis. The results are then used to update the tracking on the index node so that unused old
     * commits can be flagged for removal.
     * <p>
     * Package private for testing.
     */
    void updateCommitUseTrackingForInactiveShards(Supplier<Long> time) {
        shardsCommitsStates.forEach((shardId, commitState) -> {
            if (commitState.isClosed() == false && commitState.lastNewCommitNotificationSentTimestamp > 0) {
                long elapsed = time.get() - commitState.lastNewCommitNotificationSentTimestamp;
                if (elapsed > shardInactivityDuration.getMillis()) {
                    commitState.pollSearchShardsForInUseOldCommits();
                }
            }
        });
    }

    /**
     * A scheduled task that runs on a fixed interval to iterate through each shard to check whether its VBCC is older than
     * configured {@code virtualBccUploadMaxAge} and trigger an upload in that case.
     */
    private class ScheduledUploadMonitor extends AbstractAsyncTask {

        private ScheduledUploadMonitor(ThreadPool threadPool, Executor executor, TimeValue interval) {
            super(logger, threadPool, executor, interval, true);
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        protected void runInternal() {
            shardsCommitsStates.forEach((shardId, commitState) -> {
                if (commitState.isClosed()) {
                    return;
                }
                final var virtualBcc = commitState.getCurrentVirtualBcc();
                if (virtualBcc == null) {
                    return;
                }
                long elapsed = threadPool.relativeTimeInMillis() - virtualBcc.getCreationTimeInMillis();
                if (elapsed > virtualBccUploadMaxAge.getMillis()) {
                    commitState.maybeFreezeAndUploadCurrentVirtualBcc(virtualBcc);
                }
            });
        }
    }

    /**
     * Add a global checkpoint listener. If the global checkpoint is equal to or above the global checkpoint the listener is waiting for,
     * then the listener will be notified immediately.
     *
     * @param addGlobalCheckpointListenerFunction the function to use to add the GCP listener on the index shard
     * @param triggerTranslogReplicator           the function to use to asynchronously trigger the translog replicator sync
     * @param waitingForGlobalCheckpoint          the global checkpoint the listener is waiting for
     * @param listener                            the listener
     */
    private void addGlobalCheckpointListener(
        TriConsumer<Long, GlobalCheckpointListeners.GlobalCheckpointListener, TimeValue> addGlobalCheckpointListenerFunction,
        Runnable triggerTranslogReplicator,
        final long waitingForGlobalCheckpoint,
        final ActionListener<Void> listener
    ) {
        innerAddGlobalCheckpointListener(
            addGlobalCheckpointListenerFunction,
            triggerTranslogReplicator,
            waitingForGlobalCheckpoint,
            listener,
            true
        );
    }

    private void innerAddGlobalCheckpointListener(
        TriConsumer<Long, GlobalCheckpointListeners.GlobalCheckpointListener, TimeValue> addGlobalCheckpointListenerFunction,
        Runnable triggerTranslogReplicator,
        final long waitingForGlobalCheckpoint,
        final ActionListener<Void> listener,
        final boolean retryOnTimeout
    ) {
        ActionListener.run(listener, l -> {
            // If a refresh comes in at the beginning of a last long-running bulk request that fills less than 16MB translog, then
            // the refresh will have to wait until the bulk completes (when the translog will be synced). To improve refresh
            // performance, we set a timeout (equal to the translog replicator flush interval) so that we trigger the translog sync,
            // and wait for the GCP indefinitely (since the translog uploads are retried indefinitely as well).
            addGlobalCheckpointListenerFunction.apply(waitingForGlobalCheckpoint, new GlobalCheckpointListeners.GlobalCheckpointListener() {
                @Override
                public Executor executor() {
                    return EsExecutors.DIRECT_EXECUTOR_SERVICE;
                }

                @Override
                public void accept(long globalCheckpoint, Exception e) {
                    if (globalCheckpoint != UNASSIGNED_SEQ_NO) {
                        assert waitingForGlobalCheckpoint <= globalCheckpoint
                            : "only advanced to [" + globalCheckpoint + "] while waiting for [" + waitingForGlobalCheckpoint + "]";
                        l.onResponse(null);
                    } else {
                        if (e instanceof TimeoutException) {
                            try {
                                // TODO: ideally we'd pass the checkpoint to the translog replicator sync call so it ensures seqnos up to
                                // the given one are persisted. This'd avoid the need for provoking the sync of higher seqnos unnecessarily.
                                triggerTranslogReplicator.run();
                            } catch (Exception ex) {
                                logger.debug(() -> "failed to trigger translog replicator sync", ex);
                            }
                            innerAddGlobalCheckpointListener(
                                addGlobalCheckpointListenerFunction,
                                triggerTranslogReplicator,
                                waitingForGlobalCheckpoint,
                                l,
                                false
                            );
                        } else {
                            assert e != null;
                            l.onFailure(e);
                        }
                    }
                }
            }, retryOnTimeout ? gcpListenerTranslogSyncTimeout : null);
        });
    }

    public void onCommitCreation(StatelessCommitRef reference) {
        boolean success = false;
        try {
            var shardId = reference.getShardId();
            var generation = reference.getGeneration();

            ShardCommitState commitState = getSafe(shardsCommitsStates, reference.getShardId());
            if (commitState.recoveredGeneration == generation) {
                logger.debug("{} skipping upload of recovered commit [{}]", shardId, generation);
                IOUtils.closeWhileHandlingException(reference);
                return;
            }

            assert reference.getPrimaryTerm() == commitState.allocationPrimaryTerm;
            logger.trace(
                () -> format("%s created commit %s", shardId, new PrimaryTermAndGeneration(reference.getPrimaryTerm(), generation))
            );

            // TODO: we can also check whether we need upload before appending to avoid creating VBCC just above the cache region size

            final VirtualBatchedCompoundCommit virtualBcc;
            final boolean commitAfterRelocationStarted;
            final Optional<IndexShardRoutingTable> shardRoutingTable = shardRoutingFinder.apply(shardId);
            synchronized (commitState) {
                // Have to check under lock before creating vbcc to ensure that the shard has not closed.
                if (commitState.isClosed()) {
                    logger.debug(
                        "{} aborting commit creation [state={}][primary term={}][generation={}]",
                        shardId,
                        commitState.state,
                        reference.getPrimaryTerm(),
                        generation
                    );
                    IOUtils.closeWhileHandlingException(reference);
                    return;
                }
                virtualBcc = commitState.appendCommit(reference);
                virtualBcc.addNotifiedSearchNodeIds(
                    shardRoutingTable.map(e -> e.unpromotableShards())
                        .orElse(List.of())
                        .stream()
                        .map(shardRouting -> shardRouting.currentNodeId())
                        .toList()
                );
                commitAfterRelocationStarted = commitState.isRelocating() && reference.getGeneration() > commitState.maxGenerationToUpload;
            }
            success = true;

            // todo: ES-8431 remove commitState.isInitializingNoSearch, we only need this for relocations now.
            // It's possible that a background merge is triggered by the relocation flushes, we do not want to notify
            // the search nodes about this commit since the segments in that commit can overlap with some of the segments
            // that might be created by the new primary node and can have different contents.
            if (shardRoutingTable.isEmpty() || commitState.isInitializingNoSearch() || commitAfterRelocationStarted) {
                // for initializing shards, the applied state may not yet be available in `ClusterService.state()`.
                // however, except for peer recovery, we can safely assume no search shards.
                commitState.notifyCommitNotificationSuccessListeners(generation);
            } else {
                // Fetch these values up front for consistent relative values: `virtualBcc` and `commitState` may be modified later in
                // parallel with the network request handling.
                var lastCompoundCommit = virtualBcc.lastCompoundCommit();
                var batchedCompoundCommitGeneration = virtualBcc.getPrimaryTermAndGeneration().generation();
                var maxUploadedBccTermAndGen = commitState.getMaxUploadedBccTermAndGen();

                // Non-uploaded new commit notifications should be sent after ensuring the operations are persisted. We achieve that
                // by (conservatively) waiting for the max seqno to be persisted by the translog replicator. This ensures that any searched
                // data is persisted in the object store, and that the search shards can safely update their global checkpoint to the value
                // of the local checkpoint in the commit.
                try {
                    var maxSeqNo = Long.parseLong(reference.getIndexCommit().getUserData().get(SequenceNumbers.MAX_SEQ_NO));
                    addGlobalCheckpointListener(
                        commitState.addGlobalCheckpointListenerFunction,
                        commitState.triggerTranslogReplicator,
                        maxSeqNo,
                        ActionListener.wrap(
                            ignored -> commitState.sendNewCommitNotification(
                                shardRoutingTable.get(),
                                lastCompoundCommit,
                                batchedCompoundCommitGeneration,
                                maxUploadedBccTermAndGen
                            ),
                            ex -> {
                                if (ex instanceof IndexShardClosedException) {
                                    // The shard was closed while waiting for the GCP. We can safely ignore this exception.
                                    logger.trace(
                                        () -> "shard closed while waiting for GCP to send new commit notification for "
                                            + lastCompoundCommit,
                                        ex
                                    );
                                } else {
                                    assert false : ex;
                                    logger.warn(
                                        "unexpected exception while waiting for GCP to send new commit notification for "
                                            + lastCompoundCommit,
                                        ex
                                    );
                                }
                            }
                        )
                    );
                } catch (IOException e) {
                    assert false : e; // should never happen, none of the Lucene implementations throw this.
                    throw new UncheckedIOException(e);
                }
            }

            if (commitState.shouldUploadVirtualBcc(virtualBcc)) {
                commitState.maybeFreezeAndUploadCurrentVirtualBcc(virtualBcc);
            }
        } catch (Exception ex) {
            assert false : ex;
            logger.warn(Strings.format("failed to handle new commit [%s], generation [%s]", reference, reference.getGeneration()), ex);
            throw ex;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(reference);
            }
        }
    }

    // VBCC uploads are retried indefinitely unless the shard has been closed or it has experienced too many IO errors
    private class UploadRetryDecider implements Predicate<Exception> {
        final ShardCommitState commitState;
        int localIOExceptions = 0;

        UploadRetryDecider(ShardCommitState commitState) {
            this.commitState = commitState;
        }

        public boolean test(Exception e) {
            if (commitState.isClosed()) {
                return false;
            }

            if (e instanceof ObjectStoreService.LocalIOException) {
                localIOExceptions++;
            }

            return localIOExceptions < bccUploadMaxIoRetries;
        }
    }

    private void createAndRunCommitUpload(
        ShardCommitState commitState,
        VirtualBatchedCompoundCommit virtualBcc,
        ShardCommitState.BlobReference blobReference
    ) {
        logger.debug(
            () -> Strings.format(
                "%s uploading batch compound commit %s: %s",
                virtualBcc.getShardId(),
                virtualBcc.getPrimaryTermAndGeneration(),
                virtualBcc.getPendingCompoundCommits().stream().map(pc -> pc.getCommitReference().getSegmentsFileName()).toList()
            )
        );
        // The CommitUpload listener is called after releasing the reference to the Lucene commit,
        // it's possible that due to a slow upload the commit is deleted in the meanwhile, therefore
        // we should acquire a reference to avoid deleting the commit before notifying the unpromotable shards.
        // todo: reevaluate this.
        blobReference.incRef();
        var bccUpload = new BatchedCompoundCommitUploadTask(
            threadPool,
            cacheWarmingService,
            objectStoreService,
            new UploadRetryDecider(commitState),
            commitState::pauseUpload,
            commitState::runUploadWhenCommitIsReady,
            virtualBcc,
            TimeValue.timeValueMillis(50),
            ActionListener.runAfter(new ActionListener<>() {
                @Override
                public void onResponse(BatchedCompoundCommit uploadedBcc) {
                    try {
                        commitState.markBccUploaded(uploadedBcc);
                        commitState.sendNewUploadedCommitNotification(blobReference, uploadedBcc);
                    } catch (Exception e) {
                        // TODO: we should assert false here once we fix https://elasticco.atlassian.net/browse/ES-8336
                        logger.warn(
                            () -> format(
                                "%s failed to send new uploaded BCC [%s] notification",
                                virtualBcc.getShardId(),
                                virtualBcc.getPrimaryTermAndGeneration().generation()
                            ),
                            e
                        );
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    assert assertClosedOrRejectionFailure(e);
                    ShardCommitState.State state = commitState.state;
                    if (commitState.isClosed()) {
                        logger.debug(
                            () -> format(
                                "%s failed to upload BCC [%s] to object store because shard has invalid state %s",
                                virtualBcc.getShardId(),
                                virtualBcc.getPrimaryTermAndGeneration().generation(),
                                state
                            ),
                            e
                        );
                    } else if (e instanceof ObjectStoreService.LocalIOException) {
                        failShard(
                            commitState.shardId,
                            virtualBcc.getPrimaryTermAndGeneration().generation(),
                            (ObjectStoreService.LocalIOException) e
                        );
                    } else {
                        logger.warn(
                            () -> format(
                                "%s failed to upload BCC [%s] to object store for unexpected reason",
                                virtualBcc.getShardId(),
                                virtualBcc.getPrimaryTermAndGeneration().generation()
                            ),
                            e
                        );
                    }
                }

                private boolean assertClosedOrRejectionFailure(final Exception e) {
                    final var closed = commitState.isClosed();
                    assert closed
                        || e instanceof ObjectStoreService.LocalIOException
                        || e instanceof EsRejectedExecutionException
                        || e instanceof IndexNotFoundException
                        || e instanceof ShardNotFoundException : closed + " vs " + e;
                    return true;
                }
            }, () -> {
                IOUtils.closeWhileHandlingException(virtualBcc);
                blobReference.decRef();
            })
        );

        // Update the histograms with the new remote blob store upload info.
        bccSizeInMegabytesHistogram.record(ByteSizeUnit.BYTES.toMB(virtualBcc.getTotalSizeInBytes()));
        bccNumberCommitsHistogram.record(virtualBcc.size());
        bccAgeHistogram.record(threadPool.relativeTimeInMillis() - virtualBcc.getCreationTimeInMillis());

        bccUpload.run();
    }

    void failShard(ShardId shardId, long generation, ObjectStoreService.LocalIOException error) {
        final var indexService = indicesService.indexService(shardId.getIndex());
        if (indexService == null) {
            logger.info(format("%s index not found, cannot fail BCC [%s] for IO error", shardId, generation), error);
            return;
        }
        IndexShard shard = indexService.getShard(shardId.id());
        shard.failShard(format("%s failed to upload BCC [%s] due to IO error", shardId, generation), error);
    }

    public boolean hasPendingBccUploads(ShardId shardId) {
        try {
            ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
            return commitState.pendingUploadBccGenerations.isEmpty() == false;
        } catch (AlreadyClosedException ace) {
            return false;
        }
    }

    public void register(
        ShardId shardId,
        long primaryTerm,
        BooleanSupplier inititalizingNoSearchSupplier,
        TriConsumer<Long, GlobalCheckpointListeners.GlobalCheckpointListener, TimeValue> addGlobalCheckpointListenerFunction,
        Runnable triggerTranslogReplicator
    ) {
        ShardCommitState existing = shardsCommitsStates.put(
            shardId,
            createShardCommitState(
                shardId,
                primaryTerm,
                inititalizingNoSearchSupplier,
                addGlobalCheckpointListenerFunction,
                triggerTranslogReplicator
            )
        );
        assert existing == null : shardId + " already registered";
    }

    protected ShardCommitState createShardCommitState(
        ShardId shardId,
        long primaryTerm,
        BooleanSupplier inititalizingNoSearchSupplier,
        TriConsumer<Long, GlobalCheckpointListeners.GlobalCheckpointListener, TimeValue> addGlobalCheckpointListenerFunction,
        Runnable triggerTranslogReplicator
    ) {
        return new ShardCommitState(
            shardId,
            primaryTerm,
            inititalizingNoSearchSupplier,
            addGlobalCheckpointListenerFunction,
            triggerTranslogReplicator
        );
    }

    public void closeShard(ShardId shardId) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.close();
    }

    public void unregister(ShardId shardId) {
        ShardCommitState removed = shardsCommitsStates.remove(shardId);
        assert removed != null : shardId + " not registered";
        assert removed.isClosed();
        removed.unregistered();
    }

    /**
     * Marks the {@link ShardCommitState} as deleted, which will delete associated blobs upon the forthcoming {@link #unregister(ShardId)}.
     * @param shardId the shard to mark as deleted
     */
    public void delete(ShardId shardId) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.delete();
    }

    public void onGenerationalFileDeletion(ShardId shardId, String filename) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.onGenerationalFileDeletion(filename);
    }

    public void addListenerForUploadedGeneration(ShardId shardId, long generation, ActionListener<Void> listener) {
        requireNonNull(listener, "listener cannot be null");
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.addListenerForUploadedGeneration(generation, listener);
    }

    public void ensureMaxGenerationToUploadForFlush(ShardId shardId, long generation) {
        final ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.ensureMaxGenerationToUploadForFlush(generation);
    }

    public long getMaxGenerationToUploadForFlush(ShardId shardId) {
        final ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        return commitState.getMaxGenerationToUploadForFlush();
    }

    // Visible for testing
    public VirtualBatchedCompoundCommit getCurrentVirtualBcc(ShardId shardId) {
        final ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        return commitState.getCurrentVirtualBcc();
    }

    /**
     * @param uploadedBcc the BCC that was uploaded
     * @param blobFileRanges the individual files and blob file ranges that are still necessary to be able to access, including
     *                      being held by open readers or being part of a commit that is not yet deleted by lucene.
     *                      Always includes all files from the new commit.
     */
    public record UploadedBccInfo(BatchedCompoundCommit uploadedBcc, Map<String, BlobFileRanges> blobFileRanges) {}

    public void addConsumerForNewUploadedBcc(ShardId shardId, Consumer<UploadedBccInfo> listener) {
        requireNonNull(listener, "listener cannot be null");
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.addConsumerForNewUploadedBcc(listener);
    }

    // Visible for testing
    Set<String> getFilesWithBlobLocations(ShardId shardId) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        return commitState.blobLocations.keySet();
    }

    @Nullable
    public BlobLocation getBlobLocation(ShardId shardId, String fileName) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        var commitAndBlobLocation = commitState.blobLocations.get(fileName);
        if (commitAndBlobLocation != null) {
            return commitAndBlobLocation.blobLocation;
        }
        return null;
    }

    // Visible for testing
    @Nullable
    public BatchedCompoundCommit getLatestUploadedBcc(ShardId shardId) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        return commitState.latestUploadedBcc;
    }

    // Visible for testing
    Set<String> getAllSearchNodesRetainingCommitsForShard(ShardId shardId) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        return commitState.getAllSearchNodesRetainingCommits();
    }

    /**
     * Returns the {@link ShardCommitState} for the given {@link ShardId}. Throws an exception if there is no commit state found, meaning
     * that the shard is closed.
     *
     * Visible for testing.
     */
    private static ShardCommitState getSafe(ConcurrentHashMap<ShardId, ShardCommitState> map, ShardId shardId) {
        final ShardCommitState commitState = map.get(shardId);
        if (commitState == null) {
            throw new AlreadyClosedException("shard [" + shardId + "] has already been closed");
        }
        return commitState;
    }

    class ShardCommitState implements IndexEngineLocalReaderListener, CommitBCCResolver {
        private static final long EMPTY_GENERATION_NOTIFIED_SENTINEL = -1;

        private enum State {
            RUNNING,
            RELOCATING,
            CLOSED
        }

        private final ShardId shardId;
        private final long allocationPrimaryTerm;
        private final BooleanSupplier inititalizingNoSearchSupplier;
        private final TriConsumer<Long, GlobalCheckpointListeners.GlobalCheckpointListener, TimeValue> addGlobalCheckpointListenerFunction;
        private final Runnable triggerTranslogReplicator;

        // The following three fields represent the state of a batched compound commit can have in its lifecycle.
        // 1. currentVirtualBcc - A BCC starts its lifecycle from here. It is used to append new CCs. The field itself begins from null,
        // gets assigned at commit appending time, and gets resets back to null again by freeze, then starts over. Mutating this field
        // MUST be performed with synchronization.
        // 2. pendingUploadBccGenerations - When the current VBCC is frozen, it is added to the pending list for upload. This is a map of
        // VBCC generation to VBCC to keep track of the VBCCs that are pending to upload. The VBCCs are also used to serve data fetching
        // request from search nodes. A VBCC is removed from this list once it is uploaded.
        // 3. latestUploadedBcc - This field tracks highest generation BCC ever uploaded. It is updated with the VBCC that just gets
        // uploaded which is then removed from pendingUploadBccGenerations.
        private volatile VirtualBatchedCompoundCommit currentVirtualBcc = null;
        private final Map<Long, VirtualBatchedCompoundCommit> pendingUploadBccGenerations = new ConcurrentHashMap<>();
        private volatile BatchedCompoundCommit latestUploadedBcc = null;
        // NOTE When moving a VBCC through its lifecycle, we must update it first in the new state before remove it from the old state.
        // That is, we must first add it to the `pendingUploadBccGenerations` before un-assigning it from `currentVirtualBcc`,
        // and we must set it to `latestUploadedBcc` before removing it from `pendingUploadBccGenerations`. This is to ensure the VBCC
        // is visible to the outside of the world throughout its lifecycle. See `maybeFreezeAndUploadCurrentVirtualBcc` and
        // `handleUploadedBcc`.
        // The order must be reversed when reading a BCC based on its generation, i.e. we must first check `currentVirtualBcc`,
        // then `pendingUploadBccGenerations` and lastly `latestUploadedBcc`, to have full coverage _without_ using synchronization.

        private List<Tuple<Long, ActionListener<Void>>> generationListeners = null;
        private List<Consumer<UploadedBccInfo>> uploadedBccConsumers = null;
        private volatile long recoveredGeneration = -1;
        private volatile long recoveredPrimaryTerm = -1;

        /**
         * The highest generation number that we have received from an uploaded commit notification response from search shards.
         */
        private final AtomicLong uploadedGenerationNotified = new AtomicLong(EMPTY_GENERATION_NOTIFIED_SENTINEL);
        private volatile long maxGenerationToUpload = Long.MAX_VALUE;
        private final AtomicLong maxGenerationToUploadForFlush = new AtomicLong(-1);
        // Does not need to be volatile because it uses reads/writes of state for visibility
        private boolean relocated = false;
        private volatile State state = State.RUNNING;
        private volatile boolean isDeleted;
        // map BCC generations to BCC blob instances
        private final Map<PrimaryTermAndGeneration, BlobReference> primaryTermAndGenToBlobReference = new ConcurrentHashMap<>();

        // maps file names to their (maybe future) compound commit blob & blob location
        private final Map<String, CommitAndBlobLocation> blobLocations = new ConcurrentHashMap<>();
        private volatile long lastNewCommitNotificationSentTimestamp = -1;
        // Map commits to BCCs
        private final Map<PrimaryTermAndGeneration, CommitReferencesInfo> commitReferencesInfos = new ConcurrentHashMap<>();

        // Visible for testing
        ShardCommitState(
            ShardId shardId,
            long allocationPrimaryTerm,
            BooleanSupplier inititalizingNoSearchSupplier,
            TriConsumer<Long, GlobalCheckpointListeners.GlobalCheckpointListener, TimeValue> addGlobalCheckpointListenerFunction,
            Runnable triggerTranslogReplicator
        ) {
            this.shardId = shardId;
            this.allocationPrimaryTerm = allocationPrimaryTerm;
            this.inititalizingNoSearchSupplier = inititalizingNoSearchSupplier;
            this.addGlobalCheckpointListenerFunction = addGlobalCheckpointListenerFunction;
            this.triggerTranslogReplicator = triggerTranslogReplicator;
        }

        /**
         * Returns whether to skip uploading the commit file with the specified generation.
         *
         * When a shard is in the process of relocating, we change the state to {@link State#RELOCATING} and set a max
         * generation to attempt to upload: we won't do further writes/uploads beyond that max generation.
         */
        private boolean pauseUpload(long uploadGeneration) {
            return state != StatelessCommitService.ShardCommitState.State.RUNNING && uploadGeneration > maxGenerationToUpload;
        }

        private boolean isClosed() {
            return state == State.CLOSED;
        }

        private boolean isRelocating() {
            return state == State.RELOCATING;
        }

        /**
         * Check if the shard is still in an initializing state according to the last applied state on the shard that cannot have search
         * shards
         */
        private boolean isInitializingNoSearch() {
            return inititalizingNoSearchSupplier.getAsBoolean();
        }

        private void markBccRecovered(BatchedCompoundCommit recoveredBcc, Set<BlobFile> otherBlobs) {
            assert recoveredBcc != null;
            assert otherBlobs != null;
            assert primaryTermAndGenToBlobReference.isEmpty() : primaryTermAndGenToBlobReference;
            assert blobLocations.isEmpty() : blobLocations;

            final var recoveredCommit = recoveredBcc.lastCompoundCommit();
            Map<PrimaryTermAndGeneration, Map<String, BlobLocation>> referencedBlobs = new HashMap<>();
            final var bccStoredFiles = recoveredBcc.getAllInternalFiles();
            // It's possible that the recovered commit uses files from a different commit stored
            // in the same BCC, hence we need to ensure that those are also present in the recovered BCC BlobReference
            final var recoveredBCCFilesUsedByRecoveredCommit = new HashSet<String>();
            for (Map.Entry<String, BlobLocation> referencedBlob : recoveredCommit.commitFiles().entrySet()) {
                // The recoveredCommit may contain non-internal files that belong to the same BCC,
                // that's why we consider all the BCC internal files when we check if we need to
                // create an BlobReference for a commit file that is stored in a different BCC
                if (bccStoredFiles.contains(referencedBlob.getKey()) == false) {
                    referencedBlobs.computeIfAbsent(
                        referencedBlob.getValue().getBatchedCompoundCommitTermAndGeneration(),
                        primaryTermAndGeneration -> new HashMap<>()
                    ).put(referencedBlob.getKey(), referencedBlob.getValue());
                } else {
                    recoveredBCCFilesUsedByRecoveredCommit.add(referencedBlob.getKey());
                }
            }

            List<BlobReference> previousBCCBlobs = new ArrayList<>(otherBlobs.size());
            // create a compound commit blob instance for the recovery commit
            for (BlobFile nonRecoveredBlobFile : otherBlobs) {
                if (StatelessCompoundCommit.startsWithBlobPrefix(nonRecoveredBlobFile.blobName())) {
                    PrimaryTermAndGeneration nonRecoveredTermGen = nonRecoveredBlobFile.termAndGeneration();

                    Map<String, BlobLocation> internalFiles = referencedBlobs.getOrDefault(nonRecoveredTermGen, Collections.emptyMap());
                    // We don't know which CCs are included in the non-recovered BCCs,
                    // hence the empty set for the included commits in the blob reference
                    BlobReference nonRecoveredBlobReference = new BlobReference(
                        nonRecoveredTermGen,
                        internalFiles.keySet(),
                        Set.of(),
                        Set.of(),
                        // The blob might contain generational files, but these will be read from the recovered commit
                        // (as they're copied over to new blobs), that's why we consider that the blob does not contain
                        // any generational file.
                        Set.of()
                    );
                    internalFiles.forEach((key, value) -> {
                        var previous = blobLocations.put(key, new CommitAndBlobLocation(nonRecoveredBlobReference, value));
                        assert previous == null : key + ':' + previous;
                    });
                    primaryTermAndGenToBlobReference.put(nonRecoveredTermGen, nonRecoveredBlobReference);
                    previousBCCBlobs.add(nonRecoveredBlobReference);
                } else {
                    logger.warn(
                        () -> format(
                            "%s found object store file which does not match compound commit file naming pattern [%s]",
                            shardId,
                            nonRecoveredBlobFile
                        )
                    );
                }
            }

            var referencedBCCsForRecoveryCommit = previousBCCBlobs.stream()
                .filter(bccBlobReference -> referencedBlobs.containsKey(bccBlobReference.getPrimaryTermAndGeneration()))
                .collect(Collectors.toUnmodifiableSet());

            final Set<String> includedGenerationalFiles = recoveredCommit.commitFiles()
                .keySet()
                .stream()
                .filter(StatelessCommitService::isGenerationalFile)
                .collect(Collectors.toSet());

            var recoveryBCCBlob = new BlobReference(
                recoveredBcc.primaryTermAndGeneration(),
                recoveredBCCFilesUsedByRecoveredCommit,
                referencedBCCsForRecoveryCommit,
                // During recovery we only read the latest stored commit,
                // hence we consider that the recovered blob reference contains only that commit
                Set.of(recoveredCommit.primaryTermAndGeneration()),
                includedGenerationalFiles
            );

            assert primaryTermAndGenToBlobReference.containsKey(recoveredCommit.primaryTermAndGeneration()) == false;

            // TODO: ES-8235 Maybe structure BlobReference and related data structures better?
            // Populate dependent BlobReference data structures before we publish the recoveryBCCBlob
            // by adding it to primaryTermAndGenToBlobReference (i.e. the recovery might fail due to the index being deleted
            // while recovery is running and when we try to clean up some of the related data structures it can trip assertions
            // due to them being in a non-consistent state)
            recoveredBCCFilesUsedByRecoveredCommit.forEach(fileName -> {
                var fileBlobLocation = recoveredCommit.commitFiles().get(fileName);
                assert fileBlobLocation != null : fileName;
                var existing = blobLocations.put(fileName, new CommitAndBlobLocation(recoveryBCCBlob, fileBlobLocation));
                assert existing == null : fileName + ':' + existing;
            });

            var referencedBCCGenerationsByRecoveredCommit = BatchedCompoundCommit.computeReferencedBCCGenerations(recoveredCommit);
            commitReferencesInfos.put(
                recoveredCommit.primaryTermAndGeneration(),
                new CommitReferencesInfo(recoveredBcc.primaryTermAndGeneration(), referencedBCCGenerationsByRecoveredCommit)
            );

            primaryTermAndGenToBlobReference.put(recoveryBCCBlob.getPrimaryTermAndGeneration(), recoveryBCCBlob);

            var currentUnpromotableShardAssignedNodes = shardRoutingFinder.apply(shardId)
                .map(IndexShardRoutingTable::unpromotableShards)
                .orElse(List.of())
                .stream()
                .map(ShardRouting::currentNodeId)
                .collect(Collectors.toSet());

            primaryTermAndGenToBlobReference.values()
                .forEach(commit -> trackOutstandingUnpromotableShardCommitRef(currentUnpromotableShardAssignedNodes, commit));

            // Decrement all of the non-recovered BCCs that are not referenced by the recovered commit
            for (BlobReference blobReference : primaryTermAndGenToBlobReference.values()) {
                if (blobReference.getPrimaryTermAndGeneration().equals(recoveryBCCBlob.getPrimaryTermAndGeneration()) == false) {
                    blobReference.removeAllLocalCommitsRefs();
                    blobReference.removeAllGenerationalFilesRefs();
                }
                if (referencedBCCGenerationsByRecoveredCommit.contains(blobReference.getPrimaryTermAndGeneration()) == false) {
                    blobReference.closedLocalReaders();
                }
            }

            recoveredPrimaryTerm = recoveredCommit.primaryTerm();
            recoveredGeneration = recoveredCommit.generation();
            assert assertRecoveredCommitFilesHaveBlobLocations(Map.copyOf(recoveredCommit.commitFiles()), Map.copyOf(blobLocations));
            handleUploadedBcc(recoveredBcc, false);
        }

        public void ensureMaxGenerationToUploadForFlush(long generation) {
            if (isClosed()) {
                return;
            }
            final long previousMaxGeneration = maxGenerationToUploadForFlush.getAndUpdate(v -> Math.max(v, generation));
            if (previousMaxGeneration >= generation) {
                return;
            }

            final var virtualBcc = getCurrentVirtualBcc();
            if (virtualBcc == null) {
                assert assertGenerationIsUploadedOrPending(generation);
                return;
            }

            if (generation < virtualBcc.getPrimaryTermAndGeneration().generation()) {
                return;
            }

            assert virtualBcc.getMaxGeneration() >= generation
                : "requested generation ["
                    + generation
                    + "] is larger than the max available generation ["
                    + virtualBcc.getMaxGeneration()
                    + "]";
            maybeFreezeAndUploadCurrentVirtualBcc(virtualBcc);
        }

        public VirtualBatchedCompoundCommit getCurrentVirtualBcc() {
            return currentVirtualBcc;
        }

        @Nullable
        public VirtualBatchedCompoundCommit getCurrentOrPendingUploadVirtualBcc() {
            var virtualBcc = getCurrentVirtualBcc();
            if (virtualBcc == null) {
                virtualBcc = getMaxPendingUploadBcc().orElse(null);
            }
            return virtualBcc;
        }

        /**
         * Get the current {@link VirtualBatchedCompoundCommit}, or one of the {@link VirtualBatchedCompoundCommit} that are being
         * uploaded. Else, return null.
         *
         * Note the requested generation must correspond to either the current VBCC, one of the VBCC pending to be uploaded, or less than
         * or equal to the max uploaded generation.
         */
        public @Nullable VirtualBatchedCompoundCommit getVirtualBatchedCompoundCommit(PrimaryTermAndGeneration primaryTermAndGeneration) {
            var currentVirtualBcc = getCurrentVirtualBcc();
            if (currentVirtualBcc != null && currentVirtualBcc.getPrimaryTermAndGeneration().equals(primaryTermAndGeneration)) {
                return currentVirtualBcc;
            }
            var pendingUploadVirtualBcc = pendingUploadBccGenerations.get(primaryTermAndGeneration.generation());
            if (pendingUploadVirtualBcc != null) {
                assert pendingUploadVirtualBcc.getPrimaryTermAndGeneration().equals(primaryTermAndGeneration);
                return pendingUploadVirtualBcc;
            }

            // We did not find the generation, so it should be already uploaded.
            if (getMaxUploadedGeneration() >= 0) {
                checkGenerationIsUploadedOrPending(primaryTermAndGeneration.generation());
            } else {
                var recoveredGeneration = this.recoveredGeneration;
                if (primaryTermAndGeneration.generation() <= recoveredGeneration) {
                    throw new IllegalStateException(
                        "requested generation ["
                            + primaryTermAndGeneration.generation()
                            + "] is less than or equal to the recovered generation ["
                            + recoveredGeneration
                            + "]"
                    );
                }
            }

            return null;
        }

        private Optional<VirtualBatchedCompoundCommit> getMaxPendingUploadBcc() {
            return pendingUploadBccGenerations.values()
                .stream()
                .max(Comparator.comparing(VirtualBatchedCompoundCommit::getPrimaryTermAndGeneration));
        }

        private Optional<VirtualBatchedCompoundCommit> getMaxPendingUploadBccBeforeGeneration(long generation) {
            return pendingUploadBccGenerations.values()
                .stream()
                .filter(vbcc -> vbcc.getPrimaryTermAndGeneration().generation() < generation)
                .max(Comparator.comparingLong(vbcc -> vbcc.getPrimaryTermAndGeneration().generation()));
        }

        /**
         * Add the given {@link StatelessCommitRef} to the current VBCC. If the current VBCC is null, a new
         * one will be created with the {@link StatelessCommitRef}'s generation and set to be the current
         * before appending.
         * @param reference The reference to be added
         * @return The VBCC that the given {@link StatelessCommitRef} has been added into.
         */
        private VirtualBatchedCompoundCommit appendCommit(StatelessCommitRef reference) {
            assert Thread.holdsLock(this);

            if (currentVirtualBcc == null) {
                final var newVirtualBcc = new VirtualBatchedCompoundCommit(
                    shardId,
                    ephemeralNodeIdSupplier.get(),
                    reference.getPrimaryTerm(),
                    reference.getGeneration(),
                    fileName -> getBlobLocation(shardId, fileName),
                    threadPool::relativeTimeInMillis,
                    cacheRegionSizeInBytes,
                    estimatedMaxHeaderSizeInBytes
                );
                final boolean appended = newVirtualBcc.appendCommit(reference, useInternalFilesReplicatedContent);
                assert appended : "append must be successful since the VBCC is new and empty";
                logger.trace(
                    () -> Strings.format(
                        "%s created new VBCC generation [%s]",
                        shardId,
                        newVirtualBcc.getPrimaryTermAndGeneration().generation()
                    )
                );
                currentVirtualBcc = newVirtualBcc;
            } else {
                final boolean appended = currentVirtualBcc.appendCommit(reference, useInternalFilesReplicatedContent);
                assert appended : "append must be successful since append and freeze have exclusive access";
                logger.trace(
                    () -> Strings.format(
                        "%s appended CC generation [%s] to VBCC generation [%s]",
                        shardId,
                        reference.getGeneration(),
                        currentVirtualBcc.getPrimaryTermAndGeneration().generation()
                    )
                );
            }

            var pendingCompoundCommit = currentVirtualBcc.getLastPendingCompoundCommit();
            var statelessCompoundCommit = pendingCompoundCommit.getStatelessCompoundCommit();
            var commitPrimaryTermAndGeneration = statelessCompoundCommit.primaryTermAndGeneration();

            assert commitPrimaryTermAndGeneration.primaryTerm() == reference.getPrimaryTerm();
            assert commitPrimaryTermAndGeneration.generation() == reference.getGeneration();

            Set<PrimaryTermAndGeneration> referencedBCCs = new HashSet<>(
                BatchedCompoundCommit.computeReferencedBCCGenerations(statelessCompoundCommit)
            );

            // For generational files used by local readers we must reference the original blob that contained them as Lucene might
            // delete the file and rely on the fact that in POSIX deleted files are kept around until the last process releases the
            // file handle, hence the generational files deletion tracking is not enough.
            for (Map.Entry<String, BlobLocation> entry : statelessCompoundCommit.commitFiles().entrySet()) {
                if (isGenerationalFile(entry.getKey()) && reference.getAdditionalFiles().contains(entry.getKey()) == false) {
                    CommitAndBlobLocation commitAndBlobLocation = blobLocations.get(entry.getKey());
                    if (commitAndBlobLocation != null) {
                        referencedBCCs.add(commitAndBlobLocation.blobReference.getPrimaryTermAndGeneration());
                    }
                }
            }
            var previousCommitReferencesInfo = commitReferencesInfos.put(
                commitPrimaryTermAndGeneration,
                new CommitReferencesInfo(currentVirtualBcc.getPrimaryTermAndGeneration(), Collections.unmodifiableSet(referencedBCCs))
            );

            assert previousCommitReferencesInfo == null
                : statelessCompoundCommit.primaryTermAndGeneration() + " was already present in " + commitReferencesInfos;

            return currentVirtualBcc;
        }

        /**
         * Freezes the current VBCC, creates the associated {@link BlobReference}, resets it back to null
         * and upload it if it is the same instance as the specified VBCC. Otherwise this method is a noop.
         * @param expectedVirtualBcc The expected current VBCC instance
         */
        private void maybeFreezeAndUploadCurrentVirtualBcc(VirtualBatchedCompoundCommit expectedVirtualBcc) {
            assert expectedVirtualBcc.getShardId().equals(shardId);

            final BlobReference blobReference;
            synchronized (this) {
                if (currentVirtualBcc != expectedVirtualBcc) {
                    assert expectedVirtualBcc.isFrozen();
                    assert assertGenerationIsUploadedOrPending(expectedVirtualBcc.getPrimaryTermAndGeneration().generation());
                    logger.trace(
                        () -> Strings.format(
                            "%s VBCC generation [%s] is concurrently frozen",
                            shardId,
                            expectedVirtualBcc.getPrimaryTermAndGeneration().generation()
                        )
                    );
                    return;
                }
                if (isClosed()) {
                    logger.debug(
                        () -> Strings.format(
                            "%s aborting freeze for VBCC generation [%s] [state=%s]",
                            shardId,
                            expectedVirtualBcc.getPrimaryTermAndGeneration().generation(),
                            state
                        )
                    );
                    return;
                }
                final boolean frozen = expectedVirtualBcc.freeze();
                assert frozen : "freeze must be successful since it is invoked exclusively";

                // Create the blobReference which updates blobLocations, init search nodes commit usage tracking.
                // We do this prior to adding to pending upload generations since we rely on this for search commit registration.
                blobReference = createBlobReference(expectedVirtualBcc);

                final var previous = pendingUploadBccGenerations.put(
                    expectedVirtualBcc.getPrimaryTermAndGeneration().generation(),
                    expectedVirtualBcc
                );
                assert previous == null : "expected null, but got " + previous;
                // reset after add to pending list so that vbcc is always visible as either pending or current
                currentVirtualBcc = null;
                logger.trace(
                    () -> Strings.format(
                        "%s reset current VBCC generation [%s] after freeze",
                        shardId,
                        expectedVirtualBcc.getPrimaryTermAndGeneration().generation()
                    )
                );
            }
            createAndRunCommitUpload(this, expectedVirtualBcc, blobReference);
        }

        protected boolean shouldUploadVirtualBcc(VirtualBatchedCompoundCommit virtualBcc) {
            return virtualBcc.getTotalSizeInBytes() >= bccUploadMaxSizeInBytes
                || virtualBcc.getPendingCompoundCommits().size() >= bccMaxAmountOfCommits;
        }

        private BlobReference createBlobReference(VirtualBatchedCompoundCommit virtualBcc) {
            assert isDeleted == false : "shard " + shardId + " is deleted when trying to add commit data";
            assert commitReferencesInfos.keySet().containsAll(virtualBcc.getPendingCompoundCommitGenerations())
                : "Commit references infos should be populated when new commits are appended";
            final var commitFiles = virtualBcc.getPendingCompoundCommits()
                .stream()
                .flatMap(pc -> pc.getCommitReference().getCommitFiles().stream())
                .collect(Collectors.toUnmodifiableSet());
            final var additionalFiles = virtualBcc.getPendingCompoundCommits()
                .stream()
                .flatMap(pc -> pc.getCommitReference().getAdditionalFiles().stream())
                .collect(Collectors.toUnmodifiableSet());

            var internalFiles = virtualBcc.getPendingCompoundCommits()
                .stream()
                // StatelessCompoundCommit#internalFiles contains additionalFiles + copied generational files
                .flatMap(pendingCompoundCommit -> pendingCompoundCommit.getStatelessCompoundCommit().internalFiles().stream())
                .collect(Collectors.toSet());
            final Set<String> trackedGenerationalFiles = additionalFiles.stream()
                .filter(StatelessCommitService::isGenerationalFile)
                .collect(Collectors.toUnmodifiableSet());

            // Generational files lifecycle is tracked independently, meaning that these are kept around until the generational file
            // is deleted by Lucene. Hence, we don't need to include the BlobReference in the referencedBCCs set that tracks the local
            // dependencies as we had to do before such tracking was introduced. Additionally, if a VBCC that's being uploaded needs
            // to read a generational file to copy it into the new blob, the VBCC holds a Lucene commit reference that guarantees that
            // the file won't be deleted until the upload finishes.
            var referencedBCCs = commitFiles.stream().filter(fileName -> internalFiles.contains(fileName) == false).peek(fileName -> {
                assert isGenerationalFile(fileName) == false;
            }).map(fileName -> {
                final var commitAndBlobLocation = blobLocations.get(fileName);
                if (commitAndBlobLocation == null) {
                    final var message = Strings.format(
                        """
                            [%s] blobLocations missing [%s]; \
                            primaryTerm=%d, generation=%d, commitFiles=%s, additionalFiles=%s, blobLocations=%s""",
                        shardId,
                        fileName,
                        virtualBcc.getPrimaryTermAndGeneration().primaryTerm(),
                        virtualBcc.getPrimaryTermAndGeneration().generation(),
                        commitFiles,
                        additionalFiles,
                        blobLocations.keySet()
                    );
                    assert false : message;
                    throw new IllegalStateException(message);
                }
                return commitAndBlobLocation.blobReference;
            }).collect(Collectors.toUnmodifiableSet());

            // create a compound commit blob instance for the new BCC
            var blobReference = new BlobReference(
                virtualBcc.getPrimaryTermAndGeneration(),
                additionalFiles,
                referencedBCCs,
                virtualBcc.getPendingCompoundCommitGenerations(),
                trackedGenerationalFiles,
                virtualBcc.getNotifiedSearchNodeIds()
            );

            if (primaryTermAndGenToBlobReference.putIfAbsent(blobReference.getPrimaryTermAndGeneration(), blobReference) != null) {
                throw new IllegalArgumentException(blobReference + " already exists");
            }

            // add pending blob locations for new files
            // Use getInternalLocations() to include copied generational files.
            virtualBcc.getInternalLocations().keySet().forEach(fileName -> {
                final BlobLocation blobLocation = virtualBcc.getBlobLocation(fileName);
                blobLocations.compute(fileName, (ignored, existing) -> {
                    if (existing == null) {
                        logger.trace("registering [{}]->[{}}]", fileName, blobReference.primaryTermAndGeneration);
                        return new CommitAndBlobLocation(blobReference, blobLocation);
                    } else {
                        assert isGenerationalFile(fileName)
                            && existing.blobLocation.compoundFileGeneration() < blobLocation.compoundFileGeneration()
                            : fileName + ':' + existing + ':' + blobLocation;
                        logger.trace(
                            "{} ignoring generational file [{}] updated location [{}]->[{}], keeping [{}]",
                            shardId,
                            fileName,
                            existing.blobLocation().getBatchedCompoundCommitTermAndGeneration(),
                            blobLocation.getBatchedCompoundCommitTermAndGeneration(),
                            existing.blobReference().getPrimaryTermAndGeneration()
                        );
                        return existing;
                    }
                });
            });
            return blobReference;
        }

        @Override
        public Set<PrimaryTermAndGeneration> resolveReferencedBCCsForCommit(long generation) {
            // It's possible that a background task such as force merge creates a commit after the shard
            // has relocated or closed and these commits are not appended into VBCCs nor uploaded.
            // In such cases we just respond back with an empty set since we're not tracking these.
            if (isClosed()) {
                return Set.of();
            }
            var commitReferencesInfo = getCommitReferencesInfoForGeneration(generation);
            return commitReferencesInfo.referencedBCCs();
        }

        private CommitReferencesInfo getCommitReferencesInfoForGeneration(long generation) {
            PrimaryTermAndGeneration primaryTermAndGeneration = resolvePrimaryTermForGeneration(generation);
            var commitReferencesInfo = commitReferencesInfos.get(primaryTermAndGeneration);
            assert commitReferencesInfo != null : commitReferencesInfos + " " + generation;
            return commitReferencesInfo;
        }

        public void markCommitDeleted(long generation) {
            var commitPrimaryTermAndGeneration = resolvePrimaryTermForGeneration(generation);
            var commitReferencesInfo = commitReferencesInfos.get(commitPrimaryTermAndGeneration);
            if (isClosed()) {
                return;
            }
            assert commitReferencesInfo != null;
            final var blobReference = primaryTermAndGenToBlobReference.get(commitReferencesInfo.storedInBCC());
            // This method should only be invoked after the VBCC has been uploaded, as it holds Lucene commit references. This ensures that
            // requests accessing the commit files can be served until the VBCC is uploaded to the blob store.
            if (blobReference == null) {
                throw new IllegalStateException(
                    Strings.format(
                        "Unable to mark commit with %s as deleted. "
                            + "recoveredGeneration: %s, recoveredPrimaryTerm: %s, allocationPrimaryTerm: %s, "
                            + "primaryTermAndGenToBlobReference: %s",
                        commitPrimaryTermAndGeneration,
                        recoveredGeneration,
                        recoveredPrimaryTerm,
                        allocationPrimaryTerm,
                        primaryTermAndGenToBlobReference
                    )
                );
            }
            blobReference.removeLocalCommitRef(commitPrimaryTermAndGeneration);
        }

        private PrimaryTermAndGeneration resolvePrimaryTermForGeneration(long generation) {
            return new PrimaryTermAndGeneration(
                generation == recoveredGeneration ? recoveredPrimaryTerm : allocationPrimaryTerm,
                generation
            );
        }

        public void markBccUploaded(BatchedCompoundCommit batchedCompoundCommit) {
            handleUploadedBcc(batchedCompoundCommit, true);
        }

        private void handleUploadedBcc(BatchedCompoundCommit uploadedBcc, boolean isUpload) {
            assert isDeleted == false : "shard " + shardId + " is deleted when trying to handle uploaded commit " + uploadedBcc;
            final long newBccGeneration = uploadedBcc.primaryTermAndGeneration().generation(); // for managing pending uploads
            final long newGeneration = uploadedBcc.lastCompoundCommit().generation(); // for notifying generation listeners

            // We use two synchronized blocks to ensure:
            // 1. Listeners are completed outside the synchronized blocks
            // 2. BCC upload consumers are triggered in generation order
            // 3. latestUploadedBcc, pendingUploadBccGenerations and generationListeners
            // are updated in a single synchronized block to avoid racing

            final List<ActionListener<UploadedBccInfo>> listenersToFire = new ArrayList<>();
            synchronized (this) {
                if (newBccGeneration <= getMaxUploadedBccGeneration()) {
                    if (isUpload) {
                        // Remove the BCC from the pending list regardless just in case
                        pendingUploadBccGenerations.remove(newBccGeneration);
                    }
                    assert false
                        : "out of order BCC generation ["
                            + newBccGeneration
                            + "] <= ["
                            + getMaxUploadedBccGeneration()
                            + "] for shard "
                            + shardId;
                    return;
                }

                if (uploadedBccConsumers != null) {
                    for (var consumer : uploadedBccConsumers) {
                        listenersToFire.add(ActionListener.wrap(consumer::accept, e -> {}));
                    }
                }
            }

            // From this point onwards the IndexDirectory can delete the local files and rely on the files that are uploaded
            // into the blob store, hence we just provide the Lucene file -> BlobLocation map for files that are already uploaded
            // as it's possible the blobLocations contains files from new commits that are not uploaded yet
            final Map<String, BlobFileRanges> uploadedFilesBlobLocations =
                // all blob locations uploaded since the shard started that are still in use
                blobLocations.entrySet()
                    .stream()
                    .filter(
                        blobLocationEntry -> blobLocationEntry.getValue()
                            .blobReference()
                            .getPrimaryTermAndGeneration()
                            .onOrBefore(uploadedBcc.primaryTermAndGeneration())
                    )
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> new BlobFileRanges(entry.getValue().blobLocation())));

            assert uploadedFilesBlobLocations.values()
                .stream()
                .allMatch(
                    blobLocation -> blobLocation.getBatchedCompoundCommitTermAndGeneration()
                        .onOrBefore(uploadedBcc.primaryTermAndGeneration())
                ) : uploadedFilesBlobLocations + " vs " + uploadedBcc.primaryTermAndGeneration() + " " + uploadedBcc;
            final var uploadedBccInfo = new UploadedBccInfo(uploadedBcc, uploadedFilesBlobLocations);
            // upload consumers must be triggered in generation order, hence trigger before removing from `pendingUploadBccGenerations`.
            Exception exception = null;
            try {
                ActionListener.onResponse(listenersToFire, uploadedBccInfo);
            } catch (Exception e) {
                assert false : e;
                exception = e;
            }

            listenersToFire.clear();
            synchronized (this) {
                assert newBccGeneration > getMaxUploadedBccGeneration()
                    : "out of order BCC generation ["
                        + newBccGeneration
                        + "] <= ["
                        + getMaxUploadedBccGeneration()
                        + "] for shard "
                        + shardId;
                latestUploadedBcc = uploadedBcc;
                if (isUpload) {
                    // Remove the BCC from the pending list *after* upload consumers but *before* generation listeners are fired
                    var removed = pendingUploadBccGenerations.remove(newBccGeneration);
                    assert removed != null : newBccGeneration + "not found";
                }
                if (generationListeners != null) {
                    List<Tuple<Long, ActionListener<Void>>> listenersToReregister = null;
                    for (Tuple<Long, ActionListener<Void>> tuple : generationListeners) {
                        Long generation = tuple.v1();
                        if (newGeneration >= generation) {
                            listenersToFire.add(tuple.v2().map(c -> null));
                        } else {
                            if (listenersToReregister == null) {
                                listenersToReregister = new ArrayList<>();
                            }
                            listenersToReregister.add(tuple);
                        }
                    }
                    generationListeners = listenersToReregister;
                }
            }

            // It is OK for generation listeners to be completed out of generation order
            try {
                ActionListener.onResponse(listenersToFire, uploadedBccInfo);
            } catch (Exception e) {
                assert false : e;
                if (exception != null) {
                    exception.addSuppressed(e);
                } else {
                    exception = e;
                }
            }

            if (exception != null) {
                throw (RuntimeException) exception;
            }
        }

        /**
         * Gets the max uploaded BCC primary term and generation, by accessing the latest uploaded {@link BatchedCompoundCommit}
         * without synchronization, or null if no upload has happened.
         */
        @Nullable
        public PrimaryTermAndGeneration getMaxUploadedBccTermAndGen() {
            return latestUploadedBcc == null ? null : latestUploadedBcc.primaryTermAndGeneration();
        }

        /**
         * Gets the max uploaded BCC generation, by accessing the latest uploaded {@link BatchedCompoundCommit}
         * without synchronization, or -1 if no upload has happened.
         */
        public long getMaxUploadedBccGeneration() {
            return latestUploadedBcc == null ? -1 : latestUploadedBcc.primaryTermAndGeneration().generation();
        }

        /**
         * Gets the max uploaded generation, i.e. the generation of the last CC in the BCC, by accessing the
         * latest uploaded {@link BatchedCompoundCommit} without synchronization, or -1 if no upload has happened.
         */
        public long getMaxUploadedGeneration() {
            return latestUploadedBcc == null ? -1 : latestUploadedBcc.lastCompoundCommit().generation();
        }

        /**
         * Ensures that the provided generation is uploaded or pending and returns null. Otherwise, returns an error message.
         */
        private @Nullable String isUploadedOrPending(long generation) {
            final var maxPendingUploadBcc = getMaxPendingUploadBcc();
            final var maxUploadedCcGen = getMaxUploadedGeneration();
            if (generation > maxUploadedCcGen) {
                final String message = format(
                    "generation [%s] is not covered by either maxUploadedGeneration [%s] or maxPendingUploadBcc [%s] on node [%s]",
                    generation,
                    maxUploadedCcGen,
                    maxPendingUploadBcc,
                    clusterService.localNode().getId()
                );
                assert maxPendingUploadBcc.isPresent() : message;
                final var maxPendingUploadCcGen = maxPendingUploadBcc.isPresent() ? maxPendingUploadBcc.get().getMaxGeneration() : -1L;
                if (generation > maxPendingUploadCcGen) {
                    return message;
                }
            }
            return null;
        }

        private boolean assertGenerationIsUploadedOrPending(long generation) {
            var errorMessage = isUploadedOrPending(generation);
            if (errorMessage != null) {
                assert false : errorMessage;
            }
            return true;
        }

        private void checkGenerationIsUploadedOrPending(long generation) {
            var errorMessage = isUploadedOrPending(generation);
            if (errorMessage != null) {
                assert false : errorMessage;
                throw new IllegalStateException(errorMessage);
            }
        }

        /**
         * Uploads a {@link BatchedCompoundCommit} via a callback uploadListener when the commit is ready for upload.
         */
        private void runUploadWhenCommitIsReady(
            ActionListener<Void> uploadListener,
            ActionListener<BatchedCompoundCommit> listener,
            long generation
        ) {
            Optional<VirtualBatchedCompoundCommit> optVBCC = getMaxPendingUploadBccBeforeGeneration(generation);
            if (optVBCC.isPresent() == false) {
                // Run the upload.
                uploadListener.onResponse(null);
            } else {
                long vbccGeneration = optVBCC.get().getPrimaryTermAndGeneration().generation();
                logger.trace("{} waiting for commit [{}] to finish before uploading commit [{}]", shardId, vbccGeneration, generation);
                addListenerForUploadedGeneration(vbccGeneration, listener.delegateFailure((unusedListener, unusedResponse) -> {
                    assert pendingUploadBccGenerations.containsKey(vbccGeneration) == false
                        : "missingGeneration [" + vbccGeneration + "] still in " + pendingUploadBccGenerations.keySet();
                    runUploadWhenCommitIsReady(uploadListener, listener, generation);
                }));
            }
        }

        private void sendNewCommitNotification(
            IndexShardRoutingTable shardRoutingTable,
            StatelessCompoundCommit lastCompoundCommit,
            long batchedCompoundCommitGeneration,
            PrimaryTermAndGeneration maxUploadedBccTermAndGen
        ) {
            assert isRelocating() == false || lastCompoundCommit.generation() <= maxGenerationToUpload
                : "Request generation="
                    + lastCompoundCommit.generation()
                    + " maxGenerationToUpload="
                    + maxGenerationToUpload
                    + " state="
                    + state;

            lastNewCommitNotificationSentTimestamp = threadPool.relativeTimeInMillis();
            statelessCommitNotificationPublisher.sendNewCommitNotification(
                shardRoutingTable,
                lastCompoundCommit,
                batchedCompoundCommitGeneration,
                maxUploadedBccTermAndGen,
                clusterService.state().version(),
                clusterService.localNode().getId(),
                ActionListener.wrap(Void -> {
                    // Do NOT update uploadedGenerationNotified since it is used for file deleting tracking
                    // TODO: Process the response for old commits that are no-longer-in-use similar to how it is done on upload
                    // notification
                    notifyCommitNotificationSuccessListeners(lastCompoundCommit.generation());
                }, e -> logNotificationException(lastCompoundCommit.generation(), batchedCompoundCommitGeneration, "create", e))
            );
        }

        private void notifyCommitNotificationSuccessListeners(long compoundCommitGeneration) {
            var consumer = commitNotificationSuccessListeners.get(shardId);
            if (consumer != null) {
                consumer.accept(compoundCommitGeneration);
            }
        }

        /**
         * Broadcasts notification of a new uploaded BCC to all the search nodes hosting a replica shard for the given shard commit.
         */
        private void sendNewUploadedCommitNotification(BlobReference blobReference, BatchedCompoundCommit uploadedBcc) {
            assert uploadedBcc != null;

            var notificationCommitGeneration = uploadedBcc.lastCompoundCommit().generation();
            var notificationCommitBCCDependencies = resolveReferencedBCCsForCommit(notificationCommitGeneration);
            Optional<IndexShardRoutingTable> shardRoutingTable = shardRoutingFinder.apply(uploadedBcc.shardId());
            Optional<Set<String>> optCurrentRoutingNodesWithAssignedSearchShards = shardRoutingTable.map(
                routingTable -> routingTable.unpromotableShards()
                    .stream()
                    .filter(ShardRouting::assignedToNode)
                    .map(ShardRouting::currentNodeId)
                    .collect(Collectors.toSet())
            );

            var allSearchNodesRetainingCommits = getAllSearchNodesRetainingCommits();
            // TODO (ES-9638): optCurrentRoutingNodesWithAssignedSearchShards.isEmpty() checks the Optional, not whether the set is empty.
            if (allSearchNodesRetainingCommits.isEmpty() && optCurrentRoutingNodesWithAssignedSearchShards.isEmpty()) {
                // No search nodes hold shard commit references.
                // Initializing or deleting the index.

                // This is a noop, but do it for completeness anyway.
                trackOutstandingUnpromotableShardCommitRef(Set.of(), blobReference);
                lastNewCommitNotificationSentTimestamp = threadPool.relativeTimeInMillis();

                onNewUploadedCommitNotificationResponse(
                    Set.of(),
                    uploadedBcc.primaryTermAndGeneration().generation(),
                    notificationCommitGeneration,
                    notificationCommitBCCDependencies,
                    Set.of()
                );

                return;
            }

            Set<String> currentRoutingNodesWithAssignedSearchShards = optCurrentRoutingNodesWithAssignedSearchShards.orElse(Set.of());

            // We may not have currently assigned shards, or even an index, but it is possible that we still have some search nodes that
            // recently held search shards that may still be actively using shard commits. We'll send out requests to the old search nodes
            // as well as any current search nodes.
            trackOutstandingUnpromotableShardCommitRef(currentRoutingNodesWithAssignedSearchShards, blobReference);
            lastNewCommitNotificationSentTimestamp = threadPool.relativeTimeInMillis();

            statelessCommitNotificationPublisher.sendNewUploadedCommitNotificationAndFetchInUseCommits(
                shardRoutingTable.isPresent() ? shardRoutingTable.get() : null,
                currentRoutingNodesWithAssignedSearchShards,
                allSearchNodesRetainingCommits,
                uploadedBcc,
                clusterService.state().version(),
                clusterService.localNode().getId(),
                clusterService,
                ActionListener.wrap(searchNodesAndCommitsResult -> {
                    onNewUploadedCommitNotificationResponse(
                        searchNodesAndCommitsResult.allSearchNodes(),
                        uploadedBcc.primaryTermAndGeneration().generation(),
                        notificationCommitGeneration,
                        notificationCommitBCCDependencies,
                        searchNodesAndCommitsResult.commitsInUse()
                    );
                },
                    e -> logNotificationException(
                        notificationCommitGeneration,
                        uploadedBcc.primaryTermAndGeneration().generation(),
                        "upload",
                        e
                    )
                )
            );
        }

        private void logNotificationException(long generation, long bccGeneration, String verb, Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof ConnectTransportException) {
                logger.debug(
                    () -> format(
                        "%s failed to notify search shards after " + verb + " commit of gen [%s] (BCC [%s]) due to connection issues",
                        shardId,
                        generation,
                        bccGeneration
                    ),
                    e
                );
            } else {
                logger.warn(
                    () -> format(
                        "%s failed to notify search shards after " + verb + " commit of gen [%s] (BCC [%s])",
                        shardId,
                        generation,
                        bccGeneration
                    ),
                    e
                );
            }
        }

        /**
         * Asks all the search shards what commits they are still actively using for reads. Only sends out requests if the index shard is
         * still maintaining commits older than the latest one, when there is the potential to delete old commits found to be no longer in
         * use. Any old commits newly discovered to have been released by all search shards can be flagged for deletion.
         */
        private void pollSearchShardsForInUseOldCommits() {
            final BatchedCompoundCommit latestBatchedCompoundCommitUploaded;
            final BlobReference latestBlobReference;
            final CommitReferencesInfo latestCommitReferencesInfo;

            // Get latest uploaded stateless compound commit and the respective compound commit blob
            synchronized (this) {
                if (latestUploadedBcc == null) {
                    return;
                }
                latestBatchedCompoundCommitUploaded = latestUploadedBcc;
                PrimaryTermAndGeneration termGen = latestBatchedCompoundCommitUploaded.primaryTermAndGeneration();
                latestBlobReference = primaryTermAndGenToBlobReference.get(termGen);
                assert latestBlobReference != null : "could not find latest " + termGen + " in compound commit blobs";
                latestCommitReferencesInfo = commitReferencesInfos.get(
                    latestBatchedCompoundCommitUploaded.lastCompoundCommit().primaryTermAndGeneration()
                );
                assert latestCommitReferencesInfo != null
                    : "Unable to get latest commit reference info from " + latestBatchedCompoundCommitUploaded;
            }

            final boolean anyOldBlobReferencesStillInUse = primaryTermAndGenToBlobReference.values()
                .stream()
                .anyMatch(
                    blobReference -> latestCommitReferencesInfo.referencesBCC(blobReference.primaryTermAndGeneration) == false
                        && blobReference.isExternalReadersClosed() == false
                );

            // Resend the notification if older blob references are still in use
            if (anyOldBlobReferencesStillInUse) {
                logger.debug("sending new commit notifications for inactive or routing changed shard [{}]", shardId);
                sendNewUploadedCommitNotification(latestBlobReference, latestBatchedCompoundCommitUploaded);
            } else {
                lastNewCommitNotificationSentTimestamp = threadPool.relativeTimeInMillis();
            }
        }

        /**
         * Register a listener that is invoked once a BCC containing CC with the given generation has been uploaded to the object store.
         * The listener is invoked only once.
         *
         * @param generation the commit generation
         * @param listener the listener
         */
        private void addListenerForUploadedGeneration(long generation, ActionListener<Void> listener) {
            boolean completeListenerSuccess = false;
            boolean completeListenerClosed = false;
            synchronized (this) {
                if (isClosed()) {
                    completeListenerClosed = true;
                } else if (getMaxUploadedGeneration() >= generation) {
                    // TODO: different listeners may want ccGen or bccGen and should be handled separately. See also ES-8261
                    // Location already visible, just call the listener
                    completeListenerSuccess = true;
                } else {
                    List<Tuple<Long, ActionListener<Void>>> listeners = generationListeners;
                    ActionListener<Void> contextPreservingListener = ContextPreservingActionListener.wrapPreservingContext(
                        listener,
                        threadPool.getThreadContext()
                    );
                    if (listeners == null) {
                        listeners = new ArrayList<>();
                    }
                    listeners.add(new Tuple<>(generation, contextPreservingListener));
                    generationListeners = listeners;
                }
            }

            if (completeListenerClosed) {
                if (relocated) {
                    listener.onFailure(new UnavailableShardsException(shardId, "shard relocated"));
                } else {
                    listener.onFailure(new AlreadyClosedException("shard [" + shardId + "] has already been closed"));
                }
            } else if (completeListenerSuccess) {
                listener.onResponse(null);
            }
        }

        /**
         * Register a consumer that is invoked everytime a new commit has been uploaded to the object store
         * @param consumer the consumer
         */
        public void addConsumerForNewUploadedBcc(Consumer<UploadedBccInfo> consumer) {
            synchronized (this) {
                if (isClosed() == false) {
                    if (uploadedBccConsumers == null) {
                        uploadedBccConsumers = new ArrayList<>();
                    }
                    uploadedBccConsumers.add(consumer);
                }
            }
        }

        private List<ActionListener<Void>> closeAndGetListeners() {
            List<Tuple<Long, ActionListener<Void>>> listenersToFail;
            synchronized (this) {
                if (isClosed()) {
                    return Collections.emptyList();
                }
                state = State.CLOSED;
                listenersToFail = generationListeners;
                generationListeners = null;
                uploadedBccConsumers = null;
            }

            if (listenersToFail != null) {
                return listenersToFail.stream().map(Tuple::v2).collect(Collectors.toList());
            } else {
                return Collections.emptyList();
            }
        }

        private void close() {
            List<ActionListener<Void>> listenersToFail = closeAndGetListeners();
            final var virtualBcc = currentVirtualBcc;
            if (virtualBcc != null) {
                // Release commit references held by the current VBCC
                // TODO: maybe upload before releasing in some cases as a future optimization?
                IOUtils.closeWhileHandlingException(virtualBcc);
            }

            if (listenersToFail.isEmpty() == false) {
                // Have to fork, because we are on applier thread and thus if a listener uses cluster state it will fail.
                // using generic is safe, since we close all shards (but not the stores) before shutting down thread pools.
                threadPool.generic()
                    .execute(
                        () -> ActionListener.onFailure(
                            listenersToFail,
                            new AlreadyClosedException("shard [" + shardId + "] has already been closed")
                        )
                    );
            }
        }

        private void unregistered() {
            if (isDeleted) {
                // clear all unpromotable references
                updateUnpromotableShardAssignedNodes(Set.of(), Long.MAX_VALUE, Set.of());
                primaryTermAndGenToBlobReference.values().forEach(blobReference -> {
                    blobReference.closedLocalReaders();
                    blobReference.removeAllLocalCommitsRefs();
                    blobReference.removeAllGenerationalFilesRefs();
                });
            }
        }

        private void markRelocating(long minRelocatedGeneration, ActionListener<Void> listener) {
            long toWaitFor;
            synchronized (this) {
                // We wait for the max generation we see at the moment to be uploaded. Generations are always uploaded in order so this
                // logic works. Additionally, at minimum we wait for minRelocatedGeneration to be uploaded. It is possible it has already
                // been uploaded which would make the listener be triggered immediately.
                ensureMaxGenerationToUploadForFlush(minRelocatedGeneration);
                toWaitFor = getMaxPendingUploadBcc().map(VirtualBatchedCompoundCommit::getMaxGeneration).orElse(minRelocatedGeneration);
                assert toWaitFor >= minRelocatedGeneration : toWaitFor + " < " + minRelocatedGeneration;
                assert assertGenerationIsUploadedOrPending(toWaitFor);

                maxGenerationToUpload = toWaitFor;
                assert state == State.RUNNING;
                state = State.RELOCATING;
            }

            addListenerForUploadedGeneration(toWaitFor, listener);
        }

        /**
         * This method will mark a shard as fully relocated. At this point, no more commits can ever be uploaded for this instance of shard
         * commit state.
         */
        private void markRelocated() {
            List<ActionListener<Void>> listenersToFail;
            synchronized (this) {
                if (state != State.CLOSED) {
                    relocated = true;
                    listenersToFail = closeAndGetListeners();
                } else {
                    listenersToFail = Collections.emptyList();
                }
            }

            ActionListener.onFailure(listenersToFail, new UnavailableShardsException(shardId, "shard relocated"));
        }

        /**
         * This method will transition the shard back to RUNNING and allow new generations to be uploaded.
         */
        private void markRelocationFailed() {
            synchronized (this) {
                if (state != State.CLOSED) {
                    assert state == State.RELOCATING;
                    maxGenerationToUpload = Long.MAX_VALUE;
                    state = State.RUNNING;
                }
            }
        }

        public long getMaxGenerationToUploadForFlush() {
            return maxGenerationToUploadForFlush.get();
        }

        /**
         * Marks the shard as deleted. Any related {@link ShardCommitState.BlobReference} will be deleted in the upcoming {@link #close()}.
         */
        public void delete() {
            // idempotent
            synchronized (this) {
                isDeleted = true;
            }
        }

        /**
         * Updates the internal state with the latest information on what commits are still in use by search nodes.
         *
         * @param searchNodes combined set of all the search nodes with shard replicas.
         * @param bccNotificationGeneration the BCC generation used for the new commit notification
         * @param commitNotificationGeneration the commit generation used for the new commit notification
         * @param notificationCommitBCCDependencies the set of BCC dependencies for the commit sent in the new commit notification
         * @param primaryTermAndGenerationsInUse combined set of all the PrimaryTermAndGeneration commits in use by all search shards.
         */
        void onNewUploadedCommitNotificationResponse(
            Set<String> searchNodes,
            long bccNotificationGeneration,
            long commitNotificationGeneration,
            Set<PrimaryTermAndGeneration> notificationCommitBCCDependencies,
            Set<PrimaryTermAndGeneration> primaryTermAndGenerationsInUse
        ) {
            if (state == State.CLOSED) {
                return;
            }

            if (uploadedGenerationNotified.getAndAccumulate(commitNotificationGeneration, Math::max) > commitNotificationGeneration) {
                // no need to process backwards commit notifications
                // (but would be safe - and we rely on it, this check is just an optimistic check)
                return;
            }

            for (BlobReference blobReference : primaryTermAndGenToBlobReference.values()) {
                // we are allowed to shrink the set of search nodes for any generation <= notificationGeneration, since after the
                // notification generation has been refreshed on the search shard, we know that the shard will never add more use of any
                // earlier generations.
                if (blobReference.getPrimaryTermAndGeneration().generation() <= bccNotificationGeneration
                    && primaryTermAndGenerationsInUse.contains(blobReference.getPrimaryTermAndGeneration()) == false) {
                    // remove nodes from the search nodes set. Any search shard registered during initialization will be left until it
                    // starts responding.
                    blobReference.removeSearchNodes(searchNodes, bccNotificationGeneration, notificationCommitBCCDependencies);
                }
            }
        }

        /**
         * Collects all the search nodes that retain commits for this shard into a set.
         */
        private Set<String> getAllSearchNodesRetainingCommits() {
            return primaryTermAndGenToBlobReference.values()
                .stream()
                .map(blobReference -> blobReference.searchNodesRef.get())
                .filter(Objects::nonNull)
                .flatMap(nodes -> nodes.stream())
                .collect(Collectors.toSet());
        }

        void updateUnpromotableShardAssignedNodes(Set<String> currentUnpromotableNodes) {
            var bccGenerationAndDependencies = getUploadedNotifiedGenerationAndDependencies();
            updateUnpromotableShardAssignedNodes(
                currentUnpromotableNodes,
                bccGenerationAndDependencies.v1(),
                bccGenerationAndDependencies.v2()
            );
        }

        void updateUnpromotableShardAssignedNodes(
            Set<String> currentUnpromotableNodes,
            long bccNotificationGeneration,
            Set<PrimaryTermAndGeneration> generationNotifiedBCCDependencies
        ) {
            for (BlobReference blobReference : primaryTermAndGenToBlobReference.values()) {
                blobReference.retainSearchNodes(currentUnpromotableNodes, bccNotificationGeneration, generationNotifiedBCCDependencies);
            }
        }

        void onRemoveNodesFromCluster(Set<String> removedNodeIds) {
            var bccGenerationAndDependencies = getUploadedNotifiedGenerationAndDependencies();
            for (BlobReference blobReference : primaryTermAndGenToBlobReference.values()) {
                blobReference.removeSearchNodes(removedNodeIds, bccGenerationAndDependencies.v1(), bccGenerationAndDependencies.v2());
            }
        }

        private Tuple<Long, Set<PrimaryTermAndGeneration>> getUploadedNotifiedGenerationAndDependencies() {
            long generationNotified = this.uploadedGenerationNotified.get();
            final long bccGeneration;
            final Set<PrimaryTermAndGeneration> generationNotifiedBCCDependencies;
            if (generationNotified == EMPTY_GENERATION_NOTIFIED_SENTINEL) {
                bccGeneration = generationNotified;
                generationNotifiedBCCDependencies = Set.of();
            } else {
                var commitReferencesInfo = getCommitReferencesInfoForGeneration(generationNotified);
                bccGeneration = commitReferencesInfo.storedInBCC().generation();
                generationNotifiedBCCDependencies = commitReferencesInfo.referencedBCCs();
            }
            return new Tuple<>(bccGeneration, generationNotifiedBCCDependencies);
        }

        /**
         * Registers the given set of 'nodes' in the {@link BlobReference} for the specified 'commitReference'.
         */
        void trackOutstandingUnpromotableShardCommitRef(Set<String> nodes, BlobReference commitReference) {
            boolean success = registerUnpromoteableCommitRefs(nodes, commitReference);
            // it is fine if a newer commit notification removed the registration, since then blobReference cannot be used
            // by search shard readers anymore.
            // Sometimes shard can be deleted (see `isDeleted` flag) before new commit upload message is sent out
            // hence registerUnpromoteableCommitRefs can return false since the BlobReference search node sets has been cleared by the index
            // deletion
            assert success || isDeleted || commitReference.getPrimaryTermAndGeneration().generation() < uploadedGenerationNotified.get();
        }

        /**
         * Register commit used by unpromotable, returning the commit to use by the unpromotable.
         */
        void registerCommitForUnpromotableRecovery(
            @Nullable PrimaryTermAndGeneration batchedCompoundGeneration,
            PrimaryTermAndGeneration compoundCommitGeneration,
            String nodeId,
            ActionListener<RegisterCommitResponse> listener
        ) {
            if (isClosed()) {
                // If the shard is concurrently relocated, throwing exception to make search node retry after getting a new cluster state
                throw new ShardNotFoundException(shardId, "shard commit state is " + (relocated ? "relocated" : "closed"));
            }
            // If the indexing shard is not finished initializing from the object store, we are not
            // able to register the commit for recovery. For now, fail the registration request.
            // TODO: we should be able to handle this case by either retrying the registration or keep the
            // registration and run it after the indexing shard is finished initializing.
            // TODO We should force a flush at the end of the indexing shard recovery so that latestUploadedBcc will be non null after
            // shard is initialized (ES-8327)

            // Consider minimum pending generation also available to search, since we are in the process of uploading it and search
            // node may have seen it before we process that the upload succeeded. This is also reasonable, because only timing
            // means it is not yet considered uploaded here.
            Optional<VirtualBatchedCompoundCommit> latestUploading = pendingUploadBccGenerations.values()
                .stream()
                .min(Comparator.comparing(VirtualBatchedCompoundCommit::getPrimaryTermAndGeneration));
            var latestUploaded = this.latestUploadedBcc;
            if (latestUploaded == null) {
                throw new NoShardAvailableActionException(shardId, "indexing shard is initializing");
            }
            // onOrAfter (really just "on", see next if block) means the shard *is* uploaded, since the search shard saw it
            AbstractBatchedCompoundCommit availableBcc = latestUploading.map(o -> (AbstractBatchedCompoundCommit) o)
                .filter(bcc -> compoundCommitGeneration.onOrAfter(bcc.lastCompoundCommit().primaryTermAndGeneration()))
                .orElse(latestUploaded);
            var availableCommit = availableBcc.lastCompoundCommit();
            if (compoundCommitGeneration.after(availableCommit.primaryTermAndGeneration())) {
                throw new RecoveryCommitTooNewException(
                    shardId,
                    "Compound commit "
                        + compoundCommitGeneration
                        + " used for registration is newer than the uploaded compound commit "
                        + availableCommit.toShortDescription()
                        + " in batch "
                        + availableBcc.primaryTermAndGeneration()
                        + " from "
                        + (latestUploading.isEmpty() ? "latest" : "pending")
                        + " uploaded commit"
                );
            }

            // search shard is on a node that does not register with a BCC generation (so it does not support recovering from a VBCC
            // and should use the last uploaded BCC)
            if (batchedCompoundGeneration == null) {
                registerLastUploadedBccForUnpromotableRecovery(Set.of(nodeId), availableBcc, listener);
            } else {
                registerVirtualBccForUnpromotableRecovery(Set.of(nodeId), availableBcc, listener);
            }
        }

        /**
         * Register the virtual batched compound commit as the commit to use for the unpromotable shard recovery.
         * <p>
         * If a VBCC exists at the time this method is called, then the latest appended commit of that VBCC is retrieved to compute a list
         * of referenced BCCs to retain during the recovery. The method then tries to register the {@code nodeId} for every referenced BCC
         * (using {@link #registerUnpromoteableCommitRefs(Set, BlobReference)}). If that works a registration response is returned to the
         * listener, with the latest uploaded BCC term/generation and a compound commit to use from the VBCC. Otherwise the method retries.
         *
         * @param nodeIds      a set containing the search shard's node id
         * @param availableBcc a commit that is uploaded/available, but not necessarily yet in latestUploadedBcc
         * @param listener     the listener to receive the {@link RegisterCommitResponse}
         */
        private void registerVirtualBccForUnpromotableRecovery(
            Set<String> nodeIds,
            AbstractBatchedCompoundCommit availableBcc,
            ActionListener<RegisterCommitResponse> listener
        ) {
            while (true) {
                var virtual = getCurrentOrPendingUploadVirtualBcc();
                if (virtual == null || isClosed()) {
                    break;
                }
                final var virtualPrimaryTermAndGeneration = virtual.getPrimaryTermAndGeneration();
                final var virtualPendingCompoundCommit = virtual.getLastPendingCompoundCommit();
                final var virtualCompoundCommit = virtualPendingCompoundCommit.getStatelessCompoundCommit();

                var referencedPrimaryTermAndGenerations = BatchedCompoundCommit.computeReferencedBCCGenerations(virtualCompoundCommit)
                    .stream()
                    // Exclude the virtual compound commit that is pending upload from the list of referenced BCC term/generations because:
                    // - it does not exist in the object store yet (and therefore does not need to be retained for deletion)
                    // - the search shard will be registered against this virtual compound commit the next time a new commit notification
                    // is sent (see trackOutstandingUnpromotableShardCommitRef in sendNewUploadedCommitNotificationAndFetchInUseCommits)
                    .filter(primaryTermAndGeneration -> primaryTermAndGeneration.before(virtualPrimaryTermAndGeneration))
                    .collect(Collectors.toSet());
                if (registerForUnpromotableRecovery(referencedPrimaryTermAndGenerations, nodeIds)) {
                    var maxSeqNo = virtualPendingCompoundCommit.getMaxSeqNo();
                    var response = new RegisterCommitResponse(
                        PrimaryTermAndGeneration.max(latestUploadedBcc.primaryTermAndGeneration(), availableBcc.primaryTermAndGeneration()),
                        virtualCompoundCommit
                    );
                    addGlobalCheckpointListener(
                        addGlobalCheckpointListenerFunction,
                        triggerTranslogReplicator,
                        maxSeqNo,
                        listener.map(ignored -> response)
                    );
                    return;
                }
            }
            // fall back to the last uploaded BCC if VBCC do not exist yet
            registerLastUploadedBccForUnpromotableRecovery(nodeIds, availableBcc, listener);
        }

        /**
         * Register the last uploaded batched compound commit as the commit to use for the unpromotable shard recovery.
         *
         * @param nodeIds      a set containing the search shard's node id
         * @param availableBcc a commit that is uploaded/available, but not necessarily yet in latestUploadedBcc
         * @param listener     the listener to receive the {@link RegisterCommitResponse}
         */
        private void registerLastUploadedBccForUnpromotableRecovery(
            Set<String> nodeIds,
            AbstractBatchedCompoundCommit availableBcc,
            ActionListener<RegisterCommitResponse> listener
        ) {
            assert availableBcc != null;
            ActionListener.completeWith(listener, () -> {
                AbstractBatchedCompoundCommit latest = availableBcc;
                long previousGenerationUploaded = -1L;
                do {
                    if (isClosed()) {
                        break;
                    }
                    assert latest.primaryTermAndGeneration().generation() > 0;
                    assert latest.primaryTermAndGeneration().generation() > previousGenerationUploaded;
                    assert latest.primaryTermAndGeneration().primaryTerm() == allocationPrimaryTerm
                        || latest.primaryTermAndGeneration()
                            .equals(new PrimaryTermAndGeneration(recoveredPrimaryTerm, recoveredGeneration));

                    var referencedPrimaryTermAndGenerations = BatchedCompoundCommit.computeReferencedBCCGenerations(
                        latest.lastCompoundCommit()
                    );
                    if (registerForUnpromotableRecovery(referencedPrimaryTermAndGenerations, nodeIds)) {
                        return new RegisterCommitResponse(latest.primaryTermAndGeneration(), latest.lastCompoundCommit());
                    }
                    previousGenerationUploaded = latest.primaryTermAndGeneration().generation();
                } while ((latest = this.latestUploadedBcc) != null);
                throw new NoShardAvailableActionException(shardId, "Indexing shard is closed");
            });
        }

        private boolean registerForUnpromotableRecovery(Set<PrimaryTermAndGeneration> primaryTermAndGenerations, Set<String> nodeIds) {
            assert nodeIds != null && nodeIds.size() == 1;
            int registered = 0;
            for (var primaryTermAndGeneration : primaryTermAndGenerations) {
                var blobReference = primaryTermAndGenToBlobReference.get(primaryTermAndGeneration);
                if (blobReference != null && registerUnpromoteableCommitRefs(nodeIds, blobReference)) {
                    // it is ok to register a search shard against the BlobReference here even if it fails afterward for the other
                    // references: the next new commit notification will register all search shards and remove stale references.
                    assert blobReference.isExternalReadersClosed() == false;
                    registered++;
                } else {
                    logger.info(
                        "unable to lock down {} out of {} for blob {}",
                        primaryTermAndGeneration,
                        primaryTermAndGenerations,
                        blobReference
                    );
                    break;
                }
            }
            return registered == primaryTermAndGenerations.size();
        }

        /**
         * Adds the given 'nodes' to the {@link BlobReference} for 'compoundCommit'.
         */
        boolean registerUnpromoteableCommitRefs(Set<String> nodes, BlobReference compoundCommit) {
            Set<String> result = compoundCommit.addSearchNodes(nodes);
            return result != null;
        }

        @Override
        public void onLocalReaderClosed(long bccHoldingClosedCommit, Set<PrimaryTermAndGeneration> remainingReferencedBCCs) {
            for (BlobReference blobReference : primaryTermAndGenToBlobReference.values()) {
                if (remainingReferencedBCCs.contains(blobReference.getPrimaryTermAndGeneration()) == false
                    // Ensure that we don't close the latest reference when we've populated the blob reference but the local reader hasn't
                    // opened yet, meaning that's not part of the remainingReferencedBCCs
                    && blobReference.getPrimaryTermAndGeneration().generation() <= bccHoldingClosedCommit) {
                    blobReference.closedLocalReaders();
                }
            }
        }

        private void onGenerationalFileDeletion(String filename) {
            assert isGenerationalFile(filename) : filename + " is not a generational file";
            logger.trace(() -> format("%s deleted generational file [%s]", shardId, filename));
            var blobLocation = blobLocations.get(filename);
            if (blobLocation != null) { // can be null if the generation file was never commited
                assert blobLocation.blobReference != null;
                blobLocation.blobReference().removeGenerationalFileRef(filename);
            }
        }

        /**
         * A ref counted instance representing a (BCC) blob reference to the object store.
         */
        private class BlobReference extends AbstractRefCounted {
            private final PrimaryTermAndGeneration primaryTermAndGeneration;
            private final Set<PrimaryTermAndGeneration> includedCommitGenerations;
            private final Set<String> internalFiles;
            private volatile Set<BlobReference> references;
            private final AtomicBoolean readersClosed = new AtomicBoolean();
            /**
             * Set to track the commits that are opened locally by Lucene.
             * <ol>
             *     <li>Initially created with all the includedCommitGenerations that represent the CCs stored in this BCC.</li>
             *     <li> When a commit included in this BCC is locally deleted by Lucene.
             *     {@link #removeLocalCommitRef(PrimaryTermAndGeneration)} is called and the deleted element is removed from the set.
             *     </li>
             *     <li>{@link #removeAllLocalCommitsRefs()} can be called concurrently when the index is deleted,
             *     therefore we should take into account that multiple threads can compete to mark this BCC as locally deleted.</li>
             *     <li> When localCommitsRef is empty, using getAndUpdate (since AtomicReference uses == for equality checks)
             *     set the reference to null.When the Thread is able to successfully do the CAS operation from Set.of() -> null we decRef
             *     this instance and the referenced instances.
             *     </li>
             * </ol>
             */
            private final AtomicReference<Set<PrimaryTermAndGeneration>> localCommitsRef;
            /**
             * Set of search node-ids using the commit. The lifecycle of entries is like this:
             * 1. Initially created at instantiation on recovery or commit created - with an empty set.
             * 2. Add to set of nodes before sending commit notification or when search shard registers during its initialization.
             *    Only these two actions add to the set of node ids.
             * 3. Remove from set of nodes when receiving commit notification response.
             * 4. Remove from set of nodes when a new cluster state indicates a search shard is no longer allocated.
             * 5. When nodes is empty, using getAndUpdate to atomically set the reference to null.
             *    When successful this dec-refs the BlobReference's external reader ref-count.
             */
            private final AtomicReference<Set<String>> searchNodesRef;

            /**
             * Set to track the generational files that are stored in this blob and are opened locally by Lucene.
             * Generational files are copied to new blobs referencing it, but the index nodes use the first blob where they were stored
             * (this can be the blob where the file was included for the first time or the recovered blob after a recovery)
             * until the file is deleted by Lucene. Hence, this set only contains new generational files (not copied generational files) or
             * with all the generational files that belong to a recovered commit.
             */
            private final Set<String> trackedGenerationalFiles;
            /**
             * Reference that tracks the generational files that are not deleted yet
             * <ol>
             *     <li>Initially created with all the {@link #trackedGenerationalFiles} that represent
             *     the generational files must be tracked in this blob or {@code null} (the terminal state)
             *     when the blob does not contain any generational files.
             *     </li>
             *     <li> When a generational file included in this BCC is locally deleted by Lucene;
             *     {@link #removeGenerationalFileRef(String)} is called and the deleted generational file is removed from the set.
             *     </li>
             *     <li>{@link #removeAllGenerationalFilesRefs()}} can be called concurrently when the index is deleted,
             *     therefore we should take into account that multiple threads can compete to mark
             *     generational files as locally deleted.</li>
             *     <li> When generationalFilesRef is empty, using getAndUpdate (since AtomicReference uses == for equality checks)
             *     set the reference to null.When the Thread is able to successfully do the CAS operation from Set.of() -> null we decRef
             *     this instance and the referenced instances.
             *     </li>
             * </ol>
             */
            private final AtomicReference<Set<String>> generationalFilesRef;

            BlobReference(
                PrimaryTermAndGeneration primaryTermAndGeneration,
                Set<String> internalFiles,
                Set<BlobReference> references,
                Set<PrimaryTermAndGeneration> includedCommitGenerations,
                Set<String> trackedGenerationalFiles
            ) {
                this(primaryTermAndGeneration, internalFiles, references, includedCommitGenerations, trackedGenerationalFiles, Set.of());
            }

            BlobReference(
                PrimaryTermAndGeneration primaryTermAndGeneration,
                Set<String> internalFiles,
                Set<BlobReference> references,
                Set<PrimaryTermAndGeneration> includedCommitGenerations,
                Set<String> trackedGenerationalFiles,
                Set<String> searchNodes
            ) {
                this.primaryTermAndGeneration = primaryTermAndGeneration;
                this.internalFiles = Set.copyOf(internalFiles);
                this.references = references;
                this.includedCommitGenerations = Set.copyOf(includedCommitGenerations);
                this.localCommitsRef = new AtomicReference<>(Set.copyOf(includedCommitGenerations));
                this.trackedGenerationalFiles = Set.copyOf(trackedGenerationalFiles);
                searchNodesRef = new AtomicReference<>(searchNodes);
                // we both decRef closedLocalReaders, closedExternalReaders and removeGenerationalFileRef,
                // hence the extra incRefs (in addition to the 1 ref given by AbstractRefCounted constructor)
                this.incRef();
                this.incRef();
                if (trackedGenerationalFiles.isEmpty() == false) {
                    logger.trace(
                        () -> format(
                            format(
                                "%s blob reference for %s is tracking generational files %s",
                                shardId,
                                primaryTermAndGeneration,
                                trackedGenerationalFiles
                            )
                        )
                    );
                    this.generationalFilesRef = new AtomicReference<>(this.trackedGenerationalFiles);
                    this.incRef();
                } else {
                    this.generationalFilesRef = new AtomicReference<>(null);
                }
                // incRef dependencies only once since we're only tracking dependencies for Lucene local deletions
                references.forEach(AbstractRefCounted::incRef);
            }

            public PrimaryTermAndGeneration getPrimaryTermAndGeneration() {
                return primaryTermAndGeneration;
            }

            public void closedLocalReaders() {
                // be idempotent.
                if (readersClosed.compareAndSet(false, true)) {
                    logger.trace(() -> format("%s closed local reader %s", shardId, primaryTermAndGeneration));
                    decRef();
                }
            }

            void removeAllGenerationalFilesRefs() {
                removeGenerationalFileRefsAndMaybeDecRef(trackedGenerationalFiles);
            }

            void removeGenerationalFileRef(String generationalFileName) {
                assert trackedGenerationalFiles.isEmpty() == false;
                removeGenerationalFileRefsAndMaybeDecRef(Set.of(generationalFileName));
            }

            private void removeGenerationalFileRefsAndMaybeDecRef(Set<String> deletedGenerationalFileNames) {
                var remainingGenerationalFileRefs = generationalFilesRef.accumulateAndGet(
                    deletedGenerationalFileNames,
                    (existing, update) -> {
                        if (existing == null) {
                            // null is the terminal state
                            return null;
                        }
                        return Sets.difference(existing, deletedGenerationalFileNames);
                    }
                );
                if (remainingGenerationalFileRefs != null && remainingGenerationalFileRefs.isEmpty()) {
                    var previousGenerationalFileRefs = generationalFilesRef.getAndUpdate(existing -> null);
                    assert previousGenerationalFileRefs == null || previousGenerationalFileRefs.isEmpty();
                    if (previousGenerationalFileRefs != null) {
                        logger.trace(
                            () -> format("%s all containing generational files deleted for %s", shardId, primaryTermAndGeneration)
                        );
                        decRef();
                    }
                }
            }

            void removeAllLocalCommitsRefs() {
                removeLocalCommitRefsAndMaybeMarkAsLocallyDeleted(includedCommitGenerations);
            }

            void removeLocalCommitRef(PrimaryTermAndGeneration primaryTermAndGeneration) {
                removeLocalCommitRefsAndMaybeMarkAsLocallyDeleted(Set.of(primaryTermAndGeneration));
            }

            private void removeLocalCommitRefsAndMaybeMarkAsLocallyDeleted(Set<PrimaryTermAndGeneration> deletedCommits) {
                var remainingLocalCommits = localCommitsRef.accumulateAndGet(deletedCommits, (existing, update) -> {
                    if (existing == null) {
                        // null is the terminal state
                        return null;
                    }
                    return Sets.difference(existing, deletedCommits);
                });

                if (remainingLocalCommits != null && remainingLocalCommits.isEmpty()) {
                    var previousUsedLocalCommits = localCommitsRef.getAndUpdate(existing -> null);
                    if (previousUsedLocalCommits != null && previousUsedLocalCommits.isEmpty()) {
                        assert references != null : references;
                        logger.trace(
                            () -> format(
                                "%s locally deleted %s, also releases %s",
                                shardId,
                                primaryTermAndGeneration,
                                references.stream().map(BlobReference::getPrimaryTermAndGeneration).sorted().toList()
                            )
                        );
                        references.forEach(AbstractRefCounted::decRef);
                        decRef();
                        // We set references to null, in order to allow any closed referenced BlobReferences to be garbage collected.
                        references = null;
                    }
                }
            }

            /**
             * Decref the blob reference if it is no longer used by any search nodes and the latest notified generation is newer.
             * The blob reference may be removed and released if the decref releases the last refcount.
             */
            private void closedExternalReadersIfNoSearchNodesRemain(
                Set<String> remainingSearchNodes,
                long bccNotificationGeneration,
                Set<PrimaryTermAndGeneration> notificationGenerationBCCDependencies
            ) {
                if (remainingSearchNodes != null && remainingSearchNodes.isEmpty()
                // only mark it closed for readers if it is not the newest commit, since we want a new search shard to be able to use at
                // least that commit (relevant only in case there are no search shards currently).
                    && bccNotificationGeneration > primaryTermAndGeneration.generation()
                    // This prevents closing the external readers reference for a blob when the notificationGeneration
                    // commit uses files from a prior BCC reference, and no search nodes are available
                    // (e.g., when all replicas are down due to a transient issue)
                    && notificationGenerationBCCDependencies.contains(primaryTermAndGeneration) == false) {
                    final Set<String> previousSearchNodes = searchNodesRef.getAndUpdate(existing -> {
                        if (existing == null) {
                            // a concurrent thread already updated it to null. that's ok. assume the other thread handles it
                            return null;
                        } else if (existing.isEmpty()) {
                            // This thread successfully updates it to null and must close the external readers afterwards
                            return null;
                        } else {
                            // Some other thread updates the set to something else, that's ok. do nothing in this case
                            return existing;
                        }
                    });
                    if (previousSearchNodes != null && previousSearchNodes.isEmpty()) {
                        assert searchNodesRef.get() == null;
                        logger.trace(() -> Strings.format("[%s] closing external readers [%s]", shardId, primaryTermAndGeneration));
                        decRef();
                    }
                }
            }

            public boolean isExternalReadersClosed() {
                return searchNodesRef.get() == null;
            }

            /**
             * Add given nodeIds to the search nodes set unless the set is already a null, in which case a null is returned.
             * @param searchNodes The search nodeIds to be added as part of the search nodes tracking.
             * @return The unified set of search nodes after merging the given {@code searchNodes} or {@code null} if the
             * set of search nodes is already null. When it returns {@code null}, it means the reference for external readers
             * is already closed, see also {@link BlobReference#isExternalReadersClosed}.
             */
            @Nullable
            public Set<String> addSearchNodes(Set<String> searchNodes) {
                return updateSearchNodes(searchNodes, Sets::union);
            }

            /**
             * Remove the nodeIds from the search nodes set. It may mark the external readers to be closed if the result set is empty.
             */
            public void removeSearchNodes(
                Set<String> searchNodes,
                long bccNotificationGeneration,
                Set<PrimaryTermAndGeneration> notificationGenerationDependencies
            ) {
                Set<String> remainingSearchNodes = updateSearchNodes(searchNodes, Sets::difference);
                closedExternalReadersIfNoSearchNodesRemain(
                    remainingSearchNodes,
                    bccNotificationGeneration,
                    notificationGenerationDependencies
                );
            }

            /**
             * Retain the nodeIds in the search nodes set. It may mark the external readers to be closed if the result set is empty.
             */
            public void retainSearchNodes(
                Set<String> searchNodes,
                long bccNotificationGeneration,
                Set<PrimaryTermAndGeneration> notificationGenerationBCCDependencies
            ) {
                Set<String> remainingSearchNodes = updateSearchNodes(searchNodes, Sets::intersection);
                closedExternalReadersIfNoSearchNodesRemain(
                    remainingSearchNodes,
                    bccNotificationGeneration,
                    notificationGenerationBCCDependencies
                );
            }

            /**
             * Applies the updateFunc to (existing, searchNodes), updating the tracked list of search nodes using this blob.
             * @return the updated search node set reading from this blob/commit.
             */
            @Nullable
            private Set<String> updateSearchNodes(Set<String> searchNodes, BinaryOperator<Set<String>> updateFunc) {
                return searchNodesRef.accumulateAndGet(searchNodes, (existing, update) -> {
                    if (existing == null) {
                        return null; // null is the final state that can no longer change
                    }
                    return updateFunc.apply(existing, update);
                });
            }

            @Override
            protected void closeInternal() {
                logger.trace(() -> format("%s cleared all references to %s", shardId, primaryTermAndGeneration));
                final BlobReference released = this;
                internalFiles.forEach(fileName -> {
                    blobLocations.compute(fileName, (file, commitAndBlobLocation) -> {
                        var existing = commitAndBlobLocation.blobReference();
                        if (released != existing) {
                            assert isGenerationalFile(file) : file;
                            assert released.primaryTermAndGeneration.generation() > existing.primaryTermAndGeneration.generation()
                                : fileName + ':' + released + " vs " + existing;
                            logger.trace(
                                "not removing [{}] -> [{}] in [{}]",
                                fileName,
                                existing.primaryTermAndGeneration,
                                released.primaryTermAndGeneration
                            );
                            return commitAndBlobLocation;
                        }
                        logger.trace("removing [{}] -> [{}]", fileName, released.primaryTermAndGeneration);
                        return null;
                    });
                });
                includedCommitGenerations.forEach(commitPrimaryTermAndGeneration -> {
                    var commitReferencesInfo = commitReferencesInfos.remove(commitPrimaryTermAndGeneration);
                    assert commitReferencesInfo != null : commitPrimaryTermAndGeneration + " " + commitReferencesInfos;
                });

                commitCleaner.deleteCommit(new StaleCompoundCommit(shardId, primaryTermAndGeneration, allocationPrimaryTerm));
                var removed = primaryTermAndGenToBlobReference.remove(primaryTermAndGeneration);
                assert removed == this;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                BlobReference that = (BlobReference) o;
                return Objects.equals(primaryTermAndGeneration, that.primaryTermAndGeneration);
            }

            @Override
            public int hashCode() {
                return Objects.hash(primaryTermAndGeneration);
            }

            @Override
            public String toString() {
                final Set<String> searchNodes = searchNodesRef.get();
                final boolean externalReaderClosed = searchNodes == null;
                return "Batched compound commit blob "
                    + primaryTermAndGeneration
                    + " ["
                    + localCommitsRef.get()
                    + ","
                    + readersClosed.get()
                    + ","
                    + externalReaderClosed
                    + (externalReaderClosed ? "" : "=" + searchNodes)
                    + ","
                    + refCount()
                    + "]";
            }
        }

        record CommitReferencesInfo(PrimaryTermAndGeneration storedInBCC, Set<PrimaryTermAndGeneration> referencedBCCs) {
            CommitReferencesInfo {
                assert referencedBCCs.contains(storedInBCC) : referencedBCCs + " do not contain " + storedInBCC;
            }

            boolean referencesBCC(PrimaryTermAndGeneration bcc) {
                return referencedBCCs.contains(bcc);
            }
        }
    }

    /**
     * Updates the search node tracking if any search shards were moved.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            if (clusterService.state().getMinTransportVersion().before(COMMIT_NOTIFICATION_TRANSPORT_ACTION_SPLIT)) {
                if (event.routingTableChanged()) {
                    var localRoutingNode = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());

                    if (localRoutingNode == null) {
                        return;
                    }

                    // Check if any of the shards on this node are affected by the routing table change.
                    for (ShardRouting shardRouting : localRoutingNode) {
                        if (shardRouting.primary() == false) {
                            continue;
                        }
                        var shardId = shardRouting.shardId();

                        var shardCommitState = shardsCommitsStates.get(shardId);
                        // shardsCommitsStates not registered yet
                        if (shardCommitState == null) {
                            continue;
                        }

                        if (event.indexRoutingTableChanged(shardId.getIndexName())) {
                            var currentShardRoutingTable = event.state().routingTable().shardRoutingTable(shardId);

                            // If the routing for any of the shard copies changed, update the shard commit tracking.
                            if (event.previousState().routingTable().hasIndex(shardId.getIndex())
                                && event.previousState().routingTable().shardRoutingTable(shardId) != currentShardRoutingTable) {
                                var currentUnpromotableShards = currentShardRoutingTable.unpromotableShards();
                                var currentUnpromotableShardAssignedNodes = currentUnpromotableShards.stream()
                                    .filter(ShardRouting::assignedToNode)
                                    .map(ShardRouting::currentNodeId)
                                    .collect(Collectors.toSet());
                                shardCommitState.updateUnpromotableShardAssignedNodes(currentUnpromotableShardAssignedNodes);
                                return;
                            }
                        }
                    }
                }
            } else {
                if (event.nodesDelta().removed()) {
                    var removedNodeIds = event.nodesDelta().removedNodes().stream().map(node -> node.getId()).collect(Collectors.toSet());
                    for (var shardCommitState : shardsCommitsStates.values()) {
                        shardCommitState.onRemoveNodesFromCluster(removedNodeIds);
                    }
                }
            }
        } finally {
            waitForClusterStateVersion.notifyVersionProcessed(event.state().version());
        }
    }

    /**
     * Register a listener that is notified after receiving the response of new commit notification.
     * New commit notification is sent for each commit creation when {@code STATELESS_UPLOAD_DELAYED} is enabled.
     * In this case, the commit is not uploaded yet. Hence when the listener is called, the commit is _not_ guaranteed
     * to be persisted.
     */
    public void registerCommitNotificationSuccessListener(ShardId shardId, Consumer<Long> listener) {
        var previous = commitNotificationSuccessListeners.put(shardId, listener);
        // For now only the LiveVersionMapArchive uses this
        assert previous == null;
    }

    public void unregisterCommitNotificationSuccessListener(ShardId shardId) {
        var removed = commitNotificationSuccessListeners.remove(shardId);
        assert removed != null;
    }

    public static boolean isGenerationalFile(String file) {
        return file.startsWith("_") && (file.endsWith(".tmp") == false) && IndexFileNames.parseGeneration(file) > 0L;
    }

    /**
     *
     * @param shardId the shard id to register for
     * @param nodeId the nodeId using the commit
     * @param state the cluster state already applied on this node, but possibly not handled in this object yet.
     * @param listener notified when available.
     */
    public void registerCommitForUnpromotableRecovery(
        @Nullable PrimaryTermAndGeneration batchedCompoundGeneration,
        PrimaryTermAndGeneration compoundCommitGeneration,
        ShardId shardId,
        String nodeId,
        ClusterState state,
        ActionListener<RegisterCommitResponse> listener
    ) {
        // todo: assert clusterStateVersion <= clusterService.state().version();
        waitForClusterStateProcessed(state.version(), ActionRunnable.wrap(listener, (l) -> {
            var shardCommitsState = getSafe(shardsCommitsStates, shardId);
            shardCommitsState.registerCommitForUnpromotableRecovery(
                batchedCompoundGeneration,
                compoundCommitGeneration,
                nodeId,
                l.map(registrationResponse -> {
                    if (Assertions.ENABLED) {
                        assert registrationResponse.getCompoundCommit() != null;
                        var cc = registrationResponse.getCompoundCommit().primaryTermAndGeneration();
                        assert cc.onOrAfter(compoundCommitGeneration) : cc + " < " + compoundCommitGeneration;
                        var bcc = registrationResponse.getLatestUploadedBatchedCompoundCommitTermAndGen();
                        assert batchedCompoundGeneration == null || bcc.onOrAfter(batchedCompoundGeneration)
                            : bcc + " < " + batchedCompoundGeneration;
                    }
                    return registrationResponse;
                })
            );
        }));
    }

    private record CommitAndBlobLocation(ShardCommitState.BlobReference blobReference, BlobLocation blobLocation) {
        public CommitAndBlobLocation {
            assert blobReference != null && blobLocation != null : blobReference + ":" + blobLocation;
        }

        @Override
        public String toString() {
            return "CommitAndBlobLocation [blobReference=" + blobReference + ", blobLocation=" + blobLocation + ']';
        }
    }

    public IndexEngineLocalReaderListener getIndexEngineLocalReaderListenerForShard(ShardId shardId) {
        return getSafe(shardsCommitsStates, shardId);
    }

    public CommitBCCResolver getCommitBCCResolverForShard(ShardId shardId) {
        return getSafe(shardsCommitsStates, shardId);
    }

    private void waitForClusterStateProcessed(long clusterStateVersion, Runnable whenDone) {
        waitForClusterStateVersion.waitUntilVersion(clusterStateVersion, () -> threadPool.generic().execute(whenDone));
    }

    private static boolean assertRecoveredCommitFilesHaveBlobLocations(
        Map<String, BlobLocation> recoveredCommitFiles,
        Map<String, CommitAndBlobLocation> blobLocations
    ) {
        for (var commitFile : recoveredCommitFiles.entrySet()) {
            var commitFileName = commitFile.getKey();
            assert blobLocations.containsKey(commitFileName)
                : "Missing blob location for file ["
                    + commitFile
                    + "] referenced at location ["
                    + commitFile.getValue()
                    + "] in recovered commit";
        }
        return true;
    }
}
