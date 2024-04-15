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

import co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.utils.WaitForVersion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.recovery.RecoveryCommitTooNewException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.core.Strings.format;

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

    /** How frequently we check for inactive indexing shards to send new commit notifications. */
    public static final Setting<TimeValue> SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING = Setting.positiveTimeSetting(
        "shard.inactivity.monitor.interval",
        TimeValue.timeValueMinutes(30),
        Setting.Property.NodeScope
    );

    private final ObjectStoreService objectStoreService;
    private final Supplier<String> ephemeralNodeIdSupplier;
    private final Function<ShardId, IndexShardRoutingTable> shardRoutingFinder;
    private final ThreadPool threadPool;
    // We don't do null checks when reading from this sub-map because we hold a commit reference while files are being uploaded. This will
    // prevent commit deletion in the interim.
    private final ConcurrentHashMap<ShardId, ShardCommitState> shardsCommitsStates = new ConcurrentHashMap<>();
    private final ConcurrentMap<ShardId, Consumer<Long>> commitNotificationSuccessListeners = new ConcurrentHashMap<>();
    private final StatelessCommitCleaner commitCleaner;
    private final Client client;

    private final WaitForVersion waitForClusterStateVersion = new WaitForVersion();

    private final TimeValue shardInactivityDuration;
    private final TimeValue shardInactivityMonitorInterval;
    private final BooleanSupplier allNodesResolveBCCReferencesLocally;
    private final ShardInactivityMonitor shardInactivityMonitor;
    private Scheduler.Cancellable scheduledShardInactivityMonitorFuture;

    public StatelessCommitService(
        Settings settings,
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        Client client,
        StatelessCommitCleaner commitCleaner
    ) {
        this(
            settings,
            objectStoreService,
            () -> clusterService.localNode().getEphemeralId(),
            (shardId) -> clusterService.state().routingTable().shardRoutingTable(shardId),
            clusterService.threadPool(),
            client,
            commitCleaner,
            () -> clusterService.state()
                .getMinTransportVersion()
                .onOrAfter(ServerlessTransportVersions.EXPLICIT_BCC_REFERENCES_TRACKING_ADDED)
        );
    }

    public StatelessCommitService(
        Settings settings,
        ObjectStoreService objectStoreService,
        Supplier<String> ephemeralNodeIdSupplier,
        Function<ShardId, IndexShardRoutingTable> shardRouting,
        ThreadPool threadPool,
        Client client,
        StatelessCommitCleaner commitCleaner,
        BooleanSupplier allNodesResolveBCCReferencesLocally
    ) {
        this.objectStoreService = objectStoreService;
        this.ephemeralNodeIdSupplier = ephemeralNodeIdSupplier;
        this.shardRoutingFinder = shardRouting;
        this.threadPool = threadPool;
        this.client = client;
        this.commitCleaner = commitCleaner;
        this.allNodesResolveBCCReferencesLocally = allNodesResolveBCCReferencesLocally;
        this.shardInactivityDuration = SHARD_INACTIVITY_DURATION_TIME_SETTING.get(settings);
        this.shardInactivityMonitorInterval = SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING.get(settings);
        this.shardInactivityMonitor = new ShardInactivityMonitor();
    }

    public void markRecoveredBcc(ShardId shardId, BatchedCompoundCommit recoveredBcc, Set<BlobFile> unreferencedFiles) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        assert recoveredBcc != null;
        assert recoveredBcc.shardId().equals(shardId) : recoveredBcc.shardId() + " vs " + shardId;
        commitState.markBccRecovered(recoveredBcc, unreferencedFiles);
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
    }

    @Override
    protected void doStop() {
        scheduledShardInactivityMonitorFuture.cancel();
    }

    @Override
    protected void doClose() throws IOException {}

    /**
     * An always rescheduled runnable that monitors shards which have been inactive, i.e., have not received indexing, for a long time, and
     * sends new commit notification to search shards.
     */
    private class ShardInactivityMonitor implements Runnable {

        @Override
        public void run() {
            if (lifecycleState() != Lifecycle.State.STARTED) {
                return;
            }
            runInactivityMonitor(threadPool::relativeTimeInMillis);
        }
    }

    //

    /**
     * Resends the latest commit notification, to the search shards, for any index shard that hasn't written anything in a while.
     *
     * Package private for testing.
     */
    void runInactivityMonitor(Supplier<Long> time) {
        shardsCommitsStates.forEach((shardId, commitState) -> {
            if (commitState.state != ShardCommitState.State.CLOSED && commitState.lastNewCommitNotificationSentTimestamp > 0) {
                long elapsed = time.get() - commitState.lastNewCommitNotificationSentTimestamp;
                if (elapsed > shardInactivityDuration.getMillis()) {
                    commitState.maybeResendLatestNewCommitNotification();
                }
            }
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
            if (closedOrRelocated(commitState.state)) {
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

            // TODO: we can also check whether we need upload before appending to avoid creating VBCC just above the cache region size

            final var virtualBcc = commitState.appendCommit(reference);

            if (commitState.shouldUploadVirtualBcc(virtualBcc) == false) {
                assert false : "must always upload till BCC is in full motion";
                return;
            }

            commitState.maybeFreezeAndUploadCurrentVirtualBcc(virtualBcc);
            success = true;
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

    private void createAndRunCommitUpload(
        ShardCommitState commitState,
        VirtualBatchedCompoundCommit virtualBcc,
        ShardCommitState.BlobReference blobReference
    ) {
        logger.debug(
            () -> Strings.format(
                "%s uploading batch compound commit [%s][%s]",
                virtualBcc.getShardId(),
                virtualBcc.getPendingCompoundCommits().stream().map(pc -> pc.getCommitReference().getSegmentsFileName()).toList(),
                virtualBcc.getPrimaryTermAndGeneration().generation()
            )
        );
        // The CommitUpload listener is called after releasing the reference to the Lucene commit,
        // it's possible that due to a slow upload the commit is deleted in the meanwhile, therefore
        // we should acquire a reference to avoid deleting the commit before notifying the unpromotable shards.
        // todo: reevaluate this.
        blobReference.incRef();
        var bccUpload = new BatchedCompoundCommitUpload(
            commitState,
            ActionListener.runAfter(
                ActionListener.wrap(uploadedBcc -> commitState.sendNewCommitNotification(blobReference, uploadedBcc), new Consumer<>() {
                    @Override
                    public void accept(Exception e) {
                        assert assertClosedOrRejectionFailure(e);
                        ShardCommitState.State state = commitState.state;
                        if (closedOrRelocated(state)) {
                            logger.debug(
                                () -> format(
                                    "%s failed to upload BCC [%s] to object store because shard has invalid state %s",
                                    virtualBcc.getShardId(),
                                    virtualBcc.getPrimaryTermAndGeneration().generation(),
                                    state
                                ),
                                e
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
                        final var closed = closedOrRelocated(commitState.state);
                        assert closed
                            || e instanceof EsRejectedExecutionException
                            || e instanceof IndexNotFoundException
                            || e instanceof ShardNotFoundException : closed + " vs " + e;
                        return true;
                    }
                }),
                () -> {
                    IOUtils.closeWhileHandlingException(virtualBcc);
                    blobReference.decRef();
                }
            ),
            virtualBcc,
            TimeValue.timeValueMillis(50)
        );
        bccUpload.run();
    }

    public boolean hasPendingBccUploads(ShardId shardId) {
        try {
            ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
            return commitState.pendingUploadBccGenerations.isEmpty() == false;
        } catch (AlreadyClosedException ace) {
            return false;
        }
    }

    public class BatchedCompoundCommitUpload extends RetryableAction<BatchedCompoundCommit> {

        private final VirtualBatchedCompoundCommit virtualBcc;
        private final ShardCommitState shardCommitState;
        private final ShardId shardId;
        private final long generation;
        private final long startNanos;
        private final AtomicLong uploadedFileCount = new AtomicLong();
        private final AtomicLong uploadedFileBytes = new AtomicLong();
        private int uploadTryNumber = 0;

        public BatchedCompoundCommitUpload(
            ShardCommitState shardCommitState,
            ActionListener<BatchedCompoundCommit> listener,
            VirtualBatchedCompoundCommit virtualBcc,
            TimeValue initialDelay
        ) {
            super(
                logger,
                threadPool,
                initialDelay,
                TimeValue.timeValueSeconds(5),
                TimeValue.timeValueMillis(Long.MAX_VALUE),
                listener,
                threadPool.executor(Stateless.SHARD_WRITE_THREAD_POOL)
            );
            this.shardCommitState = shardCommitState;
            this.virtualBcc = virtualBcc;
            this.shardId = virtualBcc.getShardId();
            this.generation = virtualBcc.getPrimaryTermAndGeneration().generation();
            this.startNanos = threadPool.relativeTimeInNanos();
            assert virtualBcc.isFrozen();
            assert virtualBcc.getPendingCompoundCommits().size() == 1 : "must contain a single CC till BCC is in full motion";
        }

        @Override
        public void tryAction(ActionListener<BatchedCompoundCommit> listener) {
            ++uploadTryNumber;

            // When a shard is in the process of relocating, we mark a max generation to attempt to upload. If this generation is greater
            // then we fail this upload attempt. If the relocation hand-off fails then the state will be set back to RUNNING and the next
            // upload attempt will be allowed through. If the state is transition to RELOCATED or CLOSED then we still don't upload and the
            // upload task will not be retried again. We rely on and accept the max retry delay of 5s, i.e., in case of hand-off abort,
            // this upload could be delayed for up to 5 additional seconds.
            if (shardCommitState.state != ShardCommitState.State.RUNNING && generation > shardCommitState.maxGenerationToUpload) {
                logger.trace(() -> format("%s skipped upload [%s] to object because of active relocation handoff", shardId, generation));
                listener.onFailure(new IllegalStateException("Upload paused because of relocation handoff"));
            } else {
                executeUpload(listener.delegateResponse((l, e) -> {
                    logUploadAttemptFailure(e);
                    l.onFailure(e);
                }));
            }

        }

        private void logUploadAttemptFailure(Exception e) {
            if (e instanceof AlreadyClosedException) {
                logger.trace(
                    () -> format("%s failed attempt to upload commit [%s] to object store because shard closed", shardId, generation),
                    e
                );
            } else {
                org.apache.logging.log4j.util.Supplier<Object> messageSupplier = () -> format(
                    "%s failed attempt [%s] to upload commit [%s] to object store, will retry",
                    shardId,
                    uploadTryNumber,
                    generation
                );
                if (uploadTryNumber == 5) {
                    logger.warn(messageSupplier, e);
                } else {
                    logger.info(messageSupplier, e);
                }
            }
        }

        private void executeUpload(ActionListener<BatchedCompoundCommit> listener) {
            try {
                ActionListener<Void> uploadReadyListener = listener.delegateFailure((l, v) -> uploadBatchedCompoundCommitFile(l));
                checkReadyToUpload(uploadReadyListener, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void checkReadyToUpload(ActionListener<Void> readyListener, ActionListener<BatchedCompoundCommit> notReadyListener) {
            Optional<VirtualBatchedCompoundCommit> missing = shardCommitState.getMaxPendingUploadBccBeforeGeneration(generation);
            if (missing.isPresent()) {
                long missingGeneration = missing.get().getPrimaryTermAndGeneration().generation();
                logger.trace("{} waiting for commit [{}] to finish before uploading commit [{}]", shardId, missingGeneration, generation);
                shardCommitState.addListenerForUploadedGeneration(missingGeneration, notReadyListener.delegateFailure((l, unused) -> {
                    assert shardCommitState.pendingUploadBccGenerations.containsKey(missingGeneration) == false
                        : "missingGeneration [" + missingGeneration + "] still in " + shardCommitState.pendingUploadBccGenerations.keySet();
                    executeUpload(notReadyListener);
                }));
            } else {
                readyListener.onResponse(null);
            }
        }

        private void uploadBatchedCompoundCommitFile(ActionListener<BatchedCompoundCommit> listener) {
            objectStoreService.uploadBatchedCompoundCommitFile(
                virtualBcc.getPrimaryTermAndGeneration().primaryTerm(),
                // TODO: The Directory is used to get the blobContainer which can be obtained by using
                // objectStoreService, shardId and primary term. So there is no need to depend on StatelessCommitRef which gets
                // awkward when there are multiple of them.
                // For now we sill use StatelessCommitRef since VBCC can only have a single CC
                virtualBcc.getPendingCompoundCommits().get(0).getCommitReference().getDirectory(),
                startNanos,
                virtualBcc,
                listener.delegateFailure((l, uploadedBcc) -> {
                    for (Map.Entry<String, BlobLocation> entry : virtualBcc.getInternalLocations().entrySet()) {
                        uploadedFileCount.getAndIncrement();
                        uploadedFileBytes.getAndAdd(entry.getValue().fileLength());
                    }
                    // TODO: remove the following assertion when BCC can contain multiple CCs
                    assert uploadedBcc.compoundCommits().size() == 1;
                    shardCommitState.markBccUploaded(uploadedBcc);
                    final long end = threadPool.relativeTimeInNanos();
                    logger.debug(
                        () -> format(
                            "%s commit [%s] uploaded in [%s] ms (%s files, %s total bytes)",
                            shardId,
                            generation,
                            TimeValue.nsecToMSec(end - startNanos),
                            uploadedFileCount.get(),
                            uploadedFileBytes.get()
                        )
                    );
                    l.onResponse(uploadedBcc);
                })
            );
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return closedOrRelocated(shardCommitState.state) == false;
        }
    }

    public void register(ShardId shardId, long primaryTerm) {
        ShardCommitState existing = shardsCommitsStates.put(shardId, createShardCommitState(shardId, primaryTerm));
        assert existing == null : shardId + " already registered";
    }

    protected ShardCommitState createShardCommitState(ShardId shardId, long primaryTerm) {
        return new ShardCommitState(shardId, primaryTerm);
    }

    public void unregister(ShardId shardId) {
        ShardCommitState removed = shardsCommitsStates.remove(shardId);
        assert removed != null : shardId + " not registered";
        removed.close();
    }

    /**
     * Marks the {@link ShardCommitState} as deleted, which will delete associated blobs upon the forthcoming {@link #unregister(ShardId)}.
     * @param shardId the shard to mark as deleted
     */
    public void delete(ShardId shardId) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        commitState.delete();
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
     * @param filesToRetain the individual files (not blobs) that are still necessary to be able to access, including
     *                      being held by open readers or being part of a commit that is not yet deleted by lucene.
     *                      Always includes all files from the new commit.
     */
    public record UploadedBccInfo(BatchedCompoundCommit uploadedBcc, Set<String> filesToRetain) {}

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

    // Visible for testing
    @Nullable
    BlobLocation getBlobLocation(ShardId shardId, String fileName) {
        ShardCommitState commitState = getSafe(shardsCommitsStates, shardId);
        var commitAndBlobLocation = commitState.blobLocations.get(fileName);
        if (commitAndBlobLocation != null) {
            return commitAndBlobLocation.blobLocation;
        }
        return null;
    }

    private static ShardCommitState getSafe(ConcurrentHashMap<ShardId, ShardCommitState> map, ShardId shardId) {
        final ShardCommitState commitState = map.get(shardId);
        if (commitState == null) {
            throw new AlreadyClosedException("shard [" + shardId + "] has already been closed");
        }
        return commitState;
    }

    private static boolean closedOrRelocated(ShardCommitState.State state) {
        return state == ShardCommitState.State.CLOSED || state == ShardCommitState.State.RELOCATED;
    }

    // Visible for testing
    class ShardCommitState implements IndexEngineLocalReaderListener, CommitBCCResolver {
        private static final long EMPTY_GENERATION_NOTIFIED_SENTINEL = -1;

        private enum State {
            RUNNING,
            RELOCATING,
            RELOCATED,
            CLOSED
        }

        private final ShardId shardId;
        private final long allocationPrimaryTerm;

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
         * The highest generation number that we have received from a commit notification response from search shards.
         */
        private final AtomicLong generationNotified = new AtomicLong(EMPTY_GENERATION_NOTIFIED_SENTINEL);
        private volatile long maxGenerationToUpload = Long.MAX_VALUE;
        private final AtomicLong maxGenerationToUploadForFlush = new AtomicLong(-1);
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
        ShardCommitState(ShardId shardId, long allocationPrimaryTerm) {
            this.shardId = shardId;
            this.allocationPrimaryTerm = allocationPrimaryTerm;
        }

        private void markBccRecovered(BatchedCompoundCommit recoveredBcc, Set<BlobFile> nonRecoveredBlobs) {
            assert recoveredBcc != null;
            assert nonRecoveredBlobs != null;
            assert primaryTermAndGenToBlobReference.isEmpty() : primaryTermAndGenToBlobReference;
            assert blobLocations.isEmpty() : blobLocations;

            final var recoveredCommit = recoveredBcc.last();
            Map<PrimaryTermAndGeneration, Map<String, BlobLocation>> referencedBlobs = new HashMap<>();
            // TODO: ES-8246 Fix #getInternalFiles to account for multiple CCs belonging to a BCC
            final var recoveredInternalFiles = recoveredCommit.getInternalFiles();
            for (Map.Entry<String, BlobLocation> referencedBlob : recoveredCommit.commitFiles().entrySet()) {
                if (recoveredInternalFiles.contains(referencedBlob.getKey()) == false) {
                    referencedBlobs.computeIfAbsent(
                        new PrimaryTermAndGeneration(
                            referencedBlob.getValue().primaryTerm(),
                            referencedBlob.getValue().compoundFileGeneration()
                        ),
                        primaryTermAndGeneration -> new HashMap<>()
                    ).put(referencedBlob.getKey(), referencedBlob.getValue());
                }
            }

            List<BlobReference> previousBCCBlobs = new ArrayList<>(nonRecoveredBlobs.size());
            // create a compound commit blob instance for the recovery commit
            for (BlobFile nonRecoveredBlobFile : nonRecoveredBlobs) {
                if (StatelessCompoundCommit.startsWithBlobPrefix(nonRecoveredBlobFile.blobName())) {
                    PrimaryTermAndGeneration nonRecoveredTermGen = new PrimaryTermAndGeneration(
                        nonRecoveredBlobFile.primaryTerm(),
                        StatelessCompoundCommit.parseGenerationFromBlobName(nonRecoveredBlobFile.blobName())
                    );

                    Map<String, BlobLocation> internalFiles = referencedBlobs.getOrDefault(nonRecoveredTermGen, Collections.emptyMap());
                    // We don't know which CCs are included in the non-recovered BCCs,
                    // hence the empty set for the included commits in the blob reference
                    BlobReference nonRecoveredBlobReference = new BlobReference(
                        nonRecoveredTermGen,
                        internalFiles.keySet(),
                        Set.of(),
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
                .filter(commit -> referencedBlobs.containsKey(commit.getPrimaryTermAndGeneration()))
                .collect(Collectors.toUnmodifiableSet());
            var recoveryBCCBlob = new BlobReference(
                recoveredCommit.primaryTermAndGeneration(),
                recoveredCommit.getInternalFiles(),
                referencedBCCsForRecoveryCommit,
                // During recovery we only read the latest stored commit,
                // hence we consider that the recovered blob reference contains only that commit
                Set.of(recoveredCommit.primaryTermAndGeneration())
            );

            assert primaryTermAndGenToBlobReference.containsKey(recoveredCommit.primaryTermAndGeneration()) == false;

            primaryTermAndGenToBlobReference.put(recoveryBCCBlob.getPrimaryTermAndGeneration(), recoveryBCCBlob);

            recoveredCommit.getInternalFiles().forEach(fileName -> {
                var fileBlobLocation = recoveredCommit.commitFiles().get(fileName);
                assert fileBlobLocation != null : fileName;
                var existing = blobLocations.put(fileName, new CommitAndBlobLocation(recoveryBCCBlob, fileBlobLocation));
                assert existing == null : fileName + ':' + existing;
            });

            var currentUnpromotableShardAssignedNodes = shardRoutingFinder.apply(shardId)
                .unpromotableShards()
                .stream()
                .map(ShardRouting::currentNodeId)
                .collect(Collectors.toSet());

            primaryTermAndGenToBlobReference.values()
                .forEach(commit -> trackOutstandingUnpromotableShardCommitRef(currentUnpromotableShardAssignedNodes, commit));

            var referencedBCCGenerationsByRecoveredCommit = BatchedCompoundCommit.computeReferencedBCCGenerations(recoveredCommit);
            commitReferencesInfos.put(
                recoveredCommit.primaryTermAndGeneration(),
                new CommitReferencesInfo(recoveredCommit.primaryTermAndGeneration(), referencedBCCGenerationsByRecoveredCommit)
            );

            // Decrement all of the non-recovered BCCs that are not referenced by the recovered commit
            for (BlobReference blobReference : primaryTermAndGenToBlobReference.values()) {
                if (blobReference.getPrimaryTermAndGeneration().equals(recoveryBCCBlob.getPrimaryTermAndGeneration()) == false) {
                    blobReference.deleted();
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
            if (closedOrRelocated(state)) {
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
            if (getMaxUploadedGenerationRange().ccGen >= 0) {
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
            synchronized (this) {
                if (currentVirtualBcc == null) {
                    final var newVirtualBcc = new VirtualBatchedCompoundCommit(
                        shardId,
                        ephemeralNodeIdSupplier.get(),
                        reference.getPrimaryTerm(),
                        reference.getGeneration(),
                        fileName -> getBlobLocation(shardId, fileName)
                    );
                    final boolean appended = newVirtualBcc.appendCommit(reference);
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
                    final boolean appended = currentVirtualBcc.appendCommit(reference);
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

                var previousCommitReferencesInfo = commitReferencesInfos.put(
                    commitPrimaryTermAndGeneration,
                    new CommitReferencesInfo(
                        currentVirtualBcc.getPrimaryTermAndGeneration(),
                        BatchedCompoundCommit.computeReferencedBCCGenerations(statelessCompoundCommit)
                    )
                );

                assert previousCommitReferencesInfo == null
                    : statelessCompoundCommit.primaryTermAndGeneration() + " was already present in " + commitReferencesInfos;

                return currentVirtualBcc;
            }
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
                if (closedOrRelocated(state)) {
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
                // Create the blobReference which updates blobLocations, init search nodes commit usage tracking
                blobReference = createBlobReference(expectedVirtualBcc);
            }
            createAndRunCommitUpload(this, expectedVirtualBcc, blobReference);
        }

        // TODO: expand for more criteria such as size, time interval
        protected boolean shouldUploadVirtualBcc(VirtualBatchedCompoundCommit virtualBcc) {
            assert virtualBcc.getPendingCompoundCommits().size() == 1 : "must contain a single CC till BCC is in full motion";
            return virtualBcc.getPendingCompoundCommits().isEmpty() == false;
        }

        private BlobReference createBlobReference(VirtualBatchedCompoundCommit virtualBcc) {
            assert isDeleted == false : "shard " + shardId + " is deleted when trying to add commit data";
            assert virtualBcc.getPendingCompoundCommits().size() == 1 : "must contain a single CC till BCC is in full motion";
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

            // if there are external files the new instance must reference the corresponding commit blob instances
            var referencedBCCs = commitFiles.stream().filter(fileName -> additionalFiles.contains(fileName) == false).map(fileName -> {
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
                virtualBcc.getPendingCompoundCommitGenerations()
            );

            if (primaryTermAndGenToBlobReference.putIfAbsent(blobReference.getPrimaryTermAndGeneration(), blobReference) != null) {
                throw new IllegalArgumentException(blobReference + " already exists");
            }

            // add pending blob locations for new files
            // Use getInternalLocations() to include copied generational files. We want to update their blobLocations.
            virtualBcc.getInternalLocations().keySet().forEach(fileName -> {
                final BlobLocation blobLocation = virtualBcc.getBlobLocation(fileName);
                blobLocations.compute(fileName, (ignored, existing) -> {
                    if (existing == null) {
                        return new CommitAndBlobLocation(blobReference, blobLocation);
                    } else {
                        // For copied generational files, we update its blobLocation but keep the original blobReference unchanged
                        // TODO: This behaviour may be changed in future. See also https://elasticco.atlassian.net/browse/ES-7654
                        assert isGenerationalFile(fileName)
                            && existing.blobLocation.compoundFileGeneration() < blobLocation.compoundFileGeneration()
                            : fileName + ':' + existing + ':' + blobLocation;
                        return new CommitAndBlobLocation(existing.blobReference, blobLocation);
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
            if (closedOrRelocated(state)) {
                return Set.of();
            }
            PrimaryTermAndGeneration primaryTermAndGeneration = resolvePrimaryTermForGeneration(generation);
            var commitReferencesInfo = commitReferencesInfos.get(primaryTermAndGeneration);
            assert commitReferencesInfo != null : commitReferencesInfos + " " + generation;
            // TODO: this assertion won't hold once we have multiple CCs per BCC since its primary term and generation might be >= than the
            // BCC primary term and generation
            assert commitReferencesInfo.referencesBCC(primaryTermAndGeneration);

            return commitReferencesInfo.referencedBCCs();
        }

        public void markCommitDeleted(long generation) {
            var commitPrimaryTermAndGeneration = resolvePrimaryTermForGeneration(generation);
            var commitReferencesInfo = commitReferencesInfos.get(commitPrimaryTermAndGeneration);
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
            blobReference.deleted();
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
            assert uploadedBcc.size() == 1 : "BCC must have a single CC until it is fully enabled";
            final long newBccGeneration = uploadedBcc.primaryTermAndGeneration().generation(); // for managing pending uploads
            final long newGeneration = uploadedBcc.last().generation(); // for notifying generation listeners

            // We use two synchronized blocks to ensure:
            // 1. Listeners are completed outside the synchronized blocks
            // 2. BCC upload consumers are triggered in generation order
            // 3. latestUploadedBcc, pendingUploadBccGenerations and generationListeners
            // are updated in a single synchronized block to avoid racing

            final List<ActionListener<UploadedBccInfo>> listenersToFire = new ArrayList<>();
            synchronized (this) {
                if (newBccGeneration <= getMaxUploadedGenerationRange().bccGen) {
                    if (isUpload) {
                        // Remove the BCC from the pending list regardless just in case
                        pendingUploadBccGenerations.remove(newBccGeneration);
                    }
                    assert false
                        : "out of order BCC generation ["
                            + newBccGeneration
                            + "] <= ["
                            + getMaxUploadedGenerationRange().bccGen
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

            final var uploadedBccInfo = new UploadedBccInfo(uploadedBcc, Set.copyOf(blobLocations.keySet()));
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
                assert newBccGeneration > getMaxUploadedGenerationRange().bccGen
                    : "out of order BCC generation ["
                        + newBccGeneration
                        + "] <= ["
                        + getMaxUploadedGenerationRange().bccGen
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
         * Gets the max uploaded generation range for a BCC, by accessing the latest uploaded {@link BatchedCompoundCommit}
         * without synchronization, or (-1, -1) otherwise.
         */
        public GenerationRange getMaxUploadedGenerationRange() {
            final var latestUploadedCommitCopy = latestUploadedBcc; // read the volatile once
            return latestUploadedCommitCopy == null
                ? new GenerationRange(-1L, -1L)
                : new GenerationRange(
                    latestUploadedCommitCopy.primaryTermAndGeneration().generation(),
                    latestUploadedCommitCopy.last().generation()
                );
        }

        /**
         * Ensures that the provided generation is uploaded or pending and returns null. Otherwise, returns an error message.
         */
        private @Nullable String isUploadedOrPending(long generation) {
            final var maxPendingUploadBcc = getMaxPendingUploadBcc();
            final var maxUploadedCcGen = getMaxUploadedGenerationRange().ccGen;
            if (generation > maxUploadedCcGen) {
                final String message = format(
                    "generation [%s] is not covered by either maxUploadedGeneration [%s] or maxPendingUploadBcc [%s]",
                    generation,
                    maxUploadedCcGen,
                    maxPendingUploadBcc
                );
                assert maxPendingUploadBcc.isPresent() : message;
                final var maxPendingUploadBccGen = maxPendingUploadBcc.isPresent()
                    ? maxPendingUploadBcc.get().getPrimaryTermAndGeneration().generation()
                    : -1L;
                if (generation > maxPendingUploadBccGen) {
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
         * Broadcasts notification of a new uploaded BCC to all the search nodes hosting a replica shard for the given shard commit.
         */
        private void sendNewCommitNotification(BlobReference blobReference, BatchedCompoundCommit uploadedBcc) {
            assert uploadedBcc != null;

            var shardRoutingTable = shardRoutingFinder.apply(uploadedBcc.shardId());
            Set<String> nodesWithAssignedSearchShards = shardRoutingTable.unpromotableShards()
                .stream()
                .filter(ShardRouting::assignedToNode)
                .map(ShardRouting::currentNodeId)
                .collect(Collectors.toSet());
            trackOutstandingUnpromotableShardCommitRef(nodesWithAssignedSearchShards, blobReference);
            lastNewCommitNotificationSentTimestamp = threadPool.relativeTimeInMillis();

            NewCommitNotificationRequest request = new NewCommitNotificationRequest(
                shardRoutingTable,
                uploadedBcc.last(),
                uploadedBcc.primaryTermAndGeneration().generation(),
                uploadedBcc.primaryTermAndGeneration()
            );
            var notificationCommitBCCDependencies = resolveReferencedBCCsForCommit(uploadedBcc.primaryTermAndGeneration().generation());
            client.execute(TransportNewCommitNotificationAction.TYPE, request, ActionListener.wrap(response -> {
                onNewCommitNotificationResponse(
                    uploadedBcc.primaryTermAndGeneration().generation(),
                    notificationCommitBCCDependencies,
                    nodesWithAssignedSearchShards,
                    response.getPrimaryTermAndGenerationsInUse()
                );
                var consumer = commitNotificationSuccessListeners.get(shardId);
                if (consumer != null) {
                    consumer.accept(uploadedBcc.primaryTermAndGeneration().generation());
                }
            }, e -> {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof ConnectTransportException) {
                    logger.debug(
                        () -> format(
                            "%s failed to notify search shards after upload of batched compound commit [%s] due to connection issues",
                            shardId,
                            uploadedBcc.primaryTermAndGeneration().generation()
                        ),
                        e
                    );
                } else {
                    logger.warn(
                        () -> format(
                            "%s failed to notify search shards after upload of batched compound commit [%s]",
                            shardId,
                            uploadedBcc.primaryTermAndGeneration().generation()
                        ),
                        e
                    );
                }
            }));
        }

        /**
         * Resends the latest commit notification if there are any search nodes still using older blob references.
         * This will collect responses from the relevant search nodes indicating whether they are done with the older commits, allowing
         * those commits to be eventually deleted when no longer in use by any shard.
         */
        private void maybeResendLatestNewCommitNotification() {
            final BatchedCompoundCommit latestBatchedCompoundCommitUploaded;
            final BlobReference latestBlobReference;

            // Get latest uploaded stateless compound commit and the respective compound commit blob
            synchronized (this) {
                if (latestUploadedBcc == null) {
                    return;
                }
                latestBatchedCompoundCommitUploaded = latestUploadedBcc;
                PrimaryTermAndGeneration termGen = latestBatchedCompoundCommitUploaded.primaryTermAndGeneration();
                latestBlobReference = primaryTermAndGenToBlobReference.get(termGen);
                assert latestBlobReference != null : "could not find latest " + termGen + " in compound commit blobs";
            }

            final boolean anyOldBlobReferencesStillInUse = primaryTermAndGenToBlobReference.values()
                .stream()
                .anyMatch(blobReference -> blobReference != latestBlobReference && blobReference.isExternalReadersClosed() == false);

            // Resend the notification if older blob references are still in use
            if (anyOldBlobReferencesStillInUse) {
                logger.debug("sending new commit notifications for inactive shard [{}]", shardId);
                sendNewCommitNotification(latestBlobReference, latestBatchedCompoundCommitUploaded);
            } else {
                lastNewCommitNotificationSentTimestamp = threadPool.relativeTimeInMillis();
            }
        }

        /**
         * Register a listener that is invoked once a commit with the given generation has been uploaded to the object store. The listener
         * is invoked only once.
         *
         * @param generation the commit generation
         * @param listener the listener
         */
        private void addListenerForUploadedGeneration(long generation, ActionListener<Void> listener) {
            boolean completeListenerSuccess = false;
            boolean completeListenerClosed = false;
            synchronized (this) {
                if (closedOrRelocated(state)) {
                    completeListenerClosed = true;
                } else if (getMaxUploadedGenerationRange().ccGen >= generation) {
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
                if (state == State.RELOCATED) {
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
                if (closedOrRelocated(state) == false) {
                    if (uploadedBccConsumers == null) {
                        uploadedBccConsumers = new ArrayList<>();
                    }
                    uploadedBccConsumers.add(consumer);
                }
            }
        }

        private List<ActionListener<Void>> changeStateAndGetListeners(State newState) {
            List<Tuple<Long, ActionListener<Void>>> listenersToFail;
            synchronized (this) {
                assert newState == State.CLOSED || newState == State.RELOCATED : newState;
                state = newState;
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
            final var virtualBcc = currentVirtualBcc;
            if (virtualBcc != null) {
                assert state != State.RELOCATED : "relocation should have gracefully reset current VBCC";
                // Release commit references held by the current VBCC
                // TODO: maybe upload before releasing in some cases as a future optimization?
                IOUtils.closeWhileHandlingException(virtualBcc);
            }

            List<ActionListener<Void>> listenersToFail = changeStateAndGetListeners(State.CLOSED);
            ActionListener.onFailure(listenersToFail, new AlreadyClosedException("shard [" + shardId + "] has already been closed"));

            if (isDeleted) {
                // clear all unpromotable references
                updateUnpromotableShardAssignedNodes(Set.of(), Long.MAX_VALUE, Set.of());
                primaryTermAndGenToBlobReference.values().forEach(blobReference -> {
                    blobReference.closedLocalReaders();
                    blobReference.deleted();
                });
            }
        }

        private void markRelocating(long minRelocatedGeneration, ActionListener<Void> listener) {
            long toWaitFor;
            synchronized (this) {
                Optional<VirtualBatchedCompoundCommit> maxPending = getMaxPendingUploadBcc();

                // We wait for the max generation we see at the moment to be uploaded. Generations are always uploaded in order so this
                // logic works. Additionally, at minimum we wait for minRelocatedGeneration to be uploaded. It is possible it has already
                // been uploaded which would make the listener be triggered immediately.
                toWaitFor = minRelocatedGeneration;
                if (maxPending.isPresent() && maxPending.get().getMaxGeneration() > minRelocatedGeneration) {
                    toWaitFor = maxPending.get().getMaxGeneration();
                }

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
                    assert state == State.RELOCATING;
                    listenersToFail = changeStateAndGetListeners(State.RELOCATED);
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
         * @param notificationGeneration
         * @param searchNodes combined set of all the search nodes with shard replicas.
         * @param primaryTermAndGenerationsInUse combined set of all the PrimaryTermAndGeneration commits in use by all search shards.
         */
        void onNewCommitNotificationResponse(
            long notificationGeneration,
            Set<PrimaryTermAndGeneration> notificationCommitBCCDependencies,
            Set<String> searchNodes,
            Set<PrimaryTermAndGeneration> primaryTermAndGenerationsInUse
        ) {
            if (state == State.CLOSED) {
                return;
            }

            if (generationNotified.getAndAccumulate(notificationGeneration, Math::max) > notificationGeneration) {
                // no need to process backwards commit notifications
                // (but would be safe - and we rely on it, this check is just an optimistic check)
                return;
            }
            if (allNodesResolveBCCReferencesLocally() == false) {
                // Ignore notification responses until all nodes report the full set of BCCs that are in use
                return;
            }

            for (BlobReference blobReference : primaryTermAndGenToBlobReference.values()) {
                // we are allowed to shrink the set of search nodes for any generation <= notificationGeneration, since after the
                // notification generation has been refreshed on the search shard, we know that the shard will never add more use of any
                // earlier generations.
                if (blobReference.getPrimaryTermAndGeneration().generation() <= notificationGeneration
                    && primaryTermAndGenerationsInUse.contains(blobReference.getPrimaryTermAndGeneration()) == false) {
                    // remove nodes from the search nodes set. Any search shard registered during initialization will be left until it
                    // starts responding.
                    blobReference.removeSearchNodes(searchNodes, notificationGeneration, notificationCommitBCCDependencies);
                }
            }
        }

        void updateUnpromotableShardAssignedNodes(Set<String> currentUnpromotableNodes) {
            long generationNotified = this.generationNotified.get();
            Set<PrimaryTermAndGeneration> generationNotifiedBCCDependencies = generationNotified == EMPTY_GENERATION_NOTIFIED_SENTINEL
                ? Set.of()
                : resolveReferencedBCCsForCommit(generationNotified);
            updateUnpromotableShardAssignedNodes(currentUnpromotableNodes, generationNotified, generationNotifiedBCCDependencies);
        }

        void updateUnpromotableShardAssignedNodes(
            Set<String> currentUnpromotableNodes,
            long generationNotified,
            Set<PrimaryTermAndGeneration> generationNotifiedBCCDependencies
        ) {
            for (BlobReference blobReference : primaryTermAndGenToBlobReference.values()) {
                blobReference.retainSearchNodes(currentUnpromotableNodes, generationNotified, generationNotifiedBCCDependencies);
            }
        }

        /**
         * Registers the given set of 'nodes' in the {@link BlobReference} for the specified 'commitReference'.
         */
        void trackOutstandingUnpromotableShardCommitRef(Set<String> nodes, BlobReference commitReference) {
            boolean success = registerUnpromoteableCommitRefs(nodes, commitReference);
            // it is fine if a newer commit notification removed the registration, since then blobReference cannot be used
            // by search shard readers anymore.
            assert success || commitReference.getPrimaryTermAndGeneration().generation() < generationNotified.get();
        }

        /**
         * Register commit used by unpromotable, returning the commit to use by the unpromotable.
         */
        BlobReference registerCommitForUnpromotableRecovery(String nodeId, PrimaryTermAndGeneration commit) {
            // Find a commit (starting with the requested one) that could be used for unpromotable recovery
            // TODO: ES-8200 We should account for the fact that the node will register using a commit,
            // but the blob references point to BCCs
            var blobReference = primaryTermAndGenToBlobReference.get(commit);
            if (blobReference == null) {
                long lastUploadedBccGeneration = getMaxUploadedGenerationRange().bccGen;
                if (lastUploadedBccGeneration != -1) {
                    blobReference = primaryTermAndGenToBlobReference.get(resolvePrimaryTermForGeneration(lastUploadedBccGeneration));
                }
            }
            // If the indexing shard is not finished initializing from the object store, we are not
            // able to register the commit for recovery. For now, fail the registration request.
            // TODO: we should be able to handle this case by either retrying the registration or keep the
            // registration and run it after the indexing shard is finished initializing.
            if (blobReference == null) {
                throw new NoShardAvailableActionException(shardId, "indexing shard is initializing");
            }
            // TODO: We should compare with blobReference.maxGeneration once BCC is fully enabled
            if (blobReference.primaryTermAndGeneration.compareTo(commit) < 0) {
                var message = Strings.format(
                    "requested commit to register (%s) is newer than the newest known local commit (%s)",
                    commit,
                    blobReference
                );
                throw new RecoveryCommitTooNewException(shardId, message);
            }
            // Register the commit that is going to be used for unpromotable recovery
            long previousGenerationUploaded = -1;
            while (true) {
                if (blobReference != null && registerUnpromotableBCCRefsForCommit(blobReference.getPrimaryTermAndGeneration(), nodeId)) {
                    assert blobReference.isExternalReadersClosed() == false;
                    return blobReference;
                } else {
                    // TODO: the following generation management needs to be refined with ES-8200
                    long generation = getMaxUploadedGenerationRange().bccGen;
                    assert generation > previousGenerationUploaded;
                    previousGenerationUploaded = generation;
                    blobReference = primaryTermAndGenToBlobReference.get(new PrimaryTermAndGeneration(allocationPrimaryTerm, generation));
                    assert blobReference != null || getMaxUploadedGenerationRange().bccGen > generation;
                }
            }
        }

        private boolean registerUnpromotableBCCRefsForCommit(PrimaryTermAndGeneration commitPrimaryTermAndGeneration, String nodeId) {
            // TODO: maybe we should let the search node to resolve the dependencies for the commit?
            var commitInfo = commitReferencesInfos.get(commitPrimaryTermAndGeneration);
            if (commitInfo == null) {
                return false;
            }

            var referencedBCCsForCommit = commitInfo.referencedBCCs();
            for (PrimaryTermAndGeneration bccPrimaryTermAndGeneration : referencedBCCsForCommit) {
                var blobReference = primaryTermAndGenToBlobReference.get(bccPrimaryTermAndGeneration);
                // TODO: ES-8200 once we move to multiple CCs per BCC we'll need to handle the case where
                // TODO: blobReference is null for the non-uploaded BCCs
                if (blobReference == null || registerUnpromoteableCommitRefs(Set.of(nodeId), blobReference) == false) {
                    return false;
                }
            }
            return true;
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

        /**
         * A ref counted instance representing a (BCC) blob reference to the object store.
         */
        private class BlobReference extends AbstractRefCounted {
            private final PrimaryTermAndGeneration primaryTermAndGeneration;
            private final Set<PrimaryTermAndGeneration> includedCommitGenerations;
            // TODO: The internalFiles should include copied generational files once ES-7654 is resolved
            private final Set<String> internalFiles;
            private final Set<BlobReference> references;
            private final AtomicBoolean readersClosed = new AtomicBoolean();
            private final AtomicBoolean deleted = new AtomicBoolean();
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
            private final AtomicReference<Set<String>> searchNodesRef = new AtomicReference<>(Set.of());

            BlobReference(
                PrimaryTermAndGeneration primaryTermAndGeneration,
                Set<String> internalFiles,
                Set<BlobReference> references,
                Set<PrimaryTermAndGeneration> includedCommitGenerations
            ) {
                this.primaryTermAndGeneration = primaryTermAndGeneration;
                this.internalFiles = Set.copyOf(internalFiles);
                this.references = references;
                this.includedCommitGenerations = Set.copyOf(includedCommitGenerations);
                // we both decRef closedLocalReaders and closedExternalReaders, hence the extra incRef (in addition to the
                // 1 ref given by AbstractRefCounted constructor)
                this.incRef();
                this.incRef();
                // incRef dependencies only once since we're only tracking dependencies for Lucene local deletions
                references.forEach(AbstractRefCounted::incRef);
            }

            public PrimaryTermAndGeneration getPrimaryTermAndGeneration() {
                return primaryTermAndGeneration;
            }

            public void closedLocalReaders() {
                // be idempotent.
                if (readersClosed.compareAndSet(false, true)) {
                    decRef();
                }
            }

            public void deleted() {
                // TODO: ES-8234 account for multiples CCs pointing to the same BlobReference
                assert includedCommitGenerations.size() == 1 || includedCommitGenerations.isEmpty();
                // be idempotent.
                if (deleted.compareAndSet(false, true)) {
                    references.forEach(AbstractRefCounted::decRef);
                    decRef();
                }
            }

            /**
             * Decref the blob reference if it is no longer used by any search nodes and the latest notified generation is newer.
             * The blob reference may be removed and released if the decref releases the last refcount.
             */
            private void closedExternalReadersIfNoSearchNodesRemain(
                long notificationGeneration,
                Set<PrimaryTermAndGeneration> notificationGenerationBCCDependencies,
                Set<String> remainingSearchNodes
            ) {
                if (remainingSearchNodes != null && remainingSearchNodes.isEmpty()
                // only mark it closed for readers if it is not the newest commit, since we want a new search shard to be able to use at
                // least that commit (relevant only in case there are no search shards currently).
                    && notificationGeneration > primaryTermAndGeneration.generation()
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
                        logger.trace(() -> Strings.format("[%s] closing external readers", shardId));
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
                long notificationGeneration,
                Set<PrimaryTermAndGeneration> notificationGenerationDependencies
            ) {
                Set<String> remainingSearchNodes = updateSearchNodes(searchNodes, Sets::difference);
                closedExternalReadersIfNoSearchNodesRemain(
                    notificationGeneration,
                    notificationGenerationDependencies,
                    remainingSearchNodes
                );
            }

            /**
             * Retain the nodeIds in the search nodes set. It may mark the external readers to be closed if the result set is empty.
             */
            public void retainSearchNodes(
                Set<String> searchNodes,
                long notificationGeneration,
                Set<PrimaryTermAndGeneration> notificationGenerationBCCDependencies
            ) {
                Set<String> remainingSearchNodes = updateSearchNodes(searchNodes, Sets::intersection);
                closedExternalReadersIfNoSearchNodesRemain(
                    notificationGeneration,
                    notificationGenerationBCCDependencies,
                    remainingSearchNodes
                );
            }

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
                final BlobReference released = this;
                internalFiles.forEach(fileName -> {
                    blobLocations.compute(fileName, (file, commitAndBlobLocation) -> {
                        var existing = commitAndBlobLocation.blobReference();
                        if (released != existing) {
                            assert isGenerationalFile(file) : file;
                            assert released.primaryTermAndGeneration.generation() < existing.primaryTermAndGeneration.generation()
                                : fileName + ':' + released + " vs " + existing;
                            return commitAndBlobLocation;
                        }
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
                    + deleted.get()
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

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO: maybe give a grace period if the node left?
        try {
            if (event.routingTableChanged()) {
                var localShardRouting = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());

                if (localShardRouting == null) {
                    return;
                }

                for (ShardRouting shardRouting : localShardRouting) {
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

                        if (event.previousState().routingTable().hasIndex(shardId.getIndex()) == false
                            || currentShardRoutingTable != event.previousState().routingTable().shardRoutingTable(shardId)) {
                            var currentUnpromotableShards = currentShardRoutingTable.unpromotableShards();
                            var currentUnpromotableShardAssignedNodes = currentUnpromotableShards.stream()
                                .map(ShardRouting::currentNodeId)
                                .collect(Collectors.toSet());
                            shardCommitState.updateUnpromotableShardAssignedNodes(currentUnpromotableShardAssignedNodes);
                        }
                    }
                }
            }
        } finally {
            waitForClusterStateVersion.notifyVersionProcessed(event.state().version());
        }
    }

    public void registerNewCommitSuccessListener(ShardId shardId, Consumer<Long> listener) {
        var previous = commitNotificationSuccessListeners.put(shardId, listener);
        // For now only the LiveVersionMapArchive uses this
        assert previous == null;
    }

    public void unregisterNewCommitSuccessListener(ShardId shardId) {
        var removed = commitNotificationSuccessListeners.remove(shardId);
        assert removed != null;
    }

    public static boolean isGenerationalFile(String file) {
        return file.startsWith("_") && IndexFileNames.parseGeneration(file) > 0L;
    }

    /**
     *
     * @param commit the commit to register
     * @param shardId the shard id to register for
     * @param nodeId the nodeId using the commit
     * @param state the cluster state already applied on this node, but possibly not handled in this object yet.
     * @param listener notified when available.
     */
    public void registerCommitForUnpromotableRecovery(
        PrimaryTermAndGeneration commit,
        ShardId shardId,
        String nodeId,
        ClusterState state,
        ActionListener<PrimaryTermAndGeneration> listener
    ) {
        // todo: assert clusterStateVersion <= clusterService.state().version();
        waitForClusterStateProcessed(state.version(), () -> {
            ActionListener.completeWith(listener, () -> {
                var shardCommitsState = getSafe(shardsCommitsStates, shardId);
                var blobReference = shardCommitsState.registerCommitForUnpromotableRecovery(nodeId, commit);
                var proposed = blobReference.primaryTermAndGeneration;
                assert proposed.compareTo(commit) >= 0
                    : Strings.format(
                        "Proposed commit (%s) for unpromotable recovery must be newer that the requested one (%s)",
                        proposed,
                        commit
                    );
                return proposed;
            });
        });
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

    /**
     * The start and end generations for BCC
     * @param bccGen The start generation, also the BCC's generation
     * @param ccGen The end generation, which is the generation of the last CC in the BCC
     */
    private record GenerationRange(long bccGen, long ccGen) {
        GenerationRange {
            assert bccGen <= ccGen : bccGen + " > " + ccGen;
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

    private boolean allNodesResolveBCCReferencesLocally() {
        return allNodesResolveBCCReferencesLocally.getAsBoolean();
    }
}
