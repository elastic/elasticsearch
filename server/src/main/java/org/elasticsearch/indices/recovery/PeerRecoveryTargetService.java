/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.indices.recovery.RecoveriesCollection.RecoveryRef;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * The recovery target handles recoveries of peer shards of the shard+node to recover to.
 * <p>
 * Note, it can be safely assumed that there will only be a single recovery per shard (index+id) and
 * not several of them (since we don't allocate several shard replicas to the same node).
 */
public class PeerRecoveryTargetService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(PeerRecoveryTargetService.class);

    public static class Actions {
        public static final String FILES_INFO = "internal:index/shard/recovery/filesInfo";
        public static final String RESTORE_FILE_FROM_SNAPSHOT = "internal:index/shard/recovery/restore_file_from_snapshot";
        public static final String FILE_CHUNK = "internal:index/shard/recovery/file_chunk";
        public static final String CLEAN_FILES = "internal:index/shard/recovery/clean_files";
        public static final String TRANSLOG_OPS = "internal:index/shard/recovery/translog_ops";
        public static final String PREPARE_TRANSLOG = "internal:index/shard/recovery/prepare_translog";
        public static final String FINALIZE = "internal:index/shard/recovery/finalize";
        public static final String HANDOFF_PRIMARY_CONTEXT = "internal:index/shard/recovery/handoff_primary_context";
    }

    private final Client client;
    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;
    private final SnapshotFilesProvider snapshotFilesProvider;

    private final RecoveriesCollection onGoingRecoveries;

    public PeerRecoveryTargetService(
        Client client,
        ThreadPool threadPool,
        TransportService transportService,
        RecoverySettings recoverySettings,
        ClusterService clusterService,
        SnapshotFilesProvider snapshotFilesProvider
    ) {
        this.client = client;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
        this.snapshotFilesProvider = snapshotFilesProvider;
        this.onGoingRecoveries = new RecoveriesCollection(logger, threadPool);

        transportService.registerRequestHandler(
            Actions.FILES_INFO,
            threadPool.executor(ThreadPool.Names.GENERIC),
            RecoveryFilesInfoRequest::new,
            new RecoveryRequestHandler<>() {
                @Override
                protected void handleRequest(RecoveryFilesInfoRequest request, RecoveryTarget target, ActionListener<Void> listener) {
                    target.receiveFileInfo(
                        request.phase1FileNames,
                        request.phase1FileSizes,
                        request.phase1ExistingFileNames,
                        request.phase1ExistingFileSizes,
                        request.totalTranslogOps,
                        listener
                    );
                }
            }
        );
        transportService.registerRequestHandler(
            Actions.RESTORE_FILE_FROM_SNAPSHOT,
            threadPool.executor(ThreadPool.Names.GENERIC),
            RecoverySnapshotFileRequest::new,
            new RecoveryRequestHandler<>() {
                @Override
                protected void handleRequest(RecoverySnapshotFileRequest request, RecoveryTarget target, ActionListener<Void> listener) {
                    target.restoreFileFromSnapshot(request.getRepository(), request.getIndexId(), request.getFileInfo(), listener);
                }
            }
        );
        transportService.registerRequestHandler(
            Actions.FILE_CHUNK,
            threadPool.executor(ThreadPool.Names.GENERIC),
            RecoveryFileChunkRequest::new,
            new FileChunkTransportRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.CLEAN_FILES,
            threadPool.executor(ThreadPool.Names.GENERIC),
            RecoveryCleanFilesRequest::new,
            new RecoveryRequestHandler<>() {
                @Override
                protected void handleRequest(RecoveryCleanFilesRequest request, RecoveryTarget target, ActionListener<Void> listener) {
                    target.cleanFiles(
                        request.totalTranslogOps(),
                        request.getGlobalCheckpoint(),
                        request.sourceMetaSnapshot(),
                        listener.delegateFailure((l, r) -> {
                            Releasable reenableMonitor = target.disableRecoveryMonitor();
                            target.indexShard().afterCleanFiles(() -> {
                                reenableMonitor.close();
                                l.onResponse(null);
                            });
                        })
                    );
                }
            }
        );
        transportService.registerRequestHandler(
            Actions.PREPARE_TRANSLOG,
            threadPool.executor(ThreadPool.Names.GENERIC),
            RecoveryPrepareForTranslogOperationsRequest::new,
            new RecoveryRequestHandler<>() {
                @Override
                protected void handleRequest(
                    RecoveryPrepareForTranslogOperationsRequest request,
                    RecoveryTarget target,
                    ActionListener<Void> listener
                ) {
                    target.prepareForTranslogOperations(request.totalTranslogOps(), listener);
                }
            }
        );
        transportService.registerRequestHandler(
            Actions.TRANSLOG_OPS,
            threadPool.executor(ThreadPool.Names.GENERIC),
            RecoveryTranslogOperationsRequest::new,
            new TranslogOperationsRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.FINALIZE,
            threadPool.executor(ThreadPool.Names.GENERIC),
            RecoveryFinalizeRecoveryRequest::new,
            new RecoveryRequestHandler<>() {
                @Override
                protected void handleRequest(
                    RecoveryFinalizeRecoveryRequest request,
                    RecoveryTarget target,
                    ActionListener<Void> listener
                ) {
                    target.finalizeRecovery(request.globalCheckpoint(), request.trimAboveSeqNo(), listener);
                }
            }
        );
        transportService.registerRequestHandler(
            Actions.HANDOFF_PRIMARY_CONTEXT,
            threadPool.executor(ThreadPool.Names.GENERIC),
            RecoveryHandoffPrimaryContextRequest::new,
            new HandoffPrimaryContextRequestHandler()
        );
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            onGoingRecoveries.cancelRecoveriesForShard(shardId, "shard closed");
        }
    }

    public void startRecovery(
        final IndexShard indexShard,
        final DiscoveryNode sourceNode,
        final long clusterStateVersion,
        final RecoveryListener listener
    ) {
        final Releasable snapshotFileDownloadsPermit = tryAcquireSnapshotDownloadPermits();
        // create a new recovery status, and process...
        final long recoveryId = onGoingRecoveries.startRecovery(
            indexShard,
            sourceNode,
            clusterStateVersion,
            snapshotFilesProvider,
            listener,
            recoverySettings.activityTimeout(),
            snapshotFileDownloadsPermit
        );
        // we fork off quickly here and go async but this is called from the cluster state applier thread too and that can cause
        // assertions to trip if we executed it on the same thread hence we fork off to the generic threadpool.
        threadPool.generic().execute(new RecoveryRunner(recoveryId));
    }

    protected void retryRecovery(final long recoveryId, final Throwable reason, TimeValue retryAfter, TimeValue activityTimeout) {
        logger.trace(() -> format("will retry recovery with id [%s] in [%s]", recoveryId, retryAfter), reason);
        retryRecovery(recoveryId, retryAfter, activityTimeout);
    }

    protected void retryRecovery(final long recoveryId, final String reason, TimeValue retryAfter, TimeValue activityTimeout) {
        logger.trace("will retry recovery with id [{}] in [{}] (reason [{}])", recoveryId, retryAfter, reason);
        retryRecovery(recoveryId, retryAfter, activityTimeout);
    }

    private void retryRecovery(final long recoveryId, final TimeValue retryAfter, final TimeValue activityTimeout) {
        RecoveryTarget newTarget = onGoingRecoveries.resetRecovery(recoveryId, activityTimeout);
        if (newTarget != null) {
            threadPool.scheduleUnlessShuttingDown(retryAfter, threadPool.generic(), new RecoveryRunner(newTarget.recoveryId()));
        }
    }

    protected void reestablishRecovery(final StartRecoveryRequest request, final String reason, TimeValue retryAfter) {
        final long recoveryId = request.recoveryId();
        logger.trace("will try to reestablish recovery with id [{}] in [{}] (reason [{}])", recoveryId, retryAfter, reason);
        threadPool.scheduleUnlessShuttingDown(retryAfter, threadPool.generic(), new RecoveryRunner(recoveryId, request));
    }

    private void doRecovery(final long recoveryId, final StartRecoveryRequest preExistingRequest) {
        final RecoveryRef recoveryRef = onGoingRecoveries.getRecovery(recoveryId);
        if (recoveryRef == null) {
            logger.trace("not running recovery with id [{}] - can not find it (probably finished)", recoveryId);
            return;
        }
        final RecoveryTarget recoveryTarget = recoveryRef.target();
        assert recoveryTarget.sourceNode() != null : "cannot do a recovery without a source node";
        final RecoveryState recoveryState = recoveryTarget.state();
        final RecoveryState.Timer timer = recoveryState.getTimer();
        final IndexShard indexShard = recoveryTarget.indexShard();
        final Releasable onCompletion = Releasables.wrap(recoveryTarget.disableRecoveryMonitor(), recoveryRef);

        // async version of the catch/finally structure we need, but this does nothing with successes so needs further modification below
        final var cleanupOnly = ActionListener.notifyOnce(ActionListener.runBefore(ActionListener.noop().delegateResponse((l, e) -> {
            // this will be logged as warning later on...
            logger.trace("unexpected error while preparing shard for peer recovery, failing recovery", e);
            onGoingRecoveries.failRecovery(
                recoveryId,
                new RecoveryFailedException(recoveryTarget.state(), "failed to prepare shard for recovery", e),
                true
            );
        }), onCompletion::close));

        if (indexShard.routingEntry().isPromotableToPrimary() == false) {
            assert preExistingRequest == null;
            assert indexShard.indexSettings().getIndexMetadata().isSearchableSnapshot() == false;
            client.execute(
                StatelessUnpromotableRelocationAction.TYPE,
                new StatelessUnpromotableRelocationAction.Request(
                    recoveryId,
                    indexShard.shardId(),
                    indexShard.routingEntry().allocationId().getId(),
                    recoveryTarget.clusterStateVersion()
                ),
                cleanupOnly.delegateFailure((l, unused) -> {
                    ActionListener.completeWith(l, () -> {
                        onGoingRecoveries.markRecoveryAsDone(recoveryId);
                        return null;
                    });
                })
            );
            return;
        }

        if (indexShard.routingEntry().isSearchable() == false && recoveryState.getPrimary()) {
            assert preExistingRequest == null;
            assert indexShard.indexSettings().getIndexMetadata().isSearchableSnapshot() == false;
            try (onCompletion) {
                client.execute(
                    StatelessPrimaryRelocationAction.TYPE,
                    new StatelessPrimaryRelocationAction.Request(
                        recoveryId,
                        indexShard.shardId(),
                        transportService.getLocalNode(),
                        indexShard.routingEntry().allocationId().getId(),
                        recoveryTarget.clusterStateVersion()
                    ),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(ActionResponse.Empty ignored) {
                            onGoingRecoveries.markRecoveryAsDone(recoveryId);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            final var cause = ExceptionsHelper.unwrapCause(e);
                            final var sendShardFailure =
                                // these indicate the source shard has already failed, which will independently notify the master and fail
                                // the target shard
                                false == (cause instanceof ShardNotFoundException
                                    || cause instanceof IndexNotFoundException
                                    || cause instanceof AlreadyClosedException);

                            // TODO retries? See RecoveryResponseHandler#handleException
                            onGoingRecoveries.failRecovery(
                                recoveryId,
                                new RecoveryFailedException(recoveryState, null, e),
                                sendShardFailure
                            );
                        }
                    }
                );
                return;
            }
        }

        record StartRecoveryRequestToSend(StartRecoveryRequest startRecoveryRequest, String actionName, TransportRequest requestToSend) {}
        final ActionListener<StartRecoveryRequestToSend> toSendListener = cleanupOnly.map(r -> {
            logger.trace(
                "{} [{}]: recovery from {}",
                r.startRecoveryRequest().shardId(),
                r.actionName(),
                r.startRecoveryRequest().sourceNode()
            );
            transportService.sendRequest(
                r.startRecoveryRequest().sourceNode(),
                r.actionName(),
                r.requestToSend(),
                new RecoveryResponseHandler(r.startRecoveryRequest(), timer)
            );
            return null;
        });

        if (preExistingRequest == null) {
            SubscribableListener
                // run pre-recovery activities
                .newForked(indexShard::preRecovery)
                // recover the shard as far as possible based on data held locally
                .<Long>andThen(l -> {
                    logger.trace("{} preparing shard for peer recovery", recoveryTarget.shardId());
                    indexShard.prepareForIndexRecovery();
                    if (indexShard.indexSettings().getIndexMetadata().isSearchableSnapshot()) {
                        // for archives indices mounted as searchable snapshots, we need to call this
                        indexShard.getIndexEventListener().afterFilesRestoredFromRepository(indexShard);
                    }
                    indexShard.recoverLocallyUpToGlobalCheckpoint(ActionListener.assertOnce(l));
                })
                // peer recovery can consume a lot of disk space, so it's worth cleaning up locally ahead of the attempt
                // operation runs only if the previous operation succeeded, and returns the previous operation's result.
                // Failures at this stage aren't fatal, we can attempt to recover and then clean up again at the end. #104473
                .andThenApply(startingSeqNo -> {
                    Store.MetadataSnapshot snapshot;
                    try {
                        snapshot = indexShard.snapshotStoreMetadata();
                    } catch (IOException e) {
                        // We give up on the contents for any checked exception thrown by snapshotStoreMetadata. We don't want to
                        // allow those to bubble up and interrupt recovery because the subsequent recovery attempt is expected
                        // to fix up these problems for us if it completes successfully.
                        if (e instanceof org.apache.lucene.index.IndexNotFoundException) {
                            // this is the expected case on first recovery, so don't spam the logs with exceptions
                            logger.debug(() -> format("no snapshot found for shard %s, treating as empty", indexShard.shardId()));
                        } else {
                            logger.warn(() -> format("unable to load snapshot for shard %s, treating as empty", indexShard.shardId()), e);
                        }
                        snapshot = Store.MetadataSnapshot.EMPTY;
                    }

                    Store store = indexShard.store();
                    store.incRef();
                    try {
                        logger.debug(() -> format("cleaning up index directory for %s before recovery", indexShard.shardId()));
                        store.cleanupAndVerify("cleanup before peer recovery", snapshot);
                    } finally {
                        store.decRef();
                    }
                    return startingSeqNo;
                })
                // now construct the start-recovery request
                .andThenApply(startingSeqNo -> {
                    assert startingSeqNo == UNASSIGNED_SEQ_NO || recoveryTarget.state().getStage() == RecoveryState.Stage.TRANSLOG
                        : "unexpected recovery stage [" + recoveryTarget.state().getStage() + "] starting seqno [ " + startingSeqNo + "]";
                    try {
                        recoveryTarget.incRef();
                        final var startRequest = getStartRecoveryRequest(logger, clusterService.localNode(), recoveryTarget, startingSeqNo);
                        return new StartRecoveryRequestToSend(startRequest, PeerRecoverySourceService.Actions.START_RECOVERY, startRequest);
                    } finally {
                        recoveryTarget.decRef();
                    }
                })
                // finally send the start-recovery request
                .addListener(toSendListener);
        } else {
            toSendListener.onResponse(
                new StartRecoveryRequestToSend(
                    preExistingRequest,
                    PeerRecoverySourceService.Actions.REESTABLISH_RECOVERY,
                    new ReestablishRecoveryRequest(recoveryId, preExistingRequest.shardId(), preExistingRequest.targetAllocationId())
                )
            );
        }
    }

    // Visible for testing
    public Releasable tryAcquireSnapshotDownloadPermits() {
        return recoverySettings.tryAcquireSnapshotDownloadPermits();
    }

    // Visible for testing
    public int ongoingRecoveryCount() {
        return onGoingRecoveries.size();
    }

    /**
     * Prepare the start recovery request.
     *
     * @param logger         the logger
     * @param localNode      the local node of the recovery target
     * @param recoveryTarget the target of the recovery
     * @param startingSeqNo  a sequence number that an operation-based peer recovery can start with.
     *                       This is the first operation after the local checkpoint of the safe commit if exists.
     * @return a start recovery request
     */
    public static StartRecoveryRequest getStartRecoveryRequest(
        Logger logger,
        DiscoveryNode localNode,
        RecoveryTarget recoveryTarget,
        long startingSeqNo
    ) {
        final StartRecoveryRequest request;
        logger.trace("{} collecting local files for [{}]", recoveryTarget.shardId(), recoveryTarget.sourceNode());

        Store.MetadataSnapshot metadataSnapshot;
        try {
            if (recoveryTarget.indexShard().routingEntry().isPromotableToPrimary()) {
                metadataSnapshot = recoveryTarget.indexShard().snapshotStoreMetadata();
                // Make sure that the current translog is consistent with the Lucene index; otherwise, we have to throw away the Lucene
                // index.
                try {
                    final long globalCheckpoint = recoveryTarget.indexShard()
                        .readGlobalCheckpointForRecovery(metadataSnapshot.commitUserData());
                    assert globalCheckpoint + 1 >= startingSeqNo : "invalid startingSeqNo " + startingSeqNo + " >= " + globalCheckpoint;
                } catch (IOException | TranslogCorruptedException e) {
                    logGlobalCheckpointWarning(logger, startingSeqNo, e);
                    metadataSnapshot = Store.MetadataSnapshot.EMPTY;
                    startingSeqNo = UNASSIGNED_SEQ_NO;
                }
            } else {
                metadataSnapshot = Store.MetadataSnapshot.EMPTY;
                startingSeqNo = UNASSIGNED_SEQ_NO;
            }
        } catch (final org.apache.lucene.index.IndexNotFoundException e) {
            // happens on an empty folder. no need to log
            assert startingSeqNo == UNASSIGNED_SEQ_NO : startingSeqNo;
            logger.trace("{} shard folder empty, recovering all files", recoveryTarget);
            metadataSnapshot = Store.MetadataSnapshot.EMPTY;
        } catch (final IOException e) {
            if (startingSeqNo != UNASSIGNED_SEQ_NO) {
                logListingLocalFilesWarning(logger, startingSeqNo, e);
                startingSeqNo = UNASSIGNED_SEQ_NO;
            } else {
                logger.warn("error while listing local files, recovering as if there are none", e);
            }
            metadataSnapshot = Store.MetadataSnapshot.EMPTY;
        }
        logger.trace("{} local file count [{}]", recoveryTarget.shardId(), metadataSnapshot.size());
        request = new StartRecoveryRequest(
            recoveryTarget.shardId(),
            recoveryTarget.indexShard().routingEntry().allocationId().getId(),
            recoveryTarget.sourceNode(),
            localNode,
            recoveryTarget.clusterStateVersion(),
            metadataSnapshot,
            recoveryTarget.state().getPrimary(),
            recoveryTarget.recoveryId(),
            startingSeqNo,
            recoveryTarget.hasPermitToDownloadSnapshotFiles()
        );
        return request;
    }

    private static void logListingLocalFilesWarning(Logger logger, long startingSeqNo, IOException e) {
        logger.warn(
            () -> format(
                "error while listing local files, resetting the starting sequence number from %s "
                    + "to unassigned and recovering as if there are none",
                startingSeqNo
            ),
            e
        );
    }

    private static void logGlobalCheckpointWarning(Logger logger, long startingSeqNo, Exception e) {
        logger.warn(
            () -> format(
                "error while reading global checkpoint from translog, "
                    + "resetting the starting sequence number from %s to unassigned and recovering as if there are none",
                startingSeqNo
            ),
            e
        );
    }

    public interface RecoveryListener {
        void onRecoveryDone(
            RecoveryState state,
            ShardLongFieldRange timestampMillisFieldRange,
            ShardLongFieldRange eventIngestedMillisFieldRange
        );

        void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure);
    }

    class HandoffPrimaryContextRequestHandler implements TransportRequestHandler<RecoveryHandoffPrimaryContextRequest> {

        @Override
        public void messageReceived(final RecoveryHandoffPrimaryContextRequest request, final TransportChannel channel, Task task) {
            final RecoveryRef recoveryRef = getRecoveryRef(request.recoveryId(), request.shardId());
            boolean success = false;
            try {
                recoveryRef.target()
                    .handoffPrimaryContext(
                        request.primaryContext(),
                        ActionListener.runBefore(
                            new ChannelActionListener<>(channel).map(v -> ActionResponse.Empty.INSTANCE),
                            recoveryRef::close
                        )
                    );
                success = true;
            } finally {
                if (success == false) {
                    recoveryRef.close();
                }
            }
        }

    }

    /**
     * Acquire a reference to the given recovery, throwing an {@link IndexShardClosedException} if the recovery is unknown.
     */
    public RecoveryRef getRecoveryRef(long recoveryId, ShardId shardId) {
        return onGoingRecoveries.getRecoverySafe(recoveryId, shardId);
    }

    private class TranslogOperationsRequestHandler extends RecoveryRequestHandler<RecoveryTranslogOperationsRequest> {

        @Override
        protected void handleRequest(
            RecoveryTranslogOperationsRequest request,
            RecoveryTarget recoveryTarget,
            ActionListener<Void> listener
        ) {
            performTranslogOps(request, listener, recoveryTarget);
        }

        @Override
        protected CheckedFunction<Void, TransportResponse, Exception> responseMapping(RecoveryTarget recoveryTarget) {
            return v -> {
                try {
                    recoveryTarget.incRef();
                    return new RecoveryTranslogOperationsResponse(recoveryTarget.indexShard().getLocalCheckpoint());
                } finally {
                    recoveryTarget.decRef();
                }
            };
        }

        private void performTranslogOps(
            final RecoveryTranslogOperationsRequest request,
            final ActionListener<Void> listener,
            final RecoveryTarget recoveryTarget
        ) {
            final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
            final Consumer<Exception> retryOnMappingException = exception -> {
                // in very rare cases a translog replay from primary is processed before a mapping update on this node
                // which causes local mapping changes since the mapping (clusterstate) might not have arrived on this node.
                logger.debug("delaying recovery due to missing mapping changes", exception);
                // we do not need to use a timeout here since the entire recovery mechanism has an inactivity protection (it will be
                // canceled)
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        threadPool.generic().execute(ActionRunnable.wrap(listener, l -> {
                            try (RecoveryRef recoveryRef = getRecoveryRef(request.recoveryId(), request.shardId())) {
                                performTranslogOps(request, l, recoveryRef.target());
                            }
                        }));
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new ElasticsearchException("cluster service was closed while waiting for mapping updates"));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        // note that we do not use a timeout (see comment above)
                        listener.onFailure(
                            new ElasticsearchTimeoutException("timed out waiting for mapping updates " + "(timeout [" + timeout + "])")
                        );
                    }
                });
            };
            ClusterState state = clusterService.state();
            final ProjectMetadata project = state.metadata().projectFor(request.shardId().getIndex());
            final IndexMetadata indexMetadata = project.index(request.shardId().getIndex());
            final long mappingVersionOnTarget = indexMetadata != null ? indexMetadata.getMappingVersion() : 0L;
            recoveryTarget.indexTranslogOperations(
                request.operations(),
                request.totalTranslogOps(),
                request.maxSeenAutoIdTimestampOnPrimary(),
                request.maxSeqNoOfUpdatesOrDeletesOnPrimary(),
                request.retentionLeases(),
                request.mappingVersionOnPrimary(),
                ActionListener.wrap(checkpoint -> listener.onResponse(null), e -> {
                    // do not retry if the mapping on replica is at least as recent as the mapping
                    // that the primary used to index the operations in the request.
                    if (mappingVersionOnTarget < request.mappingVersionOnPrimary() && e instanceof MapperException) {
                        retryOnMappingException.accept(e);
                    } else {
                        listener.onFailure(e);
                    }
                })
            );
        }
    }

    private abstract class RecoveryRequestHandler<T extends RecoveryTransportRequest> implements TransportRequestHandler<T> {

        @Override
        public final void messageReceived(final T request, TransportChannel channel, Task task) throws Exception {
            try (RecoveryRef recoveryRef = getRecoveryRef(request.recoveryId(), request.shardId())) {
                final RecoveryTarget recoveryTarget = recoveryRef.target();
                final ActionListener<Void> resultListener = new ChannelActionListener<>(channel).map(responseMapping(recoveryTarget));

                final long requestSeqNo = request.requestSeqNo();
                final ActionListener<Void> listener = requestSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO
                    ? resultListener
                    : recoveryTarget.markRequestReceivedAndCreateListener(requestSeqNo, resultListener);
                if (listener != null) {
                    handleRequest(request, recoveryTarget, listener);
                }
            }
        }

        protected CheckedFunction<Void, TransportResponse, Exception> responseMapping(RecoveryTarget recoveryTarget) {
            return v -> ActionResponse.Empty.INSTANCE;
        }

        protected abstract void handleRequest(T request, RecoveryTarget target, ActionListener<Void> listener) throws IOException;
    }

    private class FileChunkTransportRequestHandler extends RecoveryRequestHandler<RecoveryFileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        protected void handleRequest(RecoveryFileChunkRequest request, RecoveryTarget target, ActionListener<Void> listener)
            throws IOException {
            final RecoveryState.Index indexState = target.state().getIndex();
            if (request.sourceThrottleTimeInNanos() != RecoveryState.Index.UNKNOWN) {
                indexState.addSourceThrottling(request.sourceThrottleTimeInNanos());
            }

            RateLimiter rateLimiter = recoverySettings.rateLimiter();
            if (rateLimiter != null) {
                long bytes = bytesSinceLastPause.addAndGet(request.content().length());
                if (bytes > rateLimiter.getMinPauseCheckBytes()) {
                    // Time to pause
                    bytesSinceLastPause.addAndGet(-bytes);
                    long throttleTimeInNanos = rateLimiter.pause(bytes);
                    indexState.addTargetThrottling(throttleTimeInNanos);
                    target.indexShard().recoveryStats().addThrottleTime(throttleTimeInNanos);
                }
            }
            target.writeFileChunk(
                request.metadata(),
                request.position(),
                request.content(),
                request.lastChunk(),
                request.totalTranslogOps(),
                listener
            );
        }
    }

    class RecoveryRunner extends AbstractRunnable {

        final long recoveryId;
        private final StartRecoveryRequest startRecoveryRequest;

        RecoveryRunner(long recoveryId) {
            this(recoveryId, null);
        }

        RecoveryRunner(long recoveryId, StartRecoveryRequest startRecoveryRequest) {
            this.recoveryId = recoveryId;
            this.startRecoveryRequest = startRecoveryRequest;
        }

        @Override
        public void onFailure(Exception e) {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecovery(recoveryId)) {
                if (recoveryRef != null) {
                    logger.error(() -> "unexpected error during recovery [" + recoveryId + "], failing shard", e);
                    onGoingRecoveries.failRecovery(
                        recoveryId,
                        new RecoveryFailedException(recoveryRef.target().state(), "unexpected error", e),
                        true // be safe
                    );
                } else {
                    logger.debug(() -> "unexpected error during recovery, but recovery id [" + recoveryId + "] is finished", e);
                }
            }
        }

        @Override
        public void doRun() {
            doRecovery(recoveryId, startRecoveryRequest);
        }
    }

    private class RecoveryResponseHandler implements TransportResponseHandler<RecoveryResponse> {

        private final long recoveryId;
        private final StartRecoveryRequest request;
        private final RecoveryState.Timer timer;

        private RecoveryResponseHandler(final StartRecoveryRequest request, final RecoveryState.Timer timer) {
            this.recoveryId = request.recoveryId();
            this.request = request;
            this.timer = timer;
        }

        @Override
        public void handleResponse(RecoveryResponse recoveryResponse) {
            final TimeValue recoveryTime = new TimeValue(timer.time());
            // do this through ongoing recoveries to remove it from the collection
            onGoingRecoveries.markRecoveryAsDone(recoveryId);
            if (logger.isTraceEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append('[')
                    .append(request.shardId().getIndex().getName())
                    .append(']')
                    .append('[')
                    .append(request.shardId().id())
                    .append("] ");
                sb.append("recovery completed from ").append(request.sourceNode()).append(", took[").append(recoveryTime).append("]\n");
                sb.append("   phase1: recovered_files [")
                    .append(recoveryResponse.phase1FileNames.size())
                    .append("]")
                    .append(" with total_size of [")
                    .append(ByteSizeValue.ofBytes(recoveryResponse.phase1TotalSize))
                    .append("]")
                    .append(", took [")
                    .append(timeValueMillis(recoveryResponse.phase1Time))
                    .append("], throttling_wait [")
                    .append(timeValueMillis(recoveryResponse.phase1ThrottlingWaitTime))
                    .append(']')
                    .append("\n");
                sb.append("         : reusing_files   [")
                    .append(recoveryResponse.phase1ExistingFileNames.size())
                    .append("] with total_size of [")
                    .append(ByteSizeValue.ofBytes(recoveryResponse.phase1ExistingTotalSize))
                    .append("]\n");
                sb.append("   phase2: start took [").append(timeValueMillis(recoveryResponse.startTime)).append("]\n");
                sb.append("         : recovered [")
                    .append(recoveryResponse.phase2Operations)
                    .append("]")
                    .append(" transaction log operations")
                    .append(", took [")
                    .append(timeValueMillis(recoveryResponse.phase2Time))
                    .append("]")
                    .append("\n");
                logger.trace("{}", sb);
            } else {
                logger.debug("{} recovery done from [{}], took [{}]", request.shardId(), request.sourceNode(), recoveryTime);
            }
        }

        @Override
        public void handleException(TransportException e) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    () -> format("[%s][%s] Got exception on recovery", request.shardId().getIndex().getName(), request.shardId().id()),
                    e
                );
            }
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (transportService.lifecycleState() != Lifecycle.State.STARTED) {
                // the node is shutting down, we just fail the recovery to release resources
                onGoingRecoveries.failRecovery(recoveryId, new RecoveryFailedException(request, "node is shutting down", cause), false);
                return;
            }
            if (cause instanceof CancellableThreads.ExecutionCancelledException) {
                // this can also come from the source wrapped in a RemoteTransportException
                onGoingRecoveries.failRecovery(
                    recoveryId,
                    new RecoveryFailedException(request, "source has canceled the recovery", cause),
                    false
                );
                return;
            }
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }
            // do it twice, in case we have double transport exception
            cause = ExceptionsHelper.unwrapCause(cause);
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }

            // here, we would add checks against exception that need to be retried (and not removeAndClean in this case)

            if (cause instanceof IllegalIndexShardStateException
                || cause instanceof IndexNotFoundException
                || cause instanceof ShardNotFoundException) {
                // if the target is not ready yet, retry
                retryRecovery(
                    recoveryId,
                    "remote shard not ready",
                    recoverySettings.retryDelayStateSync(),
                    recoverySettings.activityTimeout()
                );
                return;
            }

            // PeerRecoveryNotFound is returned when the source node cannot find the recovery requested by
            // the REESTABLISH_RECOVERY request. In this case, we delay and then attempt to restart.
            if (cause instanceof DelayRecoveryException || cause instanceof PeerRecoveryNotFound) {
                retryRecovery(recoveryId, cause, recoverySettings.retryDelayStateSync(), recoverySettings.activityTimeout());
                return;
            }

            if (cause instanceof ConnectTransportException) {
                logger.info(
                    "recovery of {} from [{}] interrupted by network disconnect, will retry in [{}]; cause: [{}]",
                    request.shardId(),
                    request.sourceNode(),
                    recoverySettings.retryDelayNetwork(),
                    cause.getMessage()
                );
                reestablishRecovery(request, cause.getMessage(), recoverySettings.retryDelayNetwork());
                return;
            }

            if (cause instanceof AlreadyClosedException) {
                onGoingRecoveries.failRecovery(recoveryId, new RecoveryFailedException(request, "source shard is closed", cause), false);
                return;
            }

            onGoingRecoveries.failRecovery(recoveryId, new RecoveryFailedException(request, e), true);
        }

        @Override
        public Executor executor() {
            // we do some heavy work like refreshes in the response so fork off to the generic threadpool
            return threadPool.generic();
        }

        @Override
        public RecoveryResponse read(StreamInput in) throws IOException {
            return new RecoveryResponse(in);
        }
    }
}
