/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotRecoveringException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.repositories.IndexId;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.cluster.metadata.IndexMetadataVerifier.isReadOnlyVerified;
import static org.elasticsearch.core.Strings.format;

/**
 * Represents a recovery where the current node is the target node of the recovery. To track recoveries in a central place, instances of
 * this class are created through {@link RecoveriesCollection}.
 */
public class RecoveryTarget extends AbstractRefCounted implements RecoveryTargetHandler {

    private final Logger logger;

    private static final AtomicLong idGenerator = new AtomicLong();

    private static final String RECOVERY_PREFIX = "recovery.";

    private final ShardId shardId;
    private final long recoveryId;
    private final IndexShard indexShard;
    private final DiscoveryNode sourceNode;
    private final long clusterStateVersion;
    private final SnapshotFilesProvider snapshotFilesProvider;
    private volatile MultiFileWriter multiFileWriter;
    private final RecoveryRequestTracker requestTracker = new RecoveryRequestTracker();
    private final Store store;
    private final PeerRecoveryTargetService.RecoveryListener listener;

    private final AtomicBoolean finished = new AtomicBoolean();

    private final CancellableThreads cancellableThreads;

    // last time this status was accessed
    private volatile long lastAccessTime = System.nanoTime();

    private final AtomicInteger recoveryMonitorBlocks = new AtomicInteger();

    @Nullable // if we're not downloading files from snapshots in this recovery
    private volatile Releasable snapshotFileDownloadsPermit;

    // placeholder for snapshotFileDownloadsPermit for use when this RecoveryTarget has been replaced by a new one due to a retry
    private static final Releasable SNAPSHOT_FILE_DOWNLOADS_PERMIT_PLACEHOLDER_FOR_RETRY = Releasables.wrap();

    // latch that can be used to blockingly wait for RecoveryTarget to be closed
    private final CountDownLatch closedLatch = new CountDownLatch(1);

    /**
     * Creates a new recovery target object that represents a recovery to the provided shard.
     *
     * @param indexShard                  local shard where we want to recover to
     * @param sourceNode                  source node of the recovery where we recover from
     * @param clusterStateVersion         version of the cluster state that initiated the recovery
     * @param snapshotFileDownloadsPermit a permit that allows to download files from a snapshot,
     *                                    limiting the concurrent snapshot file downloads per node
     *                                    preventing the exhaustion of repository resources.
     * @param listener                    called when recovery is completed/failed
     */
    @SuppressWarnings("this-escape")
    public RecoveryTarget(
        IndexShard indexShard,
        DiscoveryNode sourceNode,
        long clusterStateVersion,
        SnapshotFilesProvider snapshotFilesProvider,
        @Nullable Releasable snapshotFileDownloadsPermit,
        PeerRecoveryTargetService.RecoveryListener listener
    ) {
        this.cancellableThreads = new CancellableThreads();
        this.recoveryId = idGenerator.incrementAndGet();
        this.listener = listener;
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.sourceNode = sourceNode;
        this.clusterStateVersion = clusterStateVersion;
        this.snapshotFilesProvider = snapshotFilesProvider;
        this.snapshotFileDownloadsPermit = snapshotFileDownloadsPermit;
        this.shardId = indexShard.shardId();
        this.store = indexShard.store();
        this.multiFileWriter = createMultiFileWriter();
        // make sure the store is not released until we are done.
        store.incRef();
        indexShard.recoveryStats().incCurrentAsTarget();
    }

    private void recreateMultiFileWriter() {
        // Sometimes we need to clear the downloaded data and start from scratch
        // i.e. when we're recovering from a snapshot that's physically different to the
        // source node index files. In that case we create a new MultiFileWriter using a
        // different tempFilePrefix and close the previous writer that would take care of
        // cleaning itself once all the outstanding writes finish.
        multiFileWriter.close();
        this.multiFileWriter = createMultiFileWriter();
    }

    private MultiFileWriter createMultiFileWriter() {
        final String tempFilePrefix = RECOVERY_PREFIX + UUIDs.randomBase64UUID() + ".";
        return new MultiFileWriter(indexShard.store(), indexShard.recoveryState().getIndex(), tempFilePrefix, logger);
    }

    /**
     * Returns a fresh recovery target to retry recovery from the same source node onto the same shard and using the same listener.
     *
     * @return a copy of this recovery target
     */
    public RecoveryTarget retryCopy() {
        // If we're retrying we should remove the reference from this instance as the underlying resources
        // get released after the retry copy is created
        Releasable snapshotFileDownloadsPermitCopy = snapshotFileDownloadsPermit;
        if (snapshotFileDownloadsPermitCopy != null) {
            snapshotFileDownloadsPermit = SNAPSHOT_FILE_DOWNLOADS_PERMIT_PLACEHOLDER_FOR_RETRY;
        }
        return new RecoveryTarget(
            indexShard,
            sourceNode,
            clusterStateVersion,
            snapshotFilesProvider,
            snapshotFileDownloadsPermitCopy,
            listener
        );
    }

    @Nullable
    public ActionListener<Void> markRequestReceivedAndCreateListener(long requestSeqNo, ActionListener<Void> listener) {
        return requestTracker.markReceivedAndCreateListener(requestSeqNo, listener);
    }

    public long recoveryId() {
        return recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public IndexShard indexShard() {
        assert hasReferences();
        return indexShard;
    }

    public DiscoveryNode sourceNode() {
        return this.sourceNode;
    }

    public long clusterStateVersion() {
        return clusterStateVersion;
    }

    public RecoveryState state() {
        return indexShard.recoveryState();
    }

    public CancellableThreads cancellableThreads() {
        return cancellableThreads;
    }

    public boolean hasPermitToDownloadSnapshotFiles() {
        return snapshotFileDownloadsPermit != null;
    }

    /** return the last time this RecoveryStatus was used (based on System.nanoTime() */
    public long lastAccessTime() {
        if (recoveryMonitorBlocks.get() == 0) {
            return lastAccessTime;
        }
        return System.nanoTime();
    }

    /** sets the lasAccessTime flag to now */
    public void setLastAccessTime() {
        lastAccessTime = System.nanoTime();
    }

    /**
     * Set flag to signal to {@link org.elasticsearch.indices.recovery.RecoveriesCollection.RecoveryMonitor} that it must not cancel this
     * recovery temporarily. This is used by the recovery clean files step to avoid recovery failure in case a long running condition was
     * added to the shard via {@link IndexShard#addCleanFilesDependency()}.
     *
     * @return releasable that once closed will re-enable liveness checks by the recovery monitor
     */
    public Releasable disableRecoveryMonitor() {
        recoveryMonitorBlocks.incrementAndGet();
        return Releasables.releaseOnce(() -> {
            setLastAccessTime();
            recoveryMonitorBlocks.decrementAndGet();
        });
    }

    public Store store() {
        assert hasReferences();
        return store;
    }

    /**
     * Closes the current recovery target and waits up to a certain timeout for resources to be freed.
     * Returns true if resetting the recovery was successful, false if the recovery target is already cancelled / failed or marked as done.
     */
    boolean resetRecovery(CancellableThreads newTargetCancellableThreads) throws IOException {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("reset of recovery with shard {} and id [{}]", shardId, recoveryId);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now.
                decRef();
            }
            try {
                newTargetCancellableThreads.execute(closedLatch::await);
            } catch (CancellableThreads.ExecutionCancelledException e) {
                logger.trace(
                    "new recovery target cancelled for shard {} while waiting on old recovery target with id [{}] to close",
                    shardId,
                    recoveryId
                );
                return false;
            }
            RecoveryState.Stage stage = indexShard.recoveryState().getStage();
            if (indexShard.recoveryState().getPrimary() && (stage == RecoveryState.Stage.FINALIZE || stage == RecoveryState.Stage.DONE)) {
                // once primary relocation has moved past the finalization step, the relocation source can put the target into primary mode
                // and start indexing as primary into the target shard (see TransportReplicationAction). Resetting the target shard in this
                // state could mean that indexing is halted until the recovery retry attempt is completed and could also destroy existing
                // documents indexed and acknowledged before the reset.
                assert stage != RecoveryState.Stage.DONE : "recovery should not have completed when it's being reset";
                throw new IllegalStateException("cannot reset recovery as previous attempt made it past finalization step");
            }
            indexShard.performRecoveryRestart();
            return true;
        }
        return false;
    }

    /**
     * cancel the recovery. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     * <p>
     * if {@link #cancellableThreads()} was used, the threads will be interrupted.
     */
    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("recovery canceled (reason: [{}])", reason);
                cancellableThreads.cancel(reason);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
        }
    }

    /**
     * fail the recovery and call listener
     *
     * @param e                exception that encapsulating the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     */
    public void fail(RecoveryFailedException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                notifyListener(e, sendShardFailure);
            } finally {
                try {
                    cancellableThreads.cancel("failed recovery [" + ExceptionsHelper.stackTrace(e) + "]");
                } finally {
                    // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                    decRef();
                }
            }
        }
    }

    public void notifyListener(RecoveryFailedException e, boolean sendShardFailure) {
        listener.onRecoveryFailure(e, sendShardFailure);
    }

    /** mark the current recovery as done */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            assert multiFileWriter.tempFileNames.isEmpty() : "not all temporary files are renamed";
            indexShard.postRecovery("peer recovery done", ActionListener.runBefore(new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    listener.onRecoveryDone(state(), indexShard.getTimestampRange(), indexShard.getEventIngestedRange());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("recovery failed after being marked as done", e);
                    notifyListener(new RecoveryFailedException(state(), "Recovery failed on post recovery step", e), true);
                }
            }, this::decRef));
        }
    }

    @Override
    protected void closeInternal() {
        assert recoveryMonitorBlocks.get() == 0;
        try {
            multiFileWriter.close();
        } finally {
            // free store. increment happens in constructor
            store.decRef();
            indexShard.recoveryStats().decCurrentAsTarget();
            closedLatch.countDown();
            releaseSnapshotFileDownloadsPermit();
        }
    }

    private void releaseSnapshotFileDownloadsPermit() {
        if (snapshotFileDownloadsPermit != null) {
            snapshotFileDownloadsPermit.close();
        }
    }

    @Override
    public String toString() {
        return shardId + " [" + recoveryId + "]";
    }

    /*** Implementation of {@link RecoveryTargetHandler } */

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            state().getIndex().setFileDetailsComplete(); // ops-based recoveries don't send the file details
            state().getTranslog().totalOperations(totalTranslogOps);
            indexShard().openEngineAndSkipTranslogRecovery();
            return null;
        });
    }

    @Override
    public void finalizeRecovery(final long globalCheckpoint, final long trimAboveSeqNo, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            indexShard.updateGlobalCheckpointOnReplica(globalCheckpoint, "finalizing recovery");
            // Persist the global checkpoint.
            indexShard.sync();
            indexShard.persistRetentionLeases();
            if (trimAboveSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                // We should erase all translog operations above trimAboveSeqNo as we have received either the same or a newer copy
                // from the recovery source in phase2. Rolling a new translog generation is not strictly required here for we won't
                // trim the current generation. It's merely to satisfy the assumption that the current generation does not have any
                // operation that would be trimmed (see TranslogWriter#assertNoSeqAbove). This assumption does not hold for peer
                // recovery because we could have received operations above startingSeqNo from the previous primary terms.
                indexShard.rollTranslogGeneration();
                // the flush or translog generation threshold can be reached after we roll a new translog
                indexShard.afterWriteOperation();
                indexShard.trimOperationOfPreviousPrimaryTerms(trimAboveSeqNo);
            }
            if (hasUncommittedOperations()) {
                indexShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            }
            indexShard.finalizeRecovery();
            return null;
        });
    }

    private boolean hasUncommittedOperations() throws IOException {
        long localCheckpointOfCommit = Long.parseLong(indexShard.commitStats().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        return indexShard.countChanges("peer-recovery", localCheckpointOfCommit + 1, Long.MAX_VALUE) > 0;
    }

    @Override
    public void handoffPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            indexShard.activateWithPrimaryContext(primaryContext);
            return null;
        });
    }

    @Override
    public void indexTranslogOperations(
        final List<Translog.Operation> operations,
        final int totalTranslogOps,
        final long maxSeenAutoIdTimestampOnPrimary,
        final long maxSeqNoOfDeletesOrUpdatesOnPrimary,
        final RetentionLeases retentionLeases,
        final long mappingVersionOnPrimary,
        final ActionListener<Long> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            final RecoveryState.Translog translog = state().getTranslog();
            translog.totalOperations(totalTranslogOps);
            assert indexShard().recoveryState() == state();
            if (indexShard().state() != IndexShardState.RECOVERING) {
                throw new IndexShardNotRecoveringException(shardId, indexShard().state());
            }
            /*
             * The maxSeenAutoIdTimestampOnPrimary received from the primary is at least the highest auto_id_timestamp from any operation
             * will be replayed. Bootstrapping this timestamp here will disable the optimization for original append-only requests
             * (source of these operations) replicated via replication. Without this step, we may have duplicate documents if we
             * replay these operations first (without timestamp), then optimize append-only requests (with timestamp).
             */
            indexShard().updateMaxUnsafeAutoIdTimestamp(maxSeenAutoIdTimestampOnPrimary);
            /*
             * Bootstrap the max_seq_no_of_updates from the primary to make sure that the max_seq_no_of_updates on this replica when
             * replaying any of these operations will be at least the max_seq_no_of_updates on the primary when that op was executed on.
             */
            indexShard().advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfDeletesOrUpdatesOnPrimary);
            /*
             * We have to update the retention leases before we start applying translog operations to ensure we are retaining according to
             * the policy.
             */
            indexShard().updateRetentionLeasesOnReplica(retentionLeases);
            for (Translog.Operation operation : operations) {
                Engine.Result result = indexShard().applyTranslogOperation(operation, Engine.Operation.Origin.PEER_RECOVERY);
                if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                    throw new MapperException("mapping updates are not allowed [" + operation + "]");
                }
                if (result.getFailure() != null) {
                    if (Assertions.ENABLED && result.getFailure() instanceof MapperException == false) {
                        throw new AssertionError("unexpected failure while replicating translog entry", result.getFailure());
                    }
                    ExceptionsHelper.reThrowIfNotNull(result.getFailure());
                }
            }
            // update stats only after all operations completed (to ensure that mapping updates don't mess with stats)
            translog.incrementRecoveredOperations(operations.size());
            indexShard().sync();
            // roll over / flush / trim if needed
            indexShard().afterWriteOperation();
            return indexShard().getLocalCheckpoint();
        });
    }

    @Override
    public void receiveFileInfo(
        List<String> phase1FileNames,
        List<Long> phase1FileSizes,
        List<String> phase1ExistingFileNames,
        List<Long> phase1ExistingFileSizes,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            indexShard.resetRecoveryStage();
            indexShard.prepareForIndexRecovery();
            recreateMultiFileWriter();
            final RecoveryState.Index index = state().getIndex();
            for (int i = 0; i < phase1ExistingFileNames.size(); i++) {
                index.addFileDetail(phase1ExistingFileNames.get(i), phase1ExistingFileSizes.get(i), true);
            }
            for (int i = 0; i < phase1FileNames.size(); i++) {
                index.addFileDetail(phase1FileNames.get(i), phase1FileSizes.get(i), false);
            }
            index.setFileDetailsComplete();
            state().getTranslog().totalOperations(totalTranslogOps);
            state().getTranslog().totalOperationsOnStart(totalTranslogOps);
            return null;
        });
    }

    @Override
    public void cleanFiles(
        int totalTranslogOps,
        long globalCheckpoint,
        Store.MetadataSnapshot sourceMetadata,
        ActionListener<Void> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            state().getTranslog().totalOperations(totalTranslogOps);
            // first, we go and move files that were created with the recovery id suffix to
            // the actual names, its ok if we have a corrupted index here, since we have replicas
            // to recover from in case of a full cluster shutdown just when this code executes...
            multiFileWriter.renameAllTempFiles();
            final Store store = store();
            store.incRef();
            try {
                if (indexShard.routingEntry().isPromotableToPrimary()) {
                    store.cleanupAndVerify("recovery CleanFilesRequestHandler", sourceMetadata);
                    bootstrap(indexShard, globalCheckpoint);
                } else {
                    indexShard.setGlobalCheckpointIfUnpromotable(globalCheckpoint);
                }
                if (indexShard.getRetentionLeases().leases().isEmpty()) {
                    // if empty, may be a fresh IndexShard, so write an empty leases file to disk
                    indexShard.persistRetentionLeases();
                    assert indexShard.loadRetentionLeases().leases().isEmpty();
                } else {
                    assert indexShard.assertRetentionLeasesPersisted();
                }
                indexShard.maybeCheckIndex();
                state().setRemoteTranslogStage();
            } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
                // this is a fatal exception at this stage.
                // this means we transferred files from the remote that have not be checksummed and they are
                // broken. We have to clean up this shard entirely, remove all files and bubble it up to the
                // source shard since this index might be broken there as well? The Source can handle this and checks
                // its content on disk if possible.
                try {
                    try {
                        store.removeCorruptionMarker();
                    } finally {
                        Lucene.cleanLuceneIndex(store.directory()); // clean up and delete all files
                    }
                } catch (Exception e) {
                    logger.debug("Failed to clean lucene index", e);
                    ex.addSuppressed(e);
                }
                RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
                fail(rfe, true);
                throw rfe;
            } catch (Exception ex) {
                RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
                fail(rfe, true);
                throw rfe;
            } finally {
                store.decRef();
            }
            return null;
        });
    }

    @Override
    public void writeFileChunk(
        StoreFileMetadata fileMetadata,
        long position,
        ReleasableBytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        try {
            state().getTranslog().totalOperations(totalTranslogOps);
            multiFileWriter.writeFileChunk(fileMetadata, position, content, lastChunk);
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void restoreFileFromSnapshot(
        String repository,
        IndexId indexId,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        ActionListener<Void> listener
    ) {
        assert hasReferences();
        assert hasPermitToDownloadSnapshotFiles();

        try (
            InputStream inputStream = snapshotFilesProvider.getInputStreamForSnapshotFile(
                repository,
                indexId,
                shardId,
                fileInfo,
                this::registerThrottleTime
            )
        ) {
            StoreFileMetadata metadata = fileInfo.metadata();
            int readSnapshotFileBufferSize = snapshotFilesProvider.getReadSnapshotFileBufferSizeForRepo(repository);
            multiFileWriter.writeFile(metadata, readSnapshotFileBufferSize, new FilterInputStream(inputStream) {
                @Override
                public int read() throws IOException {
                    cancellableThreads.checkForCancel();
                    return super.read();
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    cancellableThreads.checkForCancel();
                    return super.read(b, off, len);
                }
            });
            listener.onResponse(null);
        } catch (Exception e) {
            logger.debug(() -> format("Unable to recover snapshot file %s from repository %s", fileInfo, repository), e);
            listener.onFailure(e);
        }
    }

    private void registerThrottleTime(long throttleTimeInNanos) {
        state().getIndex().addTargetThrottling(throttleTimeInNanos);
        indexShard.recoveryStats().addThrottleTime(throttleTimeInNanos);
    }

    /** Get a temporary name for the provided file name. */
    public String getTempNameForFile(String origFile) {
        return multiFileWriter.getTempNameForFile(origFile);
    }

    private static void bootstrap(final IndexShard indexShard, long globalCheckpoint) throws IOException {
        assert indexShard.routingEntry().isPromotableToPrimary();
        final var store = indexShard.store();
        store.incRef();
        try {
            final var translogLocation = indexShard.shardPath().resolveTranslog();
            if (indexShard.hasTranslog() == false) {
                if (Assertions.ENABLED) {
                    if (indexShard.indexSettings().getIndexMetadata().isSearchableSnapshot()) {
                        long localCheckpoint = Long.parseLong(
                            store.readLastCommittedSegmentsInfo().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)
                        );
                        assert localCheckpoint == globalCheckpoint : localCheckpoint + " != " + globalCheckpoint;
                    }
                }
                if (isReadOnlyVerified(indexShard.indexSettings().getIndexMetadata())) {
                    Translog.deleteAll(translogLocation);
                }
                return;
            }
            final String translogUUID = Translog.createEmptyTranslog(
                indexShard.shardPath().resolveTranslog(),
                globalCheckpoint,
                indexShard.shardId(),
                indexShard.getPendingPrimaryTerm()
            );
            store.associateIndexWithNewTranslog(translogUUID);
        } finally {
            store.decRef();
        }
    }
}
