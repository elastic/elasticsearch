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

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotRecoveringException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
    private final MultiFileWriter multiFileWriter;
    private final Store store;
    private final PeerRecoveryTargetService.RecoveryListener listener;

    private final AtomicBoolean finished = new AtomicBoolean();

    private final CancellableThreads cancellableThreads;

    // last time this status was accessed
    private volatile long lastAccessTime = System.nanoTime();

    // latch that can be used to blockingly wait for RecoveryTarget to be closed
    private final CountDownLatch closedLatch = new CountDownLatch(1);

    /**
     * Creates a new recovery target object that represents a recovery to the provided shard.
     *
     * @param indexShard                        local shard where we want to recover to
     * @param sourceNode                        source node of the recovery where we recover from
     * @param listener                          called when recovery is completed/failed
     */
    public RecoveryTarget(IndexShard indexShard, DiscoveryNode sourceNode, PeerRecoveryTargetService.RecoveryListener listener) {
        super("recovery_status");
        this.cancellableThreads = new CancellableThreads();
        this.recoveryId = idGenerator.incrementAndGet();
        this.listener = listener;
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.sourceNode = sourceNode;
        this.shardId = indexShard.shardId();
        final String tempFilePrefix = RECOVERY_PREFIX + UUIDs.randomBase64UUID() + ".";
        this.multiFileWriter = new MultiFileWriter(indexShard.store(), indexShard.recoveryState().getIndex(), tempFilePrefix, logger,
            this::ensureRefCount);
        this.store = indexShard.store();
        // make sure the store is not released until we are done.
        store.incRef();
        indexShard.recoveryStats().incCurrentAsTarget();
    }

    /**
     * Returns a fresh recovery target to retry recovery from the same source node onto the same shard and using the same listener.
     *
     * @return a copy of this recovery target
     */
    public RecoveryTarget retryCopy() {
        return new RecoveryTarget(indexShard, sourceNode, listener);
    }

    public long recoveryId() {
        return recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public IndexShard indexShard() {
        ensureRefCount();
        return indexShard;
    }

    public DiscoveryNode sourceNode() {
        return this.sourceNode;
    }

    public RecoveryState state() {
        return indexShard.recoveryState();
    }

    public CancellableThreads cancellableThreads() {
        return cancellableThreads;
    }

    /** return the last time this RecoveryStatus was used (based on System.nanoTime() */
    public long lastAccessTime() {
        return lastAccessTime;
    }

    /** sets the lasAccessTime flag to now */
    public void setLastAccessTime() {
        lastAccessTime = System.nanoTime();
    }

    public Store store() {
        ensureRefCount();
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
                logger.trace("new recovery target cancelled for shard {} while waiting on old recovery target with id [{}] to close",
                    shardId, recoveryId);
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
        listener.onRecoveryFailure(state(), e, sendShardFailure);
    }

    /** mark the current recovery as done */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            assert multiFileWriter.tempFileNames.isEmpty() : "not all temporary files are renamed";
            try {
                // this might still throw an exception ie. if the shard is CLOSED due to some other event.
                // it's safer to decrement the reference in a try finally here.
                indexShard.postRecovery("peer recovery done");
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
            listener.onRecoveryDone(state());
        }
    }

    @Override
    protected void closeInternal() {
        try {
            multiFileWriter.close();
        } finally {
            // free store. increment happens in constructor
            store.decRef();
            indexShard.recoveryStats().decCurrentAsTarget();
            closedLatch.countDown();
        }
    }

    @Override
    public String toString() {
        return shardId + " [" + recoveryId + "]";
    }

    private void ensureRefCount() {
        if (refCount() <= 0) {
            throw new ElasticsearchException("RecoveryStatus is used but it's refcount is 0. Probably a mismatch between incRef/decRef " +
                    "calls");
        }
    }

    /*** Implementation of {@link RecoveryTargetHandler } */

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
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
        return indexShard.estimateNumberOfHistoryOperations("peer-recovery", localCheckpointOfCommit + 1) > 0;
    }

    @Override
    public void handoffPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        indexShard.activateWithPrimaryContext(primaryContext);
    }

    @Override
    public void indexTranslogOperations(
            final List<Translog.Operation> operations,
            final int totalTranslogOps,
            final long maxSeenAutoIdTimestampOnPrimary,
            final long maxSeqNoOfDeletesOrUpdatesOnPrimary,
            final RetentionLeases retentionLeases,
            final long mappingVersionOnPrimary,
            final ActionListener<Long> listener) {
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
    public void receiveFileInfo(List<String> phase1FileNames,
                                List<Long> phase1FileSizes,
                                List<String> phase1ExistingFileNames,
                                List<Long> phase1ExistingFileSizes,
                                int totalTranslogOps,
                                ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            indexShard.resetRecoveryStage();
            indexShard.prepareForIndexRecovery();
            final RecoveryState.Index index = state().getIndex();
            for (int i = 0; i < phase1ExistingFileNames.size(); i++) {
                index.addFileDetail(phase1ExistingFileNames.get(i), phase1ExistingFileSizes.get(i), true);
            }
            for (int i = 0; i < phase1FileNames.size(); i++) {
                index.addFileDetail(phase1FileNames.get(i), phase1FileSizes.get(i), false);
            }
            state().getTranslog().totalOperations(totalTranslogOps);
            state().getTranslog().totalOperationsOnStart(totalTranslogOps);
            return null;
        });
    }

    @Override
    public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetaData,
                           ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            state().getTranslog().totalOperations(totalTranslogOps);
            // first, we go and move files that were created with the recovery id suffix to
            // the actual names, its ok if we have a corrupted index here, since we have replicas
            // to recover from in case of a full cluster shutdown just when this code executes...
            multiFileWriter.renameAllTempFiles();
            final Store store = store();
            store.incRef();
            try {
                store.cleanupAndVerify("recovery CleanFilesRequestHandler", sourceMetaData);
                final String translogUUID = Translog.createEmptyTranslog(
                    indexShard.shardPath().resolveTranslog(), globalCheckpoint, shardId, indexShard.getPendingPrimaryTerm());
                store.associateIndexWithNewTranslog(translogUUID);

                if (indexShard.getRetentionLeases().leases().isEmpty()) {
                    // if empty, may be a fresh IndexShard, so write an empty leases file to disk
                    indexShard.persistRetentionLeases();
                    assert indexShard.loadRetentionLeases().leases().isEmpty();
                } else {
                    assert indexShard.assertRetentionLeasesPersisted();
                }
                indexShard.maybeCheckIndex();
                state().setStage(RecoveryState.Stage.TRANSLOG);
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
    public void writeFileChunk(StoreFileMetaData fileMetaData, long position, BytesReference content,
                               boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener) {
        try {
            state().getTranslog().totalOperations(totalTranslogOps);
            multiFileWriter.writeFileChunk(fileMetaData, position, content, lastChunk);
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /** Get a temporary name for the provided file name. */
    public String getTempNameForFile(String origFile) {
        return multiFileWriter.getTempNameForFile(origFile);
    }

    Path translogLocation() {
        return indexShard().shardPath().resolveTranslog();
    }
}
