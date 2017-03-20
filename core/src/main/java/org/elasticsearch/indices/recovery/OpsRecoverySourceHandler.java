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
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * RecoverySourceHandler handles the three phases of shard recovery, which is
 * everything relating to copying the segment files as well as sending translog
 * operations across the wire once the segments have been copied.
 *
 * Note: There is always one source handler per recovery that handles all the
 * file and translog transfer. This handler is completely isolated from other recoveries
 * while the {@link RateLimiter} passed via {@link RecoverySettings} is shared across recoveries
 * originating from this nodes to throttle the number bytes send during file transfer.
 * The transaction log phase bypasses the rate limiter entirely.
 */
public class OpsRecoverySourceHandler extends RecoverySourceHandler {

    // Request containing source and target node information
    private final StartOpsRecoveryRequest request;
    private final OpsRecoveryTargetHandler recoveryTarget;
    private final int chunkSizeInBytes;


    public OpsRecoverySourceHandler(final IndexShard shard, OpsRecoveryTargetHandler recoveryTarget,
                                    final StartOpsRecoveryRequest request,
                                    final int fileChunkSizeInBytes,
                                    final Settings nodeSettings) {
        super(shard, nodeSettings, request.targetNode());
        this.recoveryTarget = recoveryTarget;
        this.request = request;
        this.chunkSizeInBytes = fileChunkSizeInBytes;
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public RecoveryResponse recoverToTarget() throws IOException {
        try (Translog.View translogView = shard.acquireTranslogView()) {
            logger.trace("captured translog id [{}] for recovery",
                translogView.minTranslogGeneration());

            checkTranslogReadyForSequenceNumberBasedRecovery(translogView);

            if (shard.state() == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(request.shardId());
            }

            logger.trace("performing sequence numbers based recovery. starting at [{}]",
                request.getStartingSeqNo());

            logger.trace("snapshot translog for recovery; current size is [{}]",
                translogView.totalOperations());
            try {
                sendSnapshot(request.getStartingSeqNo(), translogView.snapshot(),
                    cancellableThreads, recoveryTarget, response, chunkSizeInBytes, logger);
            } catch (Exception e) {
                throw new RecoveryFailedException(request, "failed to replicate ops", e);
            }

            finalizeRecovery();
        }
        return response;
    }

    /**
     * Determines if the source translog is ready for a sequence-number-based peer recovery.
     * The main condition here is that the source translog contains all operations between the
     * local checkpoint on the target and the current maximum sequence number on the source.
     *
     * @param translogView a view of the translog on the source
     * @return {@code true} if the source is ready for a sequence-number-based recovery
     * @throws IOException if an I/O exception occurred reading the translog snapshot
     */
    void checkTranslogReadyForSequenceNumberBasedRecovery(final Translog.View translogView)
        throws IOException {
        final long startingSeqNo = request.getStartingSeqNo();
        assert startingSeqNo >= 0;
        final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();
        logger.trace("testing sequence numbers in range: [{}, {}]", startingSeqNo, endingSeqNo);
        final String failure;
        // the start recovery request is initialized with the starting sequence number set to the
        // target shard's local checkpoint plus one
        if (startingSeqNo - 1 <= endingSeqNo) {
            /*
             * We need to wait for all operations up to the current max to complete, otherwise we
             * can not guarantee that all operations in the required range will be available for
             * replaying from the translog of the source.
             */
            cancellableThreads.execute(() -> shard.waitForOpsToComplete(endingSeqNo));

            logger.trace("all operations up to [{}] completed, checking translog content",
                endingSeqNo);

            final LocalCheckpointTracker tracker =
                new LocalCheckpointTracker(shard.indexSettings(), startingSeqNo, startingSeqNo - 1);
            final Translog.Snapshot snapshot = translogView.snapshot();
            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                if (operation.seqNo() != SequenceNumbersService.UNASSIGNED_SEQ_NO) {
                    tracker.markSeqNoAsCompleted(operation.seqNo());
                }
            }
            failure = tracker.getCheckpoint() >= endingSeqNo ? null :
                "translog doesn't contain op [" + tracker.getCheckpoint() + 1 +
                    "] which is required";
        } else {
            // norelease this can currently happen if a snapshot restore rolls the primary back to a
            // previous commit point; in this situation the local checkpoint on the replica can
            // be far in advance of the maximum sequence number on the primary violating all
            // assumptions regarding local and global checkpoints
            failure = "starting sequence is higher than max seq no seen by shard [" + endingSeqNo
                + "]";
        }
        throw new RecoveryFailedException(request, failure, null);
    }

    /**
     * Send the given snapshot's operations with a sequence number greater than the specified
     * staring sequence number to this handler's target node.
     * <p>
     * Operations are bulked into a single request depending on an operation count limit or
     * size-in-bytes limit.
     *
     * @param startingSeqNo the sequence number to start recovery from,
     *                      or {@link SequenceNumbersService#UNASSIGNED_SEQ_NO} if all
     *                      ops should be sent
     * @param snapshot      a snapshot of the translog
     */
    protected static void sendSnapshot(final long startingSeqNo, final Translog.Snapshot snapshot,
                                       final CancellableThreads cancellableThreads,
                                       final OpsRecoveryTargetHandler recoveryTarget,
                                       final RecoveryResponse response,
                                       final int chunkSizeInBytes, final Logger logger)
        throws IOException {
        cancellableThreads.checkForCancel();

        final StopWatch stopWatch = new StopWatch().start();

        logger.trace("recovery: sending transaction log operations");

        // send all the snapshot's translog operations to the target
        int ops = 0;
        long size = 0;
        int totalOperations = 0;
        final List<Translog.Operation> operations = new ArrayList<>();

        if (snapshot.totalOperations() == 0) {
            logger.trace("no translog operations to send");
        }

        // send operations in batches
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            cancellableThreads.checkForCancel();
            // if we are doing a sequence-number-based recovery, we have to skip older ops for
            // which no sequence number was assigned, and  any ops before the starting sequence
            // number
            final long seqNo = operation.seqNo();
            if (startingSeqNo >= 0 &&
                (seqNo == SequenceNumbersService.UNASSIGNED_SEQ_NO || seqNo < startingSeqNo)) {
                continue;
            }
            operations.add(operation);
            ops++;
            size += operation.estimateSize();
            totalOperations++;

            // check if this request is past bytes threshold, and if so, send it off
            if (size >= chunkSizeInBytes) {
                cancellableThreads.execute(() ->
                    recoveryTarget.indexTranslogOperations(operations, snapshot.totalOperations()));
                if (logger.isTraceEnabled()) {
                    logger.trace("sent batch of [{}][{}] (total: [{}]) translog operations", ops,
                        new ByteSizeValue(size), snapshot.totalOperations());
                }
                ops = 0;
                size = 0;
                operations.clear();
            }
        }

        // send the leftover operations
        if (!operations.isEmpty()) {
            cancellableThreads.execute(() ->
                recoveryTarget.indexTranslogOperations(operations, snapshot.totalOperations()));
        }

        if (logger.isTraceEnabled()) {
            logger.trace("sent final batch of [{}][{}] (total: [{}]) translog operations", ops,
                new ByteSizeValue(size), snapshot.totalOperations());
        }

        stopWatch.stop();
        String traceSummary = "recovery: replayed [" + totalOperations + "] ops" +
            " in [" + stopWatch.totalTime() + "]";
        logger.trace(traceSummary);
        response.apprendTraceSummary(traceSummary);
    }

    /*
     * finalizes the recovery process
     */
    public void finalizeRecovery() {
        if (shard.state() == IndexShardState.CLOSED) {
            throw new IndexShardClosedException(request.shardId());
        }
        cancellableThreads.checkForCancel();
        StopWatch stopWatch = new StopWatch().start();
        logger.trace("finalizing recovery");
        cancellableThreads.execute(() -> {
            shard.markAllocationIdAsInSync(recoveryTarget.getTargetAllocationId());
            recoveryTarget.finalizeRecovery(shard.getGlobalCheckpoint());
        });

        stopWatch.stop();
        logger.trace("finalizing recovery took [{}]", stopWatch.totalTime());
    }

    @Override
    public String toString() {
        return "OpRecoveryHandler{" +
            "shardId=" + request.shardId() +
            ", sourceNode=" + request.sourceNode() +
            ", targetNode=" + request.targetNode() +
            '}';
    }

}
