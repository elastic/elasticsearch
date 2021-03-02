/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Wraps a {@link RecoveryTarget} to make all remote calls to be executed asynchronously using the provided {@code executor}.
 */
public class AsyncRecoveryTarget implements RecoveryTargetHandler {
    private final RecoveryTargetHandler target;
    private final Executor executor;

    public AsyncRecoveryTarget(RecoveryTargetHandler target, Executor executor) {
        this.executor = executor;
        this.target = target;
    }

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        executor.execute(() -> target.prepareForTranslogOperations(totalTranslogOps, listener));
    }

    @Override
    public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
        executor.execute(() -> target.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, listener));
    }

    @Override
    public void handoffPrimaryContext(ReplicationTracker.PrimaryContext primaryContext, ActionListener<Void> listener) {
        executor.execute(() -> target.handoffPrimaryContext(primaryContext, listener));
    }

    @Override
    public void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps,
                                        long maxSeenAutoIdTimestampOnPrimary, long maxSeqNoOfDeletesOrUpdatesOnPrimary,
                                        RetentionLeases retentionLeases, long mappingVersionOnPrimary, ActionListener<Long> listener) {
        executor.execute(() -> target.indexTranslogOperations(operations, totalTranslogOps, maxSeenAutoIdTimestampOnPrimary,
            maxSeqNoOfDeletesOrUpdatesOnPrimary, retentionLeases, mappingVersionOnPrimary, listener));
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames,
                                List<Long> phase1ExistingFileSizes, int totalTranslogOps, ActionListener<Void> listener) {
        executor.execute(() -> target.receiveFileInfo(
            phase1FileNames, phase1FileSizes, phase1ExistingFileNames, phase1ExistingFileSizes, totalTranslogOps, listener));
    }

    @Override
    public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetadata,
                           ActionListener<Void> listener) {
        executor.execute(() -> target.cleanFiles(totalTranslogOps, globalCheckpoint, sourceMetadata, listener));
    }

    @Override
    public void writeFileChunk(StoreFileMetadata fileMetadata, long position, ReleasableBytesReference content,
                               boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener) {
        final ReleasableBytesReference retained = content.retain();
        final ActionListener<Void> wrappedListener = ActionListener.runBefore(listener, retained::close);
        boolean success = false;
        try {
            executor.execute(() -> target.writeFileChunk(fileMetadata, position, retained, lastChunk, totalTranslogOps, wrappedListener));
            success = true;
        } finally {
            if (success == false) {
                content.decRef();
            }
        }
    }
}
