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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
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
    public void handoffPrimaryContext(ReplicationTracker.PrimaryContext primaryContext) {
        target.handoffPrimaryContext(primaryContext);
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
    public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetaData,
                           ActionListener<Void> listener) {
        executor.execute(() -> target.cleanFiles(totalTranslogOps, globalCheckpoint, sourceMetaData, listener));
    }

    @Override
    public void writeFileChunk(StoreFileMetaData fileMetaData, long position, BytesReference content,
                               boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener) {
        final BytesReference copy = new BytesArray(BytesRef.deepCopyOf(content.toBytesRef()));
        executor.execute(() -> target.writeFileChunk(fileMetaData, position, copy, lastChunk, totalTranslogOps, listener));
    }
}
