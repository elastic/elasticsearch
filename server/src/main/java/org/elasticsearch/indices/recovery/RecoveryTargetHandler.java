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
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.repositories.IndexId;

import java.util.List;

public interface RecoveryTargetHandler {

    /**
     * Prepares the target to receive translog operations, after all file have been copied
     *
     * @param totalTranslogOps  total translog operations expected to be sent
     */
    void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener);

    /**
     * The finalize request refreshes the engine now that new segments are available, enables garbage collection of tombstone files, updates
     * the global checkpoint.
     *
     * @param globalCheckpoint the global checkpoint on the recovery source
     * @param trimAboveSeqNo   The recovery target should erase its existing translog above this sequence number
     *                         from the previous primary terms.
     * @param listener         the listener which will be notified when this method is completed
     */
    void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener);

    /**
     * Handoff the primary context between the relocation source and the relocation target.
     *
     * @param primaryContext the primary context from the relocation source
     * @param listener         the listener which will be notified when this method is completed
     */
    void handoffPrimaryContext(ReplicationTracker.PrimaryContext primaryContext, ActionListener<Void> listener);

    /**
     * Index a set of translog operations on the target
     *
     * @param operations                          operations to index
     * @param totalTranslogOps                    current number of total operations expected to be indexed
     * @param maxSeenAutoIdTimestampOnPrimary     the maximum auto_id_timestamp of all append-only requests processed by the primary shard
     * @param maxSeqNoOfUpdatesOrDeletesOnPrimary the max seq_no of update operations (index operations overwrite Lucene) or delete ops on
     *                                            the primary shard when capturing these operations. This value is at least as high as the
     *                                            max_seq_no_of_updates on the primary was when any of these ops were processed on it.
     * @param retentionLeases                     the retention leases on the primary
     * @param mappingVersionOnPrimary             the mapping version which is at least as up to date as the mapping version that the
     *                                            primary used to index translog {@code operations} in this request.
     *                                            If the mapping version on the replica is not older this version, we should not retry on
     *                                            {@link org.elasticsearch.index.mapper.MapperException}; otherwise we should wait for a
     *                                            new mapping then retry.
     * @param listener                            a listener which will be notified with the local checkpoint on the target
     *                                            after these operations are successfully indexed on the target.
     */
    void indexTranslogOperations(
        List<Translog.Operation> operations,
        int totalTranslogOps,
        long maxSeenAutoIdTimestampOnPrimary,
        long maxSeqNoOfUpdatesOrDeletesOnPrimary,
        RetentionLeases retentionLeases,
        long mappingVersionOnPrimary,
        ActionListener<Long> listener
    );

    /**
     * Notifies the target of the files it is going to receive
     */
    void receiveFileInfo(
        List<String> phase1FileNames,
        List<Long> phase1FileSizes,
        List<String> phase1ExistingFileNames,
        List<Long> phase1ExistingFileSizes,
        int totalTranslogOps,
        ActionListener<Void> listener
    );

    /**
     * After all source files has been sent over, this command is sent to the target so it can clean any local
     * files that are not part of the source store
     *
     * @param totalTranslogOps an update number of translog operations that will be replayed later on
     * @param globalCheckpoint the global checkpoint on the primary
     * @param sourceMetadata   meta data of the source store
     */
    void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetadata, ActionListener<Void> listener);

    /**
     * Restores a snapshot file in the target store
     * @param repository the repository to fetch the snapshot file
     * @param indexId the repository index id that identifies the shard index
     * @param snapshotFile the actual snapshot file to download
     */
    void restoreFileFromSnapshot(
        String repository,
        IndexId indexId,
        BlobStoreIndexShardSnapshot.FileInfo snapshotFile,
        ActionListener<Void> listener
    );

    /** writes a partial file chunk to the target store */
    void writeFileChunk(
        StoreFileMetadata fileMetadata,
        long position,
        ReleasableBytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    );

    default void cancel() {}
}
