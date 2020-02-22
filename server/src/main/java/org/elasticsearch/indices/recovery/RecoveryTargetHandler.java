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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;

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
     */
    void handoffPrimaryContext(ReplicationTracker.PrimaryContext primaryContext);

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
            ActionListener<Long> listener);

    /**
     * Notifies the target of the files it is going to receive
     */
    void receiveFileInfo(List<String> phase1FileNames,
                         List<Long> phase1FileSizes,
                         List<String> phase1ExistingFileNames,
                         List<Long> phase1ExistingFileSizes,
                         int totalTranslogOps,
                         ActionListener<Void> listener);

    /**
     * After all source files has been sent over, this command is sent to the target so it can clean any local
     * files that are not part of the source store
     *
     * @param totalTranslogOps an update number of translog operations that will be replayed later on
     * @param globalCheckpoint the global checkpoint on the primary
     * @param sourceMetaData   meta data of the source store
     */
    void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetaData, ActionListener<Void> listener);

    /** writes a partial file chunk to the target store */
    void writeFileChunk(StoreFileMetaData fileMetaData, long position, BytesReference content,
                        boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener);

}
