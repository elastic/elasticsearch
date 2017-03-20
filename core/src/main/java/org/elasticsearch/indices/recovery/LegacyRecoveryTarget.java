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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.List;

public class LegacyRecoveryTarget extends RecoveryTarget implements FileRecoveryTargetHandler,
    OpsRecoveryTargetHandler, PrimaryHandoffRecoveryTargetHandler {

    private final FileRecoveryTarget fileRecoveryTarget;
    private final PrimaryHandoffRecoveryTarget primaryHandoffRecoveryTarget;

    /**
     * Creates a new recovery target object that represents a recovery to the provided shard.
     *
     * @param indexShard                        local shard where we want to recover to
     * @param sourceNode                        source node of the recovery where we recover from
     * @param listener                          called when recovery is completed/failed
     * @param ensureClusterStateVersionCallback callback to ensure that the current node is at least
     *                                          on a cluster state with the provided version;
     *                                          necessary for primary relocation so that new primary
     *                                          knows about all other ongoing replica recoveries
     *                                          when replicating documents
     *                                          (see {@link FileRecoverySourceHandler})
     */
    public LegacyRecoveryTarget(IndexShard indexShard, DiscoveryNode sourceNode,
                                PeerRecoveryTargetService.RecoveryListener listener,
                                Callback<Long> ensureClusterStateVersionCallback) {
        super(indexShard, sourceNode, listener);
        PeerRecoveryTargetService.RecoveryListener noopListener =
            new PeerRecoveryTargetService.RecoveryListener() {
                @Override
                public void onRecoveryDone(RecoveryState state) {

                }

                @Override
                public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e,
                                              boolean sendShardFailure) {

                }
            };
        fileRecoveryTarget = new FileRecoveryTarget(indexShard, sourceNode, noopListener);
        primaryHandoffRecoveryTarget = new PrimaryHandoffRecoveryTarget(indexShard, sourceNode,
            listener, ensureClusterStateVersionCallback);
    }

    private LegacyRecoveryTarget(FileRecoveryTarget fileRecoveryTarget,
                                 PrimaryHandoffRecoveryTarget primaryHandoffRecoveryTarget,
                                 PeerRecoveryTargetService.RecoveryListener listener) {
        super(fileRecoveryTarget.indexShard(), fileRecoveryTarget.sourceNode(), listener);
        this.fileRecoveryTarget = fileRecoveryTarget;
        this.primaryHandoffRecoveryTarget = primaryHandoffRecoveryTarget;
    }

    @Override
    public LegacyRecoveryTarget retryCopy() {
        return new LegacyRecoveryTarget(
            fileRecoveryTarget.retryCopy(),
            primaryHandoffRecoveryTarget.retryCopy(), listener());
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes,
                                List<String> phase1ExistingFileNames,
                                List<Long> phase1ExistingFileSizes, int totalTranslogOps) {
        fileRecoveryTarget.receiveFileInfo(phase1FileNames, phase1FileSizes,
            phase1ExistingFileNames, phase1ExistingFileSizes, totalTranslogOps);
    }

    @Override
    public void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData)
        throws IOException {
        fileRecoveryTarget.cleanFiles(totalTranslogOps, sourceMetaData);
    }

    @Override
    public void writeFileChunk(StoreFileMetaData fileMetaData, long position,
                               BytesReference content, boolean lastChunk, int totalTranslogOps)
        throws IOException {
        fileRecoveryTarget.writeFileChunk(fileMetaData, position, content, lastChunk,
            totalTranslogOps);
    }

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, long maxUnsafeAutoIdTimestamp)
        throws IOException {
        indexShard().skipTranslogRecovery(maxUnsafeAutoIdTimestamp);
        fileRecoveryTarget.prepareForTranslogOperations(totalTranslogOps, maxUnsafeAutoIdTimestamp);
    }

    @Override
    public void finalizeRecovery(long globalCheckpoint) {
        fileRecoveryTarget.finalizeRecovery(globalCheckpoint);
    }

    @Override
    public void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps) {
        fileRecoveryTarget.indexTranslogOperations(operations, totalTranslogOps);
    }

    @Override
    public String getTargetAllocationId() {
        return fileRecoveryTarget.getTargetAllocationId();
    }

    @Override
    public void ensureClusterStateVersion(long clusterStateVersion) {
        primaryHandoffRecoveryTarget.ensureClusterStateVersion(clusterStateVersion);
    }

    @Override
    protected void onResetRecovery() throws IOException {
        fileRecoveryTarget.onResetRecovery();
        primaryHandoffRecoveryTarget.onResetRecovery();
    }

    @Override
    protected boolean assertOnDone() {
        assert fileRecoveryTarget.assertOnDone();
        assert primaryHandoffRecoveryTarget.assertOnDone();
        return true;
    }

    @Override
    protected void doClose() {
        fileRecoveryTarget.doClose();
        primaryHandoffRecoveryTarget.doClose();
    }

    @Override
    public String startRecoveryActionName() {
        return PeerRecoverySourceService.Actions.START_LEGACY_RECOVERY;
    }

    @Override
    public StartRecoveryRequest createStartRecoveryRequest(Logger logger, DiscoveryNode localNode) {
        logger.trace("{} collecting local files for [{}]", shardId(), sourceNode());

        final Store.MetadataSnapshot metadataSnapshot = getStoreMetadataSnapshot(logger);
        logger.trace("{} local file count [{}]", shardId(), metadataSnapshot.size());

        return new StartLegacyRecoveryRequest(shardId(), sourceNode(), localNode, metadataSnapshot,
            state().getPrimary(), recoveryId());
    }

    /**
     * Obtains a snapshot of the store metadata for the recovery target.
     */
    private Store.MetadataSnapshot getStoreMetadataSnapshot(Logger logger) {
        try {
            if (indexShard().indexSettings().isOnSharedFilesystem()) {
                // we are not going to copy any files, so don't bother listing files, potentially
                // running into concurrency issues with the primary changing files underneath us
                return Store.MetadataSnapshot.EMPTY;
            } else {
                return indexShard().snapshotStoreMetadata();
            }
        } catch (final org.apache.lucene.index.IndexNotFoundException e) {
            // happens on an empty folder. no need to log
            logger.trace("{} shard folder empty, recovering all files", this);
            return Store.MetadataSnapshot.EMPTY;
        } catch (final IOException e) {
            logger.warn("error while listing local files, recovering as if there are none", e);
            return Store.MetadataSnapshot.EMPTY;
        }
    }


    @Override
    public RecoveryResponse createRecoveryResponse() {
        return new LegacyRecoveryResponse();
    }
}
