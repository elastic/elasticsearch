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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.RemoteTransportException;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

/**
 * RecoverySourceHandler handles the three phases of shard recovery, which is
 * everything relating to copying the segment files as well as sending translog
 * operations across the wire once the segments have been copied.
 *
 * Note: There is always one source handler per recovery that handles all the
 * file and translog transfer. This handler is completely isolated from other recoveries
 * while the {@link RateLimiter} passed via {@link RecoverySettings} is shared across recoveries
 * originating from this nodes to throttle the number bytes send during file transfer. The transaction log
 * phase bypasses the rate limiter entirely.
 */
public class FileRecoverySourceHandler extends RecoverySourceHandler {

    // Request containing source and target node information
    private final StartFileRecoveryRequest request;
    private final Supplier<Long> currentClusterStateVersionSupplier;
    private final Function<String, Releasable> delayNewRecoveries;
    private final int chunkSizeInBytes;
    private final FileRecoveryTargetHandler recoveryTarget;

    protected final RecoveryResponse response;

    public FileRecoverySourceHandler(final IndexShard shard,
                                     final FileRecoveryTargetHandler recoveryTarget,
                                     final StartFileRecoveryRequest request,
                                     final Supplier<Long> currentClusterStateVersionSupplier,
                                     Function<String, Releasable> delayNewRecoveries,
                                     final int fileChunkSizeInBytes,
                                     final Settings nodeSettings) {
        super(shard, nodeSettings, request.targetNode());
        this.recoveryTarget = recoveryTarget;
        this.request = request;
        this.currentClusterStateVersionSupplier = currentClusterStateVersionSupplier;
        this.delayNewRecoveries = delayNewRecoveries;
        this.chunkSizeInBytes = fileChunkSizeInBytes;
        this.response = new RecoveryResponse();
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public RecoveryResponse recoverToTarget() throws IOException {
        try (Translog.View translogView = shard.acquireTranslogView()) {
            logger.trace("captured translog id [{}] for recovery", translogView.minTranslogGeneration());

            final IndexCommit phase1Snapshot;
            try {
                phase1Snapshot = shard.acquireIndexCommit(false);
            } catch (final Exception e) {
                IOUtils.closeWhileHandlingException(translogView);
                throw new RecoveryEngineException(shard.shardId(), 1, "snapshot failed", e);
            }
            try {
                phase1(phase1Snapshot, translogView);
            } catch (final Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 1, "phase1 failed", e);
            } finally {
                try {
                    shard.releaseIndexCommit(phase1Snapshot);
                } catch (final IOException ex) {
                    logger.warn("releasing snapshot caused exception", ex);
                }
            }

            try {
                prepareTargetForTranslog(translogView.totalOperations(),
                    shard.segmentStats(false).getMaxUnsafeAutoIdTimestamp());
            } catch (final Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 1,
                    "prepare target for translog failed", e);
            }

            // engine was just started at the end of phase1
            if (shard.state() == IndexShardState.RELOCATED) {
                // The primary shard has been relocated while we copied files. This means that we
                // can't guarantee any more that all operations that were replicated during the file
                // copy (when the target engine was not yet opened) will be present in the local
                // translog and thus will be resent on phase2. The reason is that an operation
                // replicated by the target primary is sent to the recovery target and the local
                // shard (old primary) concurrently, meaning it may have arrived at the recovery
                // target before we opened the engine and is still in-flight on the local shard.
                //
                // Checking the relocated status here, after we opened the engine on the target,
                // is safe because primary relocation waits for all ongoing operations to
                // complete and be fully replicated. Therefore all future operation by the new
                // primary are guaranteed to reach the target shard when its engine is open.
                throw new IndexShardRelocatedException(request.shardId());
            }

            logger.trace("snapshot translog for recovery; current size is [{}]",
                translogView.totalOperations());
            try {
                OpsRecoverySourceHandler.sendSnapshot(SequenceNumbersService.UNASSIGNED_SEQ_NO,
                    translogView.snapshot(), cancellableThreads, recoveryTarget, response,
                    chunkSizeInBytes, logger);
            } catch (Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 2, "phase2 failed", e);
            }

            finalizeRecovery();
        }
        return response;
    }

    public void phase1(final IndexCommit snapshot, final Translog.View translogView) {
        cancellableThreads.checkForCancel();
        // Total size of segment files that are recovered
        long totalSize = 0;
        // Total size of segment files that were able to be re-used
        long existingTotalSize = 0;
        final List<String> phase1FileNames = new ArrayList<>();
        final List<Long> phase1FileSizes = new ArrayList<>();
        final List<String> phase1ExistingFileNames = new ArrayList<>();
        final List<Long> phase1ExistingFileSizes = new ArrayList<>();
        final Store store = shard.store();
        final StopWatch stopWatch = new StopWatch().start();
        store.incRef();
        try {
            final Store.MetadataSnapshot recoverySourceMetadata;
            try {
                recoverySourceMetadata = store.getMetadata(snapshot);
            } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                shard.failShard("recovery", ex);
                throw ex;
            }
            for (String name : snapshot.getFileNames()) {
                final StoreFileMetaData md = recoverySourceMetadata.get(name);
                if (md == null) {
                    logger.info("Snapshot differs from actual index for file: {} meta: {}", name, recoverySourceMetadata.asMap());
                    throw new CorruptIndexException("Snapshot differs from actual index - maybe index was removed metadata has " +
                            recoverySourceMetadata.asMap().size() + " files", name);
                }
            }
            // Generate a "diff" of all the identical, different, and missing
            // segment files on the target node, using the existing files on
            // the source node
            String recoverySourceSyncId = recoverySourceMetadata.getSyncId();
            String recoveryTargetSyncId = request.metadataSnapshot().getSyncId();
            final boolean recoverWithSyncId = recoverySourceSyncId != null &&
                    recoverySourceSyncId.equals(recoveryTargetSyncId);
            if (recoverWithSyncId) {
                final long numDocsTarget = request.metadataSnapshot().getNumDocs();
                final long numDocsSource = recoverySourceMetadata.getNumDocs();
                if (numDocsTarget != numDocsSource) {
                    throw new IllegalStateException("try to recover " + request.shardId() + " from primary shard with sync id but number " +
                            "of docs differ: " + numDocsSource + " (" + request.sourceNode().getName() + ", primary) vs " + numDocsTarget
                            + "(" + request.targetNode().getName() + ")");
                }
                // we shortcut recovery here because we have nothing to copy. but we must still start the engine on the target.
                // so we don't return here
                logger.trace("skipping [phase1]- identical sync id [{}] found on both source and target", recoverySourceSyncId);
            } else {
                final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(request.metadataSnapshot());
                for (StoreFileMetaData md : diff.identical) {
                    phase1ExistingFileNames.add(md.name());
                    phase1ExistingFileSizes.add(md.length());
                    existingTotalSize += md.length();
                    if (logger.isTraceEnabled()) {
                        logger.trace("recovery [phase1]: not recovering [{}], exist in local store and has checksum [{}]," +
                                        " size [{}]", md.name(), md.checksum(), md.length());
                    }
                    totalSize += md.length();
                }
                List<StoreFileMetaData> phase1Files = new ArrayList<>(diff.different.size() + diff.missing.size());
                phase1Files.addAll(diff.different);
                phase1Files.addAll(diff.missing);
                for (StoreFileMetaData md : phase1Files) {
                    if (request.metadataSnapshot().asMap().containsKey(md.name())) {
                        logger.trace("recovery [phase1]: recovering [{}], exists in local store, but is different: remote [{}], local [{}]",
                            md.name(), request.metadataSnapshot().asMap().get(md.name()), md);
                    } else {
                        logger.trace("recovery [phase1]: recovering [{}], does not exist in remote", md.name());
                    }
                    phase1FileNames.add(md.name());
                    phase1FileSizes.add(md.length());
                    totalSize += md.length();
                }

                logger.trace("recovery [phase1]: recovering_files [{}] with total_size [{}], " +
                        "reusing_files [{}] with total_size [{}]",
                        phase1FileNames.size(), new ByteSizeValue(totalSize),
                        phase1ExistingFileNames.size(), new ByteSizeValue(existingTotalSize));
                cancellableThreads.execute(() ->
                        recoveryTarget.receiveFileInfo(phase1FileNames, phase1FileSizes,
                            phase1ExistingFileNames, phase1ExistingFileSizes,
                            translogView.totalOperations()));
                // How many bytes we've copied since we last called RateLimiter.pause
                final Function<StoreFileMetaData, OutputStream> outputStreamFactories =
                        md -> new BufferedOutputStream(new RecoveryOutputStream(md, translogView), chunkSizeInBytes);
                sendFiles(store, phase1Files.toArray(new StoreFileMetaData[phase1Files.size()]), outputStreamFactories);
                // Send the CLEAN_FILES request, which takes all of the files that
                // were transferred and renames them from their temporary file
                // names to the actual file names. It also writes checksums for
                // the files after they have been renamed.
                //
                // Once the files have been renamed, any other files that are not
                // related to this recovery (out of date segments, for example)
                // are deleted
                try {
                    cancellableThreads.executeIO(() -> recoveryTarget.cleanFiles(
                        translogView.totalOperations(), recoverySourceMetadata));
                } catch (RemoteTransportException | IOException targetException) {
                    final IOException corruptIndexException;
                    // we realized that after the index was copied and we wanted to finalize the recovery
                    // the index was corrupted:
                    //   - maybe due to a broken segments file on an empty index (transferred with no checksum)
                    //   - maybe due to old segments without checksums or length only checks
                    if ((corruptIndexException = ExceptionsHelper.unwrapCorruption(targetException)) != null) {
                        try {
                            final Store.MetadataSnapshot recoverySourceMetadata1 = store.getMetadata(snapshot);
                            StoreFileMetaData[] metadata =
                                    StreamSupport.stream(recoverySourceMetadata1.spliterator(),
                                        false).toArray(StoreFileMetaData[]::new);
                            ArrayUtil.timSort(metadata, // check small files first
                                Comparator.comparingLong(StoreFileMetaData::length));
                            for (StoreFileMetaData md : metadata) {
                                cancellableThreads.checkForCancel();
                                logger.debug("checking integrity for file {} after remove " +
                                    "corruption exception", md);
                                if (store.checkIntegrityNoException(md) == false) {
                                    // we are corrupted on the primary -- fail!
                                    shard.failShard("recovery", corruptIndexException);
                                    logger.warn("Corrupted file detected {} checksum mismatch", md);
                                    throw corruptIndexException;
                                }
                            }
                        } catch (IOException ex) {
                            targetException.addSuppressed(ex);
                            throw targetException;
                        }
                        // corruption has happened on the way to replica
                        RemoteTransportException exception =
                            new RemoteTransportException(
                                "File corruption occurred on recovery but checksums are ok", null);
                        exception.addSuppressed(targetException);
                        logger.warn(
                            (org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage(
                                "{} Remote file corruption during finalization of recovery on node {}. local checksum OK",
                                shard.shardId(),
                                request.targetNode()),
                            corruptIndexException);
                        throw exception;
                    } else {
                        throw targetException;
                    }
                }
            }

            logger.trace("recovery [phase1]: took [{}]", stopWatch.totalTime());
        } catch (Exception e) {
            throw new RecoverFilesRecoveryException(request.shardId(), phase1FileNames.size(),
                new ByteSizeValue(totalSize), e);
        } finally {
            store.decRef();
            StringBuilder sb = new StringBuilder();
            sb.append("   phase1: recovered_files [").append(phase1FileNames.size()).append("]")
                .append(" with total_size of [").append(new ByteSizeValue(totalSize))
                .append("]").append(", took [").append(stopWatch.totalTime())
                .append("\n");
            sb.append("         : reusing_files   [").append(phase1ExistingFileNames.size())
                .append("] with total_size of [").append(new ByteSizeValue(existingTotalSize))
                .append("]");
            response.apprendTraceSummary(sb.toString());
        }
    }

    void prepareTargetForTranslog(final int totalTranslogOps, final long maxUnsafeAutoIdTimestamp)
        throws IOException {
        StopWatch stopWatch = new StopWatch().start();
        logger.trace("recovery [phase1]: prepare remote engine for translog");
        // Send a request preparing the new shard's translog to receive operations.
        // This ensures the shard engine is started and disables
        // garbage collection (not the JVM's GC!) of tombstone deletes.
        cancellableThreads.executeIO(() ->
            recoveryTarget.prepareForTranslogOperations(totalTranslogOps,
                maxUnsafeAutoIdTimestamp));
        stopWatch.stop();

        final String message = "recovery [phase1]: remote engine start took [" +
            stopWatch.totalTime() + "]";
        logger.trace(message);
        response.apprendTraceSummary(message);
    }


    /*
     * finalizes the recovery process
     */
    public void finalizeRecovery() {
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
        return "FileRecoveryHandler{" +
                "shardId=" + request.shardId() +
                ", sourceNode=" + request.sourceNode() +
                ", targetNode=" + request.targetNode() +
                '}';
    }


    final class RecoveryOutputStream extends OutputStream {
        private final StoreFileMetaData md;
        private final Translog.View translogView;
        private long position = 0;

        RecoveryOutputStream(StoreFileMetaData md, Translog.View translogView) {
            this.md = md;
            this.translogView = translogView;
        }

        @Override
        public void write(int b) throws IOException {
            throw new UnsupportedOperationException("we can't send single bytes over the wire");
        }

        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
            sendNextChunk(position, new BytesArray(b, offset, length), md.length() == position + length);
            position += length;
            assert md.length() >= position : "length: " + md.length() + " but positions was: " + position;
        }

        private void sendNextChunk(long position, BytesArray content, boolean lastChunk) throws IOException {
            // Actually send the file chunk to the target node, waiting for it to complete
            cancellableThreads.executeIO(() ->
                    recoveryTarget.writeFileChunk(md, position, content, lastChunk, translogView.totalOperations())
            );
            if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                throw new IndexShardClosedException(request.shardId());
            }
        }
    }

    void sendFiles(Store store, StoreFileMetaData[] files,
                   Function<StoreFileMetaData, OutputStream> outputStreamFactory) throws Exception {
        store.incRef();
        try {
            // send smallest first
            ArrayUtil.timSort(files, Comparator.comparingLong(StoreFileMetaData::length));
            for (int i = 0; i < files.length; i++) {
                final StoreFileMetaData md = files[i];
                try (IndexInput indexInput =
                         store.directory().openInput(md.name(), IOContext.READONCE)) {
                    // it's fine that we are only having the indexInput in the try/with block. The
                    // copy methods handles exceptions during close correctly and doesn't hide
                    // the original exception.
                    Streams.copy(new InputStreamIndexInput(indexInput, md.length()),
                        outputStreamFactory.apply(md));
                } catch (Exception e) {
                    final IOException corruptIndexException;
                    if ((corruptIndexException = ExceptionsHelper.unwrapCorruption(e)) != null) {
                        if (store.checkIntegrityNoException(md) == false) {
                            // we are corrupted on the primary -- fail!
                            logger.warn("Corrupted file detected {} checksum mismatch", md);
                            failEngine(corruptIndexException);
                            throw corruptIndexException;
                        } else { // corruption has happened on the way to replica
                            RemoteTransportException exception =
                                new RemoteTransportException(
                                    "File corruption occurred on recovery but checksums are ok",
                                    null);
                            exception.addSuppressed(e);
                            logger.warn(
                                (org.apache.logging.log4j.util.Supplier<?>)
                                    () -> new ParameterizedMessage(
                                    "Remote file corruption on node {}, " +
                                        "recovering {}. local checksum OK",
                                    request.targetNode(),
                                    md),
                                corruptIndexException);
                            throw exception;
                        }
                    } else {
                        throw e;
                    }
                }
            }
        } finally {
            store.decRef();
        }
    }

    protected void failEngine(IOException cause) {
        shard.failShard("recovery", cause);
    }
}
