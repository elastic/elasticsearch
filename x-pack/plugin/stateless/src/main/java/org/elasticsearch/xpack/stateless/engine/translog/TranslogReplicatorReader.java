/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine.translog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.core.Strings.format;

/**
 * A {@link Translog.Snapshot} implementation that can read the translog operations from the object store's compound translog files (as
 * stored by {@link TranslogReplicator}), which are related to a given shard id and fall within a given inclusive range of seq nos.
 */
public class TranslogReplicatorReader implements Translog.Snapshot {

    private static final Logger logger = LogManager.getLogger(TranslogReplicatorReader.class);
    public static final TimeValue SIXTY_SECONDS_SLOW_RECOVERY_THRESHOLD = TimeValue.timeValueSeconds(60);

    private final ShardId shardId;
    private final long fromSeqNo;
    private final long toSeqNo;
    private final long translogRecoveryStartFile;
    private final BooleanSupplier isClosing;

    private final BlobContainer translogBlobContainer;
    private final Iterator<? extends Translog.Operation> operations;
    private final List<BlobMetadata> blobsToRead;
    private final List<BlobMetadata> blobsMissed = new ArrayList<>();
    private final long startNanos;
    private long operationsReadNanos;
    private boolean shortCircuitDueToHole = false;
    private long previousTranslogShardGeneration = -1;
    private long filesWithShardOperations = 0;
    private long operationBytesRead = 0;
    private long operationsRead = 0;
    private long indexOperationsProcessed = 0;
    private long indexOperationsWithIDProcessed = 0;
    private long deleteOperationsProcessed = 0;
    private long noOpOperationsProcessed = 0;

    /**
     * Creates the reader and captures the compound translog files from the object store that will be read when iterating.
     *
     * @param translogBlobContainer     the translog blob container to use
     * @param shardId                   the shard id whose translog operations to return
     * @param fromSeqNo                 each returned operation is equal or larger than this seq no
     * @param toSeqNo                   each returned operation is equal or smaller than this seq no
     * @param translogRecoveryStartFile the translog file to initiate recovery from
     * @param isClosing                 indicates if the recovery should be cancelled because of engine shutdown
     * @throws IOException                related to listing blobs from the object store
     * @throws TranslogCorruptedException in case the checksum of the checkpoints of a compound translog file is incorrect, or an inner
     *                                    {@link IOException} occurred while reading from the translog file and/or the object store
     */
    public TranslogReplicatorReader(
        final BlobContainer translogBlobContainer,
        final ShardId shardId,
        final long fromSeqNo,
        final long toSeqNo,
        final long translogRecoveryStartFile,
        BooleanSupplier isClosing
    ) throws IOException {
        this.translogRecoveryStartFile = translogRecoveryStartFile;
        this.isClosing = isClosing;
        assert fromSeqNo <= toSeqNo : fromSeqNo + " > " + toSeqNo;
        assert fromSeqNo >= 0 : "fromSeqNo must be non-negative " + fromSeqNo;
        this.shardId = shardId;
        this.fromSeqNo = fromSeqNo;
        this.toSeqNo = toSeqNo;
        this.translogBlobContainer = translogBlobContainer;
        this.blobsToRead = translogBlobContainer.listBlobs(OperationPurpose.TRANSLOG)
            .entrySet()
            .stream()
            .filter(e -> Long.parseLong(e.getKey()) >= translogRecoveryStartFile)
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();
        this.operations = Iterators.flatMap(blobsToRead.iterator(), this::readBlobTranslogOperations);
        this.startNanos = System.nanoTime();
    }

    /**
     * Creates the reader and captures the compound translog files from the object store that will be read when iterating.
     *
     * @param translogBlobContainer the translog blob container to use
     * @param shardId               the shard id whose translog operations to return
     * @throws IOException          related to reading from the object store
     */
    // This ctor is only used in tests
    public TranslogReplicatorReader(final BlobContainer translogBlobContainer, final ShardId shardId) throws IOException {
        this(translogBlobContainer, shardId, 0, Long.MAX_VALUE, 0, () -> false);
    }

    @Override
    public int totalOperations() {
        return blobsToRead.isEmpty() ? 0 : RecoveryState.Translog.UNKNOWN;
    }

    private Iterator<Translog.Operation> readBlobTranslogOperations(BlobMetadata blobMetadata) {
        if (isClosing.getAsBoolean()) {
            throw new AlreadyClosedException("Translog recovery cancelled because engine is closing.");
        }
        long operationsReadStartNanos = System.nanoTime();
        try (
            StreamInput streamInput = new InputStreamStreamInput(
                translogBlobContainer.readBlob(OperationPurpose.TRANSLOG, blobMetadata.name())
            )
        ) {
            CompoundTranslogHeader translogHeader = CompoundTranslogHeader.readFromStore(blobMetadata.name(), streamInput);
            Iterator<Translog.Operation> operationIterator = getOperationIterator(blobMetadata.name(), streamInput, translogHeader);
            operationsReadNanos += System.nanoTime() - operationsReadStartNanos;
            return operationIterator;
        } catch (NoSuchFileException e) {
            // Skip this file in case it is not relevant for this shard. We will fail when reading the next translog file if there is a hole
            // in the generations
            blobsMissed.add(blobMetadata);
            return Collections.emptyIterator();
        } catch (IOException e) {
            throw new TranslogCorruptedException(blobMetadata.name(), "error while reading translog file from object store", e);
        }
    }

    private Iterator<Translog.Operation> getOperationIterator(String name, StreamInput streamInput, CompoundTranslogHeader translogHeader)
        throws IOException {
        if (shortCircuitDueToHole) {
            return Collections.emptyIterator();
        }

        Map<ShardId, TranslogMetadata> metadata = translogHeader.metadata();

        // Check if the compound translog file contains eligible operations for this shard
        if (metadata.containsKey(shardId) && metadata.get(shardId).totalOps() != 0) {
            TranslogMetadata translogMetadata = metadata.get(shardId);
            if (previousTranslogShardGeneration == -1) {
                previousTranslogShardGeneration = translogMetadata.shardTranslogGeneration();
            } else {
                long currentShardGeneration = translogMetadata.shardTranslogGeneration();
                long diff = currentShardGeneration - previousTranslogShardGeneration;
                if (currentShardGeneration != -1L && diff == 1L) {
                    previousTranslogShardGeneration = currentShardGeneration;
                } else {
                    shortCircuitDueToHole = true;

                    logger.warn(
                        format(
                            "translog recovery for shard [%s] hit hole in files while reading file [%s][%s]. went from "
                                + "[translog_shard_generation=%s] to [translog_shard_generation=%s]. at beginning of recovery listed "
                                + "files: %s. files missing during recovery: %s",
                            shardId,
                            translogBlobContainer.path(),
                            name,
                            previousTranslogShardGeneration,
                            currentShardGeneration,
                            blobsToRead.stream().map(BlobMetadata::name).toList(),
                            blobsMissed.stream().map(BlobMetadata::name).toList()
                        )
                    );

                    return Collections.emptyIterator();
                }
            }
            // Check if at least one of the operations fall within the eligible range
            if (toSeqNo >= translogMetadata.minSeqNo() && fromSeqNo <= translogMetadata.maxSeqNo()) {
                // Go to the translog file to read it
                streamInput.skipNBytes(translogMetadata.offset());
                BytesReference translogBytes = streamInput.readBytesReference((int) translogMetadata.size());

                // Read operations from the translog file
                int numOps = (int) translogMetadata.totalOps();
                List<Translog.Operation> eligibleOperations = new ArrayList<>(numOps);

                final BufferedChecksumStreamInput checksumStreamInput = new BufferedChecksumStreamInput(
                    translogBytes.streamInput(),
                    "translog replicator"
                );

                for (int i = 0; i < numOps; i++) {
                    Translog.Operation operation = Translog.readOperation(checksumStreamInput);
                    // Add only eligible operations
                    if (toSeqNo >= operation.seqNo() && fromSeqNo <= operation.seqNo()) {
                        eligibleOperations.add(operation);

                        if (operation.opType() == Translog.Operation.Type.INDEX) {
                            ++indexOperationsProcessed;
                            // If the operation does not have an autogenerated id we want to track this as it may have different indexing
                            // performance characteristics
                            if (((Translog.Index) operation).getAutoGeneratedIdTimestamp() == -1L) {
                                ++indexOperationsWithIDProcessed;
                            }
                        } else if (operation.opType() == Translog.Operation.Type.DELETE) {
                            ++deleteOperationsProcessed;
                        } else if (operation.opType() == Translog.Operation.Type.NO_OP) {
                            ++noOpOperationsProcessed;
                        }
                    }
                }

                filesWithShardOperations++;
                operationsRead += numOps;
                operationBytesRead += translogMetadata.size();
                return eligibleOperations.iterator();
            }
        }
        return Collections.emptyIterator();
    }

    /**
     * Returns the next translog operation. Returns null if finished.
     *
     * @throws TranslogCorruptedException in case the checksum of the checkpoints of a compound translog file is incorrect, or an inner
     *                                    {@link IOException} occurred while reading from the translog file and/or the object store
     */
    @Override
    public Translog.Operation next() throws IOException {
        return operations.hasNext() ? operations.next() : null;
    }

    @Override
    public void close() throws IOException {
        TimeValue recoveryTime = TimeValue.timeValueNanos(System.nanoTime() - startNanos);
        if (recoveryTime.compareTo(SIXTY_SECONDS_SLOW_RECOVERY_THRESHOLD) >= 0) {
            int blobCount = blobsToRead.size();
            logger.warn(
                "slow [{}] stateless translog recovery [translogRecoveryStartFile={}, blobsToRead_count={}, blobsToRead_first={}, "
                    + "blobsToRead_last={}, operationsReadNanos={}, blobsToRead_bytes={}, filesWithShardOperations={}, operationsRead={}, "
                    + "operationBytesRead={}, indexOperationsProcessed={}, indexOperationsWithIdProcessed={}, "
                    + "deleteOperationsProcessed={}, noOpOperationsProcessed={}]",
                recoveryTime,
                translogRecoveryStartFile,
                blobCount,
                blobCount > 0 ? blobsToRead.get(0).name() : "N/A",
                blobCount > 0 ? blobsToRead.get(blobCount - 1).name() : "N/A",
                TimeValue.timeValueNanos(operationsReadNanos),
                new ByteSizeValue(blobsToRead.stream().mapToLong(BlobMetadata::length).sum(), ByteSizeUnit.BYTES),
                filesWithShardOperations,
                operationsRead,
                new ByteSizeValue(operationBytesRead, ByteSizeUnit.BYTES),
                indexOperationsProcessed,
                indexOperationsWithIDProcessed,
                deleteOperationsProcessed,
                noOpOperationsProcessed
            );
        }

    }
}
