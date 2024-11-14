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

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

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
    private final int estimatedOperations;
    private final List<BlobMetadata> blobsToRead;
    private final long startNanos;
    private final TranslogRecoveryMetrics translogRecoveryMetrics;
    private long operationsReadNanos;
    private long createPlanNanos;
    private long listUnfilteredFilesNanos;
    private long filesWithShardOperations = 0;
    private long operationBytesRead = 0;
    private long operationsRead = 0;
    private long indexOperationsProcessed = 0;
    private long indexOperationsWithIDProcessed = 0;
    private long deleteOperationsProcessed = 0;
    private long noOpOperationsProcessed = 0;
    private long unreferencedBlobCount;
    private long unreferencedBlobSizeInBytes;

    /**
     * Creates the reader and captures the compound translog files from the object store that will be read when iterating.
     *
     * @param translogBlobContainer     the translog blob container to use
     * @param shardId                   the shard id whose translog operations to return
     * @param fromSeqNo                 each returned operation is equal or larger than this seq no
     * @param toSeqNo                   each returned operation is equal or smaller than this seq no
     * @param translogRecoveryStartFile the translog file to initiate recovery from
     * @param isClosing                 indicates if the recovery should be cancelled because of engine shutdown
     * @param translogRecoveryMetrics   metrics counter container
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
        final BooleanSupplier isClosing,
        final TranslogRecoveryMetrics translogRecoveryMetrics
    ) throws IOException {
        this.translogRecoveryStartFile = translogRecoveryStartFile;
        this.isClosing = isClosing;
        assert fromSeqNo <= toSeqNo : fromSeqNo + " > " + toSeqNo;
        assert fromSeqNo >= 0 : "fromSeqNo must be non-negative " + fromSeqNo;
        this.shardId = shardId;
        this.fromSeqNo = fromSeqNo;
        this.translogBlobContainer = translogBlobContainer;
        this.toSeqNo = toSeqNo;
        this.startNanos = System.nanoTime();
        assert fromSeqNo <= toSeqNo : fromSeqNo + " > " + toSeqNo;
        assert fromSeqNo >= 0 : "fromSeqNo must be non-negative " + fromSeqNo;
        Map<Long, BlobMetadata> unFilteredBlobs = translogBlobContainer.listBlobs(OperationPurpose.TRANSLOG)
            .entrySet()
            .stream()
            .map(e -> Map.entry(Long.parseLong(e.getKey()), e.getValue()))
            .filter(e -> e.getKey() >= translogRecoveryStartFile)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        listUnfilteredFilesNanos = System.nanoTime() - startNanos;

        RecoveryPlan recoveryPlan = createPlan(unFilteredBlobs, translogRecoveryStartFile);
        estimatedOperations = recoveryPlan.estimatedOps();

        var toRead = new ArrayList<BlobMetadata>();

        // We only recover as long as we have a contiguous set of expected files. If we hit a hole, we stop recovering and log a
        // warning. This is still valid, but rare. It is possible for a translog file to be flushed before an earlier file is flushed
        // and then the node shuts down. We would not have ACKed the operations from the later files.
        int j = 0;
        for (Long referenced : recoveryPlan.referencedTranslogFiles()) {
            BlobMetadata blobMetadata = unFilteredBlobs.get(referenced);
            if (blobMetadata == null) {
                logger.info(
                    "[{}] translog recovery hit hole [listedFiles={}, translogStartFile={}, directoryFiles={}, skippedFiles={}]",
                    shardId,
                    unFilteredBlobs.keySet().stream().mapToLong(l -> l).sorted().boxed().toList(),
                    translogRecoveryStartFile,
                    recoveryPlan.referencedTranslogFiles(),
                    recoveryPlan.referencedTranslogFiles().subList(j, recoveryPlan.referencedTranslogFiles().size())
                );
                break;
            } else {
                toRead.add(blobMetadata);
            }
            j++;
        }
        this.blobsToRead = toRead;

        logger.debug(
            () -> format("translog replicator reader opened for recovery %s", blobsToRead.stream().map(BlobMetadata::name).toList())
        );
        this.translogRecoveryMetrics = translogRecoveryMetrics;
        this.operations = Iterators.flatMap(blobsToRead.iterator(), this::readBlobTranslogOperations);
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
        this(translogBlobContainer, shardId, 0, Long.MAX_VALUE, 0, () -> false, TranslogRecoveryMetrics.NOOP);
    }

    @Override
    public int totalOperations() {
        return blobsToRead.isEmpty() ? 0 : estimatedOperations;
    }

    private record RecoveryPlan(int estimatedOps, List<Long> referencedTranslogFiles) {}

    private RecoveryPlan createPlan(Map<Long, BlobMetadata> listedBlobsAboveStartFile, long translogRecoveryStartFile) throws IOException {
        List<Long> sorted = listedBlobsAboveStartFile.keySet().stream().sorted().toList();
        for (int i = sorted.size() - 1; i >= 0; i--) {
            Long translogGeneration = sorted.get(i);
            BlobMetadata blobMetadata = listedBlobsAboveStartFile.get(translogGeneration);
            long createPlanStartNanos = System.nanoTime();
            try (
                StreamInput streamInput = new InputStreamStreamInput(
                    translogBlobContainer.readBlob(OperationPurpose.TRANSLOG, blobMetadata.name())
                )
            ) {
                CompoundTranslogHeader translogHeader = CompoundTranslogHeader.readFromStore(blobMetadata.name(), streamInput);
                createPlanNanos += System.nanoTime() - createPlanStartNanos;
                TranslogMetadata metadata = translogHeader.metadata().get(shardId);
                if (metadata != null) {
                    assert metadata.directory() != null;
                    ArrayList<Long> referenced = new ArrayList<>();
                    for (int offset : metadata.directory().referencedTranslogFileOffsets()) {
                        long absolute = translogGeneration - offset;
                        if (absolute >= translogRecoveryStartFile) {
                            referenced.add(absolute);
                        }
                    }
                    if (metadata.totalOps() > 0) {
                        referenced.add(translogGeneration);
                    }
                    return new RecoveryPlan(Math.toIntExact(metadata.directory().estimatedOperationsToRecover()), referenced);
                } else {
                    unreferencedBlobCount++;
                    unreferencedBlobSizeInBytes += blobMetadata.length();
                }
            } catch (NoSuchFileException e) {
                // It is possible for an indexing shard to delete a file since we listed files. This is valid as it is possible this file
                // was not related to the shard we are recovering. Just proceed to read the next file.
                logger.debug(
                    format("listed translog [%s] not found while searching for shard directory, moving to next file", translogGeneration),
                    e
                );
            }
        }

        return new RecoveryPlan(0, Collections.emptyList());
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
        } catch (IOException e) {
            throw new TranslogCorruptedException(blobMetadata.name(), "error while reading translog file from object store", e);
        }
    }

    private Iterator<Translog.Operation> getOperationIterator(String name, StreamInput streamInput, CompoundTranslogHeader translogHeader)
        throws IOException {
        Map<ShardId, TranslogMetadata> metadata = translogHeader.metadata();

        // Check if the compound translog file contains eligible operations for this shard
        if (metadata.containsKey(shardId) && metadata.get(shardId).totalOps() != 0) {
            TranslogMetadata translogMetadata = metadata.get(shardId);
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

        var translogReplayTime = TimeValue.timeValueNanos(System.nanoTime() - startNanos);
        var networkTime = TimeValue.timeValueNanos(listUnfilteredFilesNanos + createPlanNanos + operationsReadNanos);
        var blobCount = blobsToRead.size();
        var blobSizeInBytes = new ByteSizeValue(blobsToRead.stream().mapToLong(BlobMetadata::length).sum(), ByteSizeUnit.BYTES);

        if (translogReplayTime.compareTo(SIXTY_SECONDS_SLOW_RECOVERY_THRESHOLD) >= 0) {
            logger.warn(
                "[{}] slow stateless translog recovery [translogRecoveryStartFile={}, blobsToRead_count={}, blobsToRead_first={}, "
                    + "blobsToRead_last={}, networkTime={}, blobsToRead_bytes={}, filesWithShardOperations={}, operationsRead={}, "
                    + "operationBytesRead={}, indexOperationsProcessed={}, indexOperationsWithIdProcessed={}, "
                    + "deleteOperationsProcessed={}, noOpOperationsProcessed={}, unreferencedBlobCount={}, unreferencedBlobSizeInBytes={}]",
                translogReplayTime,
                translogRecoveryStartFile,
                blobCount,
                blobCount > 0 ? blobsToRead.get(0).name() : "N/A",
                blobCount > 0 ? blobsToRead.get(blobCount - 1).name() : "N/A",
                networkTime,
                blobSizeInBytes,
                filesWithShardOperations,
                operationsRead,
                new ByteSizeValue(operationBytesRead, ByteSizeUnit.BYTES),
                indexOperationsProcessed,
                indexOperationsWithIDProcessed,
                deleteOperationsProcessed,
                noOpOperationsProcessed,
                unreferencedBlobCount,
                unreferencedBlobSizeInBytes
            );
        }

        translogRecoveryMetrics.getTranslogReplayTimeHistogram().record(translogReplayTime.millis());
        translogRecoveryMetrics.getTranslogFilesNetworkTimeHistogram().record(networkTime.millis());

        translogRecoveryMetrics.getTranslogFilesTotalCounter().incrementBy(blobCount, Map.of("translog_blob_type", "referenced"));
        translogRecoveryMetrics.getTranslogFilesTotalCounter()
            .incrementBy(unreferencedBlobCount, Map.of("translog_blob_type", "unreferenced"));

        translogRecoveryMetrics.getTranslogFilesSizeCounter()
            .incrementBy(blobSizeInBytes.getBytes(), Map.of("translog_blob_type", "referenced"));
        translogRecoveryMetrics.getTranslogFilesSizeCounter()
            .incrementBy(unreferencedBlobSizeInBytes, Map.of("translog_blob_type", "unreferenced"));

        translogRecoveryMetrics.getTranslogOperationsTotalCounter()
            .incrementBy(indexOperationsProcessed, Map.of("translog_op_type", "index"));
        translogRecoveryMetrics.getTranslogOperationsTotalCounter()
            .incrementBy(deleteOperationsProcessed, Map.of("translog_op_type", "delete"));
        translogRecoveryMetrics.getTranslogOperationsTotalCounter()
            .incrementBy(noOpOperationsProcessed, Map.of("translog_op_type", "noop"));

        translogRecoveryMetrics.getTranslogOperationsSizeCounter().incrementBy(operationBytesRead);
    }
}
