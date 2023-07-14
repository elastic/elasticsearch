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

package co.elastic.elasticsearch.stateless.engine;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A {@link Translog.Snapshot} implementation that can read the translog operations from the object store's compound translog files (as
 * stored by {@link TranslogReplicator}), which are related to a given shard id and fall within a given inclusive range of seq nos.
 */
public class TranslogReplicatorReader implements Translog.Snapshot {

    private final ShardId shardId;
    private final long fromSeqNo;
    private final long toSeqNo;

    private final BlobContainer translogBlobContainer;
    private final Iterator<? extends Translog.Operation> operations;

    /**
     * Creates the reader and captures the compound translog files from the object store that will be read when iterating.
     *
     * @param translogBlobContainer     the translog blob container to use
     * @param shardId                   the shard id whose translog operations to return
     * @param fromSeqNo                 each returned operation is equal or larger than this seq no
     * @param toSeqNo                   each returned operation is equal or smaller than this seq no
     * @param translogRecoveryStartFile the translog file to initiate recovery from
     * @throws IOException                related to listing blobs from the object store
     * @throws TranslogCorruptedException in case the checksum of the checkpoints of a compound translog file is incorrect, or an inner
     *                                    {@link IOException} occurred while reading from the translog file and/or the object store
     */
    public TranslogReplicatorReader(
        final BlobContainer translogBlobContainer,
        final ShardId shardId,
        final long fromSeqNo,
        final long toSeqNo,
        final long translogRecoveryStartFile
    ) throws IOException {
        assert fromSeqNo <= toSeqNo : fromSeqNo + " > " + toSeqNo;
        assert fromSeqNo >= 0 : "fromSeqNo must be non-negative " + fromSeqNo;
        this.shardId = shardId;
        this.fromSeqNo = fromSeqNo;
        this.toSeqNo = toSeqNo;
        this.translogBlobContainer = translogBlobContainer;
        Iterator<BlobMetadata> blobs = translogBlobContainer.listBlobs()
            .entrySet()
            .stream()
            .filter(e -> Long.parseLong(e.getKey()) >= translogRecoveryStartFile)
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .iterator();
        operations = Iterators.flatMap(blobs, this::readBlobTranslogOperations);
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
        this(translogBlobContainer, shardId, 0, Long.MAX_VALUE, 0);
    }

    @Override
    public int totalOperations() {
        return RecoveryState.Translog.UNKNOWN;
    }

    private Iterator<Translog.Operation> readBlobTranslogOperations(BlobMetadata blobMetadata) {
        boolean tryOldVersion = false;
        try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(blobMetadata.name()))) {
            CompoundTranslogHeader translogHeader = CompoundTranslogHeader.readFromStore(blobMetadata.name(), streamInput);
            return getOperationIterator(streamInput, translogHeader);
        } catch (CompoundTranslogHeader.NoVersionCodecException e) {
            tryOldVersion = true;
        } catch (IOException e) {
            throw new TranslogCorruptedException(blobMetadata.name(), "error while reading translog file from object store", e);
        }

        assert tryOldVersion;
        return readBlobTranslogOperationsOld(blobMetadata);
    }

    private Iterator<Translog.Operation> readBlobTranslogOperationsOld(BlobMetadata blobMetadata) {
        try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(blobMetadata.name()))) {
            CompoundTranslogHeader translogHeader = CompoundTranslogHeader.readFromStoreOld(blobMetadata.name(), streamInput);
            return getOperationIterator(streamInput, translogHeader);
        } catch (IOException e) {
            throw new TranslogCorruptedException(blobMetadata.name(), "error while reading translog file from object store", e);
        }
    }

    private Iterator<Translog.Operation> getOperationIterator(StreamInput streamInput, CompoundTranslogHeader translogHeader)
        throws IOException {
        Map<ShardId, TranslogMetadata> metadata = translogHeader.metadata();

        // Check if the compound translog file contains eligible operations for this shard
        if (metadata.containsKey(shardId)) {
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
                    }
                }
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
    public void close() throws IOException {}
}
