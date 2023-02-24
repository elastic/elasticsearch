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

import co.elastic.elasticsearch.stateless.ObjectStoreService;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.index.translog.TranslogCorruptedException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * An iterator that can read the translogs from the object store's compound translog files (as stored by {@link TranslogReplicator}),
 * which are related to a given shard id and contain operations with at least a given seq no.
 *
 * For each returned translog, a corresponding {@link TranslogReplicatorReader.Entry} is returned, which contains the data (seq nos) and the
 * metadata with the size, min and max seq no of the translog, and the offset where it was found in the original compound translog file.
 *
 * Note that returned translogs may contain mixed seq no operations. Thus a translog may contain operations with a lower seq no than
 * the given starting seq no, but will contain at least one seq no that is equal or larger than the starting seq no.
 */
public class TranslogReplicatorReader implements Iterator<TranslogReplicatorReader.Entry> {

    public record Entry(TranslogMetadata metadata, BytesReference data) {}

    private final ShardId shardId;
    private final long startingSeqNo;

    private final BlobContainer translogBlobContainer;
    private final Map<String, BlobMetadata> blobs;
    private final Iterator<String> compoundFiles;
    private Entry next = null;

    /**
     * Creates the iterator and captures the compound translog files from the object store that will be read when iterating.
     *
     * @param objectStoreService the object store service to use
     * @param shardId            the shard id whose translogs to return
     * @param startingSeqNo      each returned translog will contain at least one operation whose seq no is equal or larger than this seq no
     * @throws IOException related to reading from the object store
     */
    public TranslogReplicatorReader(final ObjectStoreService objectStoreService, final ShardId shardId, final long startingSeqNo)
        throws IOException {
        this.shardId = shardId;
        this.startingSeqNo = startingSeqNo;
        this.translogBlobContainer = objectStoreService.getLocalTranslogBlobContainer();
        this.blobs = this.translogBlobContainer.listBlobs();
        this.compoundFiles = blobs.keySet().stream().sorted().iterator();
    }

    /**
     * Creates the iterator and captures the compound translog files from the object store that will be read when iterating.
     *
     * @param objectStoreService the object store service to use
     * @param shardId            the shard id whose translogs to return
     * @throws IOException related to reading from the object store
     */
    public TranslogReplicatorReader(final ObjectStoreService objectStoreService, final ShardId shardId) throws IOException {
        this(objectStoreService, shardId, 0);
    }

    /**
     * Returns whether there is a next translog entry.
     *
     * @throws TranslogCorruptedException in case the checksum of the checkpoints of a compound translog file is incorrect
     */
    @Override
    public boolean hasNext() {
        while (next == null && compoundFiles.hasNext()) {
            String compoundFile = compoundFiles.next();
            BlobMetadata metadata = blobs.get(compoundFile);
            try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(metadata.name()))) {
                BufferedChecksumStreamInput bufferedChecksumStreamInput = new BufferedChecksumStreamInput(streamInput, metadata.name());
                Map<ShardId, TranslogMetadata> checkpoints = bufferedChecksumStreamInput.readMap(ShardId::new, TranslogMetadata::new);
                long expectedChecksum = bufferedChecksumStreamInput.getChecksum();
                long readChecksum = streamInput.readLong();
                if (readChecksum != expectedChecksum) {
                    throw new TranslogCorruptedException(
                        metadata.name(),
                        "checksum verification failed - expected: 0x"
                            + Long.toHexString(expectedChecksum)
                            + ", got: 0x"
                            + Long.toHexString(readChecksum)
                    );
                }

                if (checkpoints.containsKey(shardId)) {
                    TranslogMetadata translogMetadata = checkpoints.get(shardId);
                    if (startingSeqNo <= translogMetadata.maxSeqNo()) {
                        streamInput.skipNBytes(translogMetadata.offset());
                        BytesReference shardTranslog = streamInput.readBytesReference((int) translogMetadata.size());
                        next = new Entry(translogMetadata, shardTranslog);
                    }
                }
            } catch (IOException e) {
                throw new TranslogCorruptedException(metadata.name(), "error while reading translog file from object store", e);
            }
        }
        return next != null;
    }

    /**
     * Returns the next translog entry.
     *
     * @throws NoSuchElementException if no such entry exists
     * @throws TranslogCorruptedException in case the checksum of the checkpoints of a compound translog file is incorrect
     */
    @Override
    public Entry next() {
        if (hasNext()) {
            Entry toReturn = next;
            next = null;
            return toReturn;
        } else {
            throw new NoSuchElementException();
        }
    }
}
