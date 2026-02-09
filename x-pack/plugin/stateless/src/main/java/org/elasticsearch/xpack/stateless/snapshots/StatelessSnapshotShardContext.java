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

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * A {@link SnapshotShardContext} implementation that reads shard data from an object store instead of a local shard.
 */
public class StatelessSnapshotShardContext extends SnapshotShardContext {

    private static final Logger logger = LogManager.getLogger(StatelessSnapshotShardContext.class);

    private final ShardId shardId;
    private final Store.MetadataSnapshot metadataSnapshot;
    private final Map<String, BlobLocation> fileToBlobLocations;
    private final BiFunction<ShardId, Long, BlobContainer> blobContainerFunc;

    public StatelessSnapshotShardContext(
        ShardId shardId,
        SnapshotId snapshotId,
        IndexId indexId,
        String shardStateIdentifier,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        Store.MetadataSnapshot metadataSnapshot,
        Map<String, BlobLocation> fileToBlobLocations,
        BiFunction<ShardId, Long, BlobContainer> blobContainerFunc,
        ActionListener<ShardSnapshotResult> listener
    ) {
        super(snapshotId, indexId, shardStateIdentifier, snapshotStatus, repositoryMetaVersion, snapshotStartTime, listener);
        this.shardId = shardId;
        this.metadataSnapshot = metadataSnapshot;
        this.fileToBlobLocations = fileToBlobLocations;
        this.blobContainerFunc = blobContainerFunc;
    }

    @Override
    public ShardId shardId() {
        return shardId;
    }

    @Override
    public Releasable withCommitRef() {
        return () -> {}; // No need to retain extra ref since we read from the blobstore
    }

    @Override
    public boolean isSearchableSnapshot() {
        return false;
    }

    @Override
    public Store.MetadataSnapshot metadataSnapshot() {
        return metadataSnapshot;
    }

    @Override
    public Collection<String> fileNames() {
        return fileToBlobLocations.keySet();
    }

    @Override
    public boolean assertFileContentsMatchHash(BlobStoreIndexShardSnapshot.FileInfo fileInfo) {
        try (
            var fileReader = fileReader(fileInfo.physicalName(), fileInfo.metadata());
            var indexInput = new Store.VerifyingIndexInput(
                new InputStreamIndexInput(fileInfo.physicalName(), fileReader.openInput(fileInfo.length()), fileInfo.length())
            )
        ) {
            final byte[] tmp = new byte[Math.toIntExact(fileInfo.metadata().length())];
            indexInput.readBytes(tmp, 0, tmp.length);
            assert fileInfo.metadata().hash().bytesEquals(new BytesRef(tmp));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public void failStoreIfCorrupted(Exception e) {
        if (Lucene.isCorruptionException(e)) {
            logger.warn(
                Strings.format(
                    "Encountered corruption exception while reading snapshot data for shard %s, snapshot [%s]",
                    shardId,
                    snapshotId()
                ),
                e
            );
            // We don't fail the shard since the read is from the object store and not from the local shard
        }
    }

    @Override
    public FileReader fileReader(String file, StoreFileMetadata metadata) throws IOException {
        final BlobLocation blobLocation = fileToBlobLocations.get(file);
        assert blobLocation != null : "Blob location for file [" + file + "] not found in " + fileToBlobLocations;
        final BlobContainer blobContainer = blobContainerFunc.apply(shardId, blobLocation.primaryTerm());
        return new BlobStoreFileReader(blobContainer, blobLocation);
    }

    static class BlobStoreFileReader implements SnapshotShardContext.FileReader {
        private final BlobContainer blobContainer;
        private final BlobLocation blobLocation;
        private final Checksum digest = new BufferedChecksum(new CRC32());
        private long nextPositionInBlobFile; // relative to the start of the entire blob
        private long readPosition; // relative to the start of the file
        private final long checksumPosition; // relative to the start of the file
        private final byte[] checksum = new byte[Long.BYTES];
        private InputStream currentStream; // closed when openInput is called to create a new InputStream and on close()

        BlobStoreFileReader(BlobContainer blobContainer, BlobLocation blobLocation) {
            this.blobContainer = blobContainer;
            this.blobLocation = blobLocation;
            this.nextPositionInBlobFile = blobLocation.offset();
            this.readPosition = 0;
            this.checksumPosition = blobLocation.fileLength() - Long.BYTES;
        }

        @Override
        public InputStream openInput(long limit) throws IOException {
            assert (nextPositionInBlobFile + limit) - blobLocation.offset() <= blobLocation.fileLength()
                : "Requested limit exceeds file length, offset: "
                    + blobLocation.offset()
                    + ", position: "
                    + nextPositionInBlobFile
                    + ", limit: "
                    + limit
                    + ", file length: "
                    + blobLocation.fileLength();

            if (currentStream != null) {
                currentStream.close();
            }

            assert assertPositionConsistency();

            currentStream = new FilterInputStream(
                blobContainer.readBlob(OperationPurpose.SNAPSHOT_DATA, blobLocation.blobName(), nextPositionInBlobFile, limit)
            ) {
                @Override
                public int read() throws IOException {
                    final int b = super.read();
                    if (b != -1) {
                        if (readPosition < checksumPosition) {
                            digest.update(b);
                        } else {
                            checksum[(int) (readPosition - checksumPosition)] = (byte) b;
                        }
                        readPosition += 1;
                    }
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    final int n = super.read(b, off, len);
                    if (n != -1) {
                        if (readPosition < checksumPosition) {
                            final int bytesToChecksum = Math.toIntExact(Math.min(n, checksumPosition - readPosition));
                            digest.update(b, off, bytesToChecksum);
                            if (bytesToChecksum != n) {
                                System.arraycopy(b, off + bytesToChecksum, checksum, 0, n - bytesToChecksum);
                            }
                        } else {
                            assert readPosition - checksumPosition < Long.BYTES
                                : readPosition + " >= " + checksumPosition + " + " + Long.BYTES;
                            assert n <= Long.BYTES : "Read length exceeds checksum length: " + n;
                            System.arraycopy(b, off, checksum, (int) (readPosition - checksumPosition), n);
                        }
                        readPosition += n;
                    }
                    return n;
                }

                @Override
                public long skip(long n) throws IOException {
                    assert false : "snapshot read does not skip bytes";
                    throw new UnsupportedOperationException("blob store input stream for snapshot does not support skip()");
                }

                @Override
                public boolean markSupported() {
                    return false; // Underlying input stream from the blob store does not support mark/reset
                }
            };

            nextPositionInBlobFile += limit;
            return currentStream;
        }

        private boolean assertPositionConsistency() {
            assert readPosition + blobLocation.offset() == nextPositionInBlobFile
                : "input stream was not fully consumed before opening a new one, readPosition: "
                    + readPosition
                    + " + blobOffset: "
                    + blobLocation.offset()
                    + " vs nextPositionInBlobFile: "
                    + nextPositionInBlobFile;
            return true;
        }

        @Override
        public void verify() throws IOException {
            assert assertPositionConsistency();

            final var computedChecksum = digest.getValue();
            final var storedChecksum = CodecUtil.readBELong(new ByteArrayDataInput(checksum));
            if (computedChecksum != storedChecksum) {
                throw new CorruptIndexException(
                    "verification failed : calculated="
                        + Store.digestToString(computedChecksum)
                        + " stored="
                        + Store.digestToString(storedChecksum),
                    this.toString()
                );
            }
        }

        @Override
        public void close() throws IOException {
            if (currentStream != null) {
                currentStream.close();
            }
        }
    }

    // This class is only used in assertions, see assertFileContentsMatchHash
    private static class InputStreamIndexInput extends IndexInput {

        private final InputStream inputStream;
        private final long length;
        private long filePointer = 0L;

        InputStreamIndexInput(String resourceDescription, InputStream inputStream, long length) {
            super(resourceDescription);
            assert Assertions.ENABLED : "this class is only used for assertions";
            this.inputStream = inputStream;
            this.length = length;
        }

        @Override
        public byte readByte() throws IOException {
            final var read = inputStream.read();
            if (read == -1) {
                throw new EOFException();
            }
            filePointer += 1;
            return (byte) read;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            while (len > 0) {
                final var read = inputStream.read(b, offset, len);
                if (read == -1) {
                    throw new EOFException();
                }
                filePointer += read;
                len -= read;
                offset += read;
            }
        }

        @Override
        public void close() throws IOException {}

        @Override
        public long getFilePointer() {
            return filePointer;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public void seek(long pos) throws IOException {
            if (filePointer == pos) {
                return;
            }
            throw new UnsupportedOperationException("seek is not supported");
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            throw new UnsupportedOperationException("slice is not supported");
        }
    }
}
