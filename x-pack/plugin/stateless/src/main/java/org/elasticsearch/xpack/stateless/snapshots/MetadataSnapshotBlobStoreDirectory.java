/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * A minimal Lucene {@link org.apache.lucene.store.Directory} backed by blob store.
 * Used to construct {@link Store.MetadataSnapshot} from the object store without needing
 * access to a local shard or its store.
 * <p>
 * For small files ({@code .si} and {@code segments_N}), the full content is read into memory.
 * For all other files, only the Lucene footer ({@link CodecUtil#footerLength()} bytes) is read,
 * since {@link Store.MetadataSnapshot#loadFromIndexCommit} only needs the footer checksum for
 * those files.
 */
class MetadataSnapshotBlobStoreDirectory extends BaseDirectory {

    private final Map<String, BlobLocation> blobLocations;
    private final BiFunction<ShardId, Long, BlobContainer> blobContainerFunc;
    private final ShardId shardId;

    MetadataSnapshotBlobStoreDirectory(
        Map<String, BlobLocation> blobLocations,
        BiFunction<ShardId, Long, BlobContainer> blobContainerFunc,
        ShardId shardId
    ) {
        super(NoLockFactory.INSTANCE);
        this.blobLocations = blobLocations;
        this.blobContainerFunc = blobContainerFunc;
        this.shardId = shardId;
    }

    @Override
    public String[] listAll() {
        return blobLocations.keySet().stream().sorted().toArray(String[]::new);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return getBlobLocationSafe(name).fileLength();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final var blobLocation = getBlobLocationSafe(name);
        final var blobContainer = blobContainerFunc.apply(shardId, blobLocation.primaryTerm());
        if (Store.MetadataSnapshot.isReadAsHash(name)) {
            return readFullFile(name, blobContainer, blobLocation);
        } else {
            return readFooterOnly(name, blobContainer, blobLocation);
        }
    }

    private BlobLocation getBlobLocationSafe(String name) throws IOException {
        final var blobLocation = blobLocations.get(name);
        if (blobLocation == null) {
            assert false : shardId + ": file " + name + " not found in " + blobLocations.keySet();
            throw new FileNotFoundException("file " + name + " not found for shard " + shardId);
        }
        return blobLocation;
    }

    private static IndexInput readFullFile(String name, BlobContainer blobContainer, BlobLocation blobLocation) throws IOException {
        final var fileLength = blobLocation.fileLength();
        checkFooterLength(name, fileLength);
        final var length = Math.toIntExact(fileLength);
        final var bytes = new byte[length];
        try (
            var stream = blobContainer.readBlob(OperationPurpose.SNAPSHOT_METADATA, blobLocation.blobName(), blobLocation.offset(), length)
        ) {
            stream.readNBytes(bytes, 0, length);
        }
        return new ByteArrayIndexInput(name, bytes);
    }

    private static IndexInput readFooterOnly(String name, BlobContainer blobContainer, BlobLocation blobLocation) throws IOException {
        final var fileLength = blobLocation.fileLength();
        checkFooterLength(name, fileLength);
        final var footerLength = CodecUtil.footerLength();
        final var footer = new byte[footerLength];
        try (
            var stream = blobContainer.readBlob(
                OperationPurpose.SNAPSHOT_METADATA,
                blobLocation.blobName(),
                blobLocation.offset() + fileLength - footerLength,
                footerLength
            )
        ) {
            stream.readNBytes(footer, 0, footerLength);
        }
        return new FooterOnlyIndexInput(name, fileLength, footer);
    }

    private static void checkFooterLength(String name, long fileLength) throws CorruptIndexException {
        if (fileLength < CodecUtil.footerLength()) {
            throw new CorruptIndexException("file length must be >= " + CodecUtil.footerLength() + " but was: " + fileLength, name);
        }
    }

    @Override
    public void close() {}

    @Override
    public void deleteFile(String name) {
        throw unsupported();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        throw unsupported();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw unsupported();
    }

    @Override
    public void sync(Collection<String> names) {
        throw unsupported();
    }

    @Override
    public void syncMetaData() {
        throw unsupported();
    }

    @Override
    public void rename(String source, String dest) {
        throw unsupported();
    }

    @Override
    public Set<String> getPendingDeletions() {
        throw unsupported();
    }

    private static UnsupportedOperationException unsupported() {
        assert false;
        return new UnsupportedOperationException();
    }

    /**
     * An {@link IndexInput} that only holds the Lucene footer bytes of a file.
     * Reports the full file length so that {@link CodecUtil#retrieveChecksum} can seek
     * to the correct footer position ({@code length - footerLength}) and read the checksum.
     * Any reads outside the footer range will fail.
     */
    static class FooterOnlyIndexInput extends IndexInput {
        private final long fileLength;
        private final byte[] footer;
        private final long footerStart;
        private int pos; // position relative to footerStart

        FooterOnlyIndexInput(String resourceDescription, long fileLength, byte[] footer) {
            super(resourceDescription);
            this.fileLength = fileLength;
            this.footer = footer;
            this.footerStart = fileLength - footer.length;
            this.pos = 0;
        }

        @Override
        public long length() {
            return fileLength;
        }

        @Override
        public long getFilePointer() {
            return footerStart + pos;
        }

        @Override
        public void seek(long newPos) throws IOException {
            assert newPos == footerStart : "expected seek to footer start " + footerStart + " but got " + newPos;
            if (newPos < footerStart || newPos > fileLength) {
                throw new EOFException("seek position " + newPos + " is outside footer range [" + footerStart + ", " + fileLength + "]");
            }
            this.pos = (int) (newPos - footerStart);
        }

        @Override
        public byte readByte() throws IOException {
            if (pos >= footer.length) {
                throw new EOFException("read past EOF");
            }
            return footer[pos++];
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            if (pos + len > footer.length) {
                throw new EOFException("read past EOF");
            }
            System.arraycopy(footer, pos, b, offset, len);
            pos += len;
        }

        @Override
        public void close() {}

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) {
            throw unsupported();
        }
    }
}
