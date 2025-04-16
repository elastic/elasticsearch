/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobContainerUtils;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.unmodifiableMap;

/**
 * A file system based implementation of {@link org.elasticsearch.common.blobstore.BlobContainer}.
 * All blobs in the container are stored on a file system, the location of which is specified by the {@link BlobPath}.
 *
 * Note that the methods in this implementation of {@link org.elasticsearch.common.blobstore.BlobContainer} may
 * additionally throw a {@link java.lang.SecurityException} if the configured {@link java.lang.SecurityManager}
 * does not permit read and/or write access to the underlying files.
 */
public class FsBlobContainer extends AbstractBlobContainer {

    private static final Logger logger = LogManager.getLogger(FsBlobContainer.class);

    private static final String TEMP_FILE_PREFIX = "pending-";

    protected final FsBlobStore blobStore;
    protected final Path path;

    public FsBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path) {
        super(blobPath);
        this.blobStore = blobStore;
        this.path = path;
    }

    @Override
    public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
        return listBlobsByPrefix(purpose, null);
    }

    @Override
    public Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException {
        Map<String, BlobContainer> builder = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            for (Path file : stream) {
                if (Files.isDirectory(file)) {
                    final String name = file.getFileName().toString();
                    builder.put(name, new FsBlobContainer(blobStore, path().add(name), file));
                }
            }
        }
        return unmodifiableMap(builder);
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) throws IOException {
        Map<String, BlobMetadata> builder = new HashMap<>();

        blobNamePrefix = blobNamePrefix == null ? "" : blobNamePrefix;
        try (DirectoryStream<Path> stream = newDirectoryStreamIfFound(blobNamePrefix)) {
            for (Path file : stream) {
                final BasicFileAttributes attrs;
                try {
                    attrs = Files.readAttributes(file, BasicFileAttributes.class);
                } catch (FileNotFoundException | NoSuchFileException e) {
                    // The file was concurrently deleted trying to get its attributes so we skip it here
                    continue;
                } catch (AccessDeniedException e) {
                    // The file became inaccessible for some reason, possibly an artefact of concurrent deletion (Windows?): warn and skip
                    logger.warn(Strings.format("file [%s] became inaccessible while listing [%s/%s]", file, path, blobNamePrefix), e);
                    assert Constants.WINDOWS : e;
                    continue;
                }
                if (attrs.isRegularFile()) {
                    builder.put(file.getFileName().toString(), new BlobMetadata(file.getFileName().toString(), attrs.size()));
                }
            }
        }
        return unmodifiableMap(builder);
    }

    private DirectoryStream<Path> newDirectoryStreamIfFound(String blobNamePrefix) throws IOException {
        try {
            return Files.newDirectoryStream(path, blobNamePrefix + "*");
        } catch (FileNotFoundException | NoSuchFileException e) {
            // a nonexistent directory contains no blobs
            return new DirectoryStream<>() {
                @Override
                public Iterator<Path> iterator() {
                    return Collections.emptyIterator();
                }

                @Override
                public void close() {}
            };
        }
    }

    @Override
    public DeleteResult delete(OperationPurpose purpose) throws IOException {
        final AtomicLong filesDeleted = new AtomicLong(0L);
        final AtomicLong bytesDeleted = new AtomicLong(0L);
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException impossible) throws IOException {
                assert impossible == null;
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                filesDeleted.incrementAndGet();
                bytesDeleted.addAndGet(attrs.size());
                return FileVisitResult.CONTINUE;
            }
        });
        return new DeleteResult(filesDeleted.get(), bytesDeleted.get());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) throws IOException {
        blobStore.deleteBlobs(Iterators.map(blobNames, blobName -> path.resolve(blobName).toString()));
    }

    @Override
    public boolean blobExists(OperationPurpose purpose, String blobName) {
        return Files.exists(path.resolve(blobName));
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, String name) throws IOException {
        assert BlobContainer.assertPurposeConsistency(purpose, name);
        final Path resolvedPath = path.resolve(name);
        try {
            return Files.newInputStream(resolvedPath);
        } catch (FileNotFoundException fnfe) {
            throw new NoSuchFileException("[" + name + "] blob not found");
        }
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
        assert BlobContainer.assertPurposeConsistency(purpose, blobName);
        final SeekableByteChannel channel = Files.newByteChannel(path.resolve(blobName));
        if (position > 0L) {
            if (channel.size() <= position) {
                try (channel) {
                    throw new RequestedRangeNotSatisfiedException(blobName, position, length);
                }
            }
            channel.position(position);
        }
        assert channel.position() == position;
        return Streams.limitStream(Channels.newInputStream(channel), length);
    }

    @Override
    public long readBlobPreferredLength() {
        // This container returns streams that are cheap to close early, so we can tell consumers to request as much data as possible.
        return Long.MAX_VALUE;
    }

    @Override
    public void writeBlob(OperationPurpose purpose, String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws IOException {
        assert BlobContainer.assertPurposeConsistency(purpose, blobName);
        final Path file = path.resolve(blobName);
        try {
            writeToPath(inputStream, file, blobSize);
        } catch (FileAlreadyExistsException faee) {
            if (failIfAlreadyExists) {
                throw faee;
            }
            deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(blobName));
            writeToPath(inputStream, file, blobSize);
        }
        IOUtils.fsync(path, true);
    }

    @Override
    public void writeBlob(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        assert BlobContainer.assertPurposeConsistency(purpose, blobName);
        final Path file = path.resolve(blobName);
        try {
            writeToPath(bytes, file);
        } catch (FileAlreadyExistsException faee) {
            if (failIfAlreadyExists) {
                throw faee;
            }
            deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(blobName));
            writeToPath(bytes, file);
        }
        IOUtils.fsync(path, true);
    }

    @Override
    public void writeMetadataBlob(
        OperationPurpose purpose,
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
        assert purpose != OperationPurpose.SNAPSHOT_DATA && BlobContainer.assertPurposeConsistency(purpose, blobName) : purpose;
        if (atomic) {
            final String tempBlob = tempBlobName(blobName);
            try {
                writeToPath(purpose, tempBlob, true, writer);
                moveBlobAtomic(purpose, tempBlob, blobName, failIfAlreadyExists);
            } catch (IOException ex) {
                try {
                    deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(tempBlob));
                } catch (IOException e) {
                    ex.addSuppressed(e);
                }
                throw ex;
            }
        } else {
            writeToPath(purpose, blobName, failIfAlreadyExists, writer);
        }
        IOUtils.fsync(path, true);
    }

    private void writeToPath(
        OperationPurpose purpose,
        String blobName,
        boolean failIfAlreadyExists,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
        final Path file = path.resolve(blobName);
        try {
            try (OutputStream out = blobOutputStream(file)) {
                writer.accept(out);
            }
        } catch (FileAlreadyExistsException faee) {
            if (failIfAlreadyExists) {
                throw faee;
            }
            deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(blobName));
            try (OutputStream out = blobOutputStream(file)) {
                writer.accept(out);
            }
        }
        IOUtils.fsync(file, false);
    }

    @Override
    public void writeBlobAtomic(
        OperationPurpose purpose,
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        assert purpose != OperationPurpose.SNAPSHOT_DATA && BlobContainer.assertPurposeConsistency(purpose, blobName) : purpose;
        final String tempBlob = tempBlobName(blobName);
        final Path tempBlobPath = path.resolve(tempBlob);
        try {
            writeToPath(inputStream, tempBlobPath, blobSize);
            moveBlobAtomic(purpose, tempBlob, blobName, failIfAlreadyExists);
        } catch (IOException ex) {
            try {
                deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(tempBlob));
            } catch (IOException e) {
                ex.addSuppressed(e);
            }
            throw ex;
        } finally {
            IOUtils.fsync(path, true);
        }
    }

    @Override
    public void writeBlobAtomic(OperationPurpose purpose, final String blobName, BytesReference bytes, boolean failIfAlreadyExists)
        throws IOException {
        assert purpose != OperationPurpose.SNAPSHOT_DATA && BlobContainer.assertPurposeConsistency(purpose, blobName) : purpose;
        final String tempBlob = tempBlobName(blobName);
        final Path tempBlobPath = path.resolve(tempBlob);
        try {
            writeToPath(bytes, tempBlobPath);
            moveBlobAtomic(purpose, tempBlob, blobName, failIfAlreadyExists);
        } catch (IOException ex) {
            try {
                deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(tempBlob));
            } catch (IOException e) {
                ex.addSuppressed(e);
            }
            throw ex;
        } finally {
            IOUtils.fsync(path, true);
        }
    }

    @Override
    public void copyBlob(OperationPurpose purpose, BlobContainer sourceBlobContainer, String sourceBlobName, String blobName, long blobSize)
        throws IOException {
        if (sourceBlobContainer instanceof FsBlobContainer == false) {
            throw new IllegalArgumentException("source blob container must be a FsBlobContainer");
        }
        final FsBlobContainer sourceContainer = (FsBlobContainer) sourceBlobContainer;
        final Path sourceBlobPath = sourceContainer.path.resolve(sourceBlobName);
        final String tempBlob = tempBlobName(blobName);
        final Path tempBlobPath = path.resolve(tempBlob);
        Files.copy(sourceBlobPath, tempBlobPath, StandardCopyOption.REPLACE_EXISTING);
        try {
            moveBlobAtomic(purpose, tempBlob, blobName, false);
        } catch (IOException ex) {
            try {
                deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(tempBlob));
            } catch (IOException e) {
                ex.addSuppressed(e);
            }
            throw ex;
        }
    }

    private static void writeToPath(BytesReference bytes, Path tempBlobPath) throws IOException {
        try (OutputStream outputStream = Files.newOutputStream(tempBlobPath, StandardOpenOption.CREATE_NEW)) {
            bytes.writeTo(outputStream);
        }
        IOUtils.fsync(tempBlobPath, false);
    }

    private void writeToPath(InputStream inputStream, Path tempBlobPath, long blobSize) throws IOException {
        try (OutputStream outputStream = Files.newOutputStream(tempBlobPath, StandardOpenOption.CREATE_NEW)) {
            final int bufferSize = blobStore.bufferSizeInBytes();
            long bytesWritten = org.elasticsearch.core.Streams.copy(
                inputStream,
                outputStream,
                new byte[blobSize < bufferSize ? Math.toIntExact(blobSize) : bufferSize]
            );
            assert bytesWritten == blobSize : "expected [" + blobSize + "] bytes but wrote [" + bytesWritten + "]";
        }
        IOUtils.fsync(tempBlobPath, false);
    }

    public void moveBlobAtomic(
        OperationPurpose purpose,
        final String sourceBlobName,
        final String targetBlobName,
        final boolean failIfAlreadyExists
    ) throws IOException {
        final Path sourceBlobPath = path.resolve(sourceBlobName);
        final Path targetBlobPath = path.resolve(targetBlobName);
        try {
            if (failIfAlreadyExists && Files.exists(targetBlobPath)) {
                throw new FileAlreadyExistsException("blob [" + targetBlobPath + "] already exists, cannot overwrite");
            }
            Files.move(sourceBlobPath, targetBlobPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // If the target file exists then Files.move() behaviour is implementation specific
            // the existing file might be replaced or this method fails by throwing an IOException so we retry in a non-atomic
            // way by deleting and then writing.
            if (failIfAlreadyExists) {
                throw e;
            }
            moveBlobNonAtomic(purpose, targetBlobName, sourceBlobPath, targetBlobPath, e);
        }
    }

    private void moveBlobNonAtomic(OperationPurpose purpose, String targetBlobName, Path sourceBlobPath, Path targetBlobPath, IOException e)
        throws IOException {
        try {
            deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(targetBlobName));
            Files.move(sourceBlobPath, targetBlobPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {
            ex.addSuppressed(e);
            throw e;
        }
    }

    public static String tempBlobName(final String blobName) {
        return TEMP_FILE_PREFIX + blobName + "-" + UUIDs.randomBase64UUID();
    }

    /**
     * Returns true if the blob is a leftover temporary blob.
     *
     * The temporary blobs might be left after failed atomic write operation.
     */
    public static boolean isTempBlobName(final String blobName) {
        return blobName.startsWith(TEMP_FILE_PREFIX);
    }

    private static OutputStream blobOutputStream(Path file) throws IOException {
        return Files.newOutputStream(file, StandardOpenOption.CREATE_NEW);
    }

    @Override
    public void getRegister(OperationPurpose purpose, String key, ActionListener<OptionalBytesReference> listener) {
        // no lock to acquire here, we are emulating the lack of read/read and read/write contention in cloud repositories
        ActionListener.completeWith(
            listener,
            () -> doUncontendedCompareAndExchangeRegister(path.resolve(key), BytesArray.EMPTY, BytesArray.EMPTY)
        );
    }

    @Override
    public void compareAndExchangeRegister(
        OperationPurpose purpose,
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<OptionalBytesReference> listener
    ) {
        ActionListener.completeWith(listener, () -> doCompareAndExchangeRegister(path.resolve(key), expected, updated));
    }

    private static final KeyedLock<Path> writeMutexes = new KeyedLock<>();

    private static OptionalBytesReference doCompareAndExchangeRegister(Path registerPath, BytesReference expected, BytesReference updated)
        throws IOException {
        // Emulate write/write contention as might happen in cloud repositories, at least for the case where the writers are all in this
        // JVM (e.g. for an ESIntegTestCase).
        try (var mutex = writeMutexes.tryAcquire(registerPath)) {
            return mutex == null
                ? OptionalBytesReference.MISSING
                : doUncontendedCompareAndExchangeRegister(registerPath, expected, updated);
        }
    }

    @SuppressForbidden(reason = "write to channel that we have open for locking purposes already directly")
    private static OptionalBytesReference doUncontendedCompareAndExchangeRegister(
        Path registerPath,
        BytesReference expected,
        BytesReference updated
    ) throws IOException {
        BlobContainerUtils.ensureValidRegisterContent(updated);
        try (LockedFileChannel lockedFileChannel = LockedFileChannel.open(registerPath)) {
            final FileChannel fileChannel = lockedFileChannel.fileChannel();
            final ByteBuffer readBuf = ByteBuffer.allocate(BlobContainerUtils.MAX_REGISTER_CONTENT_LENGTH);
            while (readBuf.remaining() > 0) {
                if (fileChannel.read(readBuf) == -1) {
                    break;
                }
            }
            final var found = new BytesArray(readBuf.array(), readBuf.arrayOffset(), readBuf.position());
            readBuf.clear();
            if (fileChannel.read(readBuf) != -1) {
                throw new IllegalStateException(
                    "register contains more than [" + BlobContainerUtils.MAX_REGISTER_CONTENT_LENGTH + "] bytes"
                );
            }

            if (expected.equals(found)) {
                var pageStart = 0L;
                final var iterator = updated.iterator();
                BytesRef page;
                while ((page = iterator.next()) != null) {
                    final var writeBuf = ByteBuffer.wrap(page.bytes, page.offset, page.length);
                    while (writeBuf.remaining() > 0) {
                        fileChannel.write(writeBuf, pageStart + writeBuf.position());
                    }
                    pageStart += page.length;
                }
                fileChannel.force(true);
            }
            return OptionalBytesReference.of(found);
        } catch (OverlappingFileLockException e) {
            assert false : e; // should be impossible, we protect against all concurrent operations within this JVM
            return OptionalBytesReference.MISSING;
        }
    }

    private record LockedFileChannel(FileChannel fileChannel, Closeable fileLock) implements Closeable {

        // Avoid concurrently opening/closing locked files, because this can trip an assertion within the JDK (see #93955 for details).
        // Perhaps it would work with finer-grained locks too, but we don't currently need to be fancy here.
        //
        // Also, avoid concurrent operations on FsBlobContainer registers within a single JVM with a simple blocking lock, to avoid
        // OverlappingFileLockException. FileChannel#lock blocks on concurrent operations on the file in a different process. This emulates
        // the lack of read/read and read/write contention that can happen on a cloud repository register.
        private static final ReentrantLock mutex = new ReentrantLock();

        static LockedFileChannel open(Path path) throws IOException {
            List<Closeable> resources = new ArrayList<>(3);
            try {
                mutex.lock();
                resources.add(mutex::unlock);

                final FileChannel fileChannel = openOrCreateAtomic(path);
                resources.add(fileChannel);

                final Closeable fileLock = fileChannel.lock()::close;
                resources.add(fileLock);

                final var result = new LockedFileChannel(fileChannel, fileLock);
                resources.clear();
                return result;
            } finally {
                IOUtils.closeWhileHandlingException(resources);
            }
        }

        private static FileChannel openOrCreateAtomic(Path path) throws IOException {
            try {
                if (Files.exists(path) == false) {
                    return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                }
            } catch (FileAlreadyExistsException e) {
                // ok, created concurrently by another process
            }
            return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(fileLock, fileChannel, mutex::unlock);
        }
    }
}
