/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobContainerUtils;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
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
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
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
                    return new Iterator<>() {
                        @Override
                        public boolean hasNext() {
                            return false;
                        }

                        @Override
                        public Path next() {
                            return null;
                        }
                    };
                }

                @Override
                public void close() {}
            };
        }
    }

    @Override
    public DeleteResult delete() throws IOException {
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
    public void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames) throws IOException {
        IOException ioe = null;
        long suppressedExceptions = 0;
        while (blobNames.hasNext()) {
            try {
                Path resolve = path.resolve(blobNames.next());
                IOUtils.rm(resolve);
            } catch (IOException e) {
                // IOUtils.rm puts the original exception as a string in the IOException message. Ignore no such file exception.
                if (e.getMessage().contains("NoSuchFileException") == false) {
                    // track up to 10 delete exceptions and try to continue deleting on exceptions
                    if (ioe == null) {
                        ioe = e;
                    } else if (ioe.getSuppressed().length < 10) {
                        ioe.addSuppressed(e);
                    } else {
                        ++suppressedExceptions;
                    }
                }
            }
        }
        if (ioe != null) {
            if (suppressedExceptions > 0) {
                ioe.addSuppressed(new IOException("Failed to delete files, suppressed [" + suppressedExceptions + "] failures"));
            }
            throw ioe;
        }
    }

    @Override
    public boolean blobExists(String blobName) {
        return Files.exists(path.resolve(blobName));
    }

    @Override
    public InputStream readBlob(String name) throws IOException {
        final Path resolvedPath = path.resolve(name);
        try {
            return Files.newInputStream(resolvedPath);
        } catch (FileNotFoundException fnfe) {
            throw new NoSuchFileException("[" + name + "] blob not found");
        }
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        final SeekableByteChannel channel = Files.newByteChannel(path.resolve(blobName));
        if (position > 0L) {
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
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        final Path file = path.resolve(blobName);
        try {
            writeToPath(inputStream, file, blobSize);
        } catch (FileAlreadyExistsException faee) {
            if (failIfAlreadyExists) {
                throw faee;
            }
            deleteBlobsIgnoringIfNotExists(Iterators.single(blobName));
            writeToPath(inputStream, file, blobSize);
        }
        IOUtils.fsync(path, true);
    }

    @Override
    public void writeBlob(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        final Path file = path.resolve(blobName);
        try {
            writeToPath(bytes, file);
        } catch (FileAlreadyExistsException faee) {
            if (failIfAlreadyExists) {
                throw faee;
            }
            deleteBlobsIgnoringIfNotExists(Iterators.single(blobName));
            writeToPath(bytes, file);
        }
        IOUtils.fsync(path, true);
    }

    @Override
    public void writeMetadataBlob(
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
        if (atomic) {
            final String tempBlob = tempBlobName(blobName);
            try {
                writeToPath(tempBlob, true, writer);
                moveBlobAtomic(tempBlob, blobName, failIfAlreadyExists);
            } catch (IOException ex) {
                try {
                    deleteBlobsIgnoringIfNotExists(Iterators.single(tempBlob));
                } catch (IOException e) {
                    ex.addSuppressed(e);
                }
                throw ex;
            }
        } else {
            writeToPath(blobName, failIfAlreadyExists, writer);
        }
        IOUtils.fsync(path, true);
    }

    private void writeToPath(String blobName, boolean failIfAlreadyExists, CheckedConsumer<OutputStream, IOException> writer)
        throws IOException {
        final Path file = path.resolve(blobName);
        try {
            try (OutputStream out = blobOutputStream(file)) {
                writer.accept(out);
            }
        } catch (FileAlreadyExistsException faee) {
            if (failIfAlreadyExists) {
                throw faee;
            }
            deleteBlobsIgnoringIfNotExists(Iterators.single(blobName));
            try (OutputStream out = blobOutputStream(file)) {
                writer.accept(out);
            }
        }
        IOUtils.fsync(file, false);
    }

    @Override
    public void writeBlobAtomic(final String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        final String tempBlob = tempBlobName(blobName);
        final Path tempBlobPath = path.resolve(tempBlob);
        try {
            writeToPath(bytes, tempBlobPath);
            moveBlobAtomic(tempBlob, blobName, failIfAlreadyExists);
        } catch (IOException ex) {
            try {
                deleteBlobsIgnoringIfNotExists(Iterators.single(tempBlob));
            } catch (IOException e) {
                ex.addSuppressed(e);
            }
            throw ex;
        } finally {
            IOUtils.fsync(path, true);
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
            org.elasticsearch.core.Streams.copy(
                inputStream,
                outputStream,
                new byte[blobSize < bufferSize ? Math.toIntExact(blobSize) : bufferSize]
            );
        }
        IOUtils.fsync(tempBlobPath, false);
    }

    public void moveBlobAtomic(final String sourceBlobName, final String targetBlobName, final boolean failIfAlreadyExists)
        throws IOException {
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
            moveBlobNonAtomic(targetBlobName, sourceBlobPath, targetBlobPath, e);
        }
    }

    private void moveBlobNonAtomic(String targetBlobName, Path sourceBlobPath, Path targetBlobPath, IOException e) throws IOException {
        try {
            deleteBlobsIgnoringIfNotExists(Iterators.single(targetBlobName));
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
    @SuppressForbidden(reason = "write to channel that we have open for locking purposes already directly")
    public void compareAndExchangeRegister(
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<OptionalBytesReference> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            BlobContainerUtils.ensureValidRegisterContent(updated);
            try (LockedFileChannel lockedFileChannel = LockedFileChannel.open(path.resolve(key))) {
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
                return OptionalBytesReference.MISSING;
            }
        });
    }

    private record LockedFileChannel(FileChannel fileChannel, Closeable fileLock) implements Closeable {

        // Avoid concurrently opening/closing locked files, because this can trip an assertion within the JDK (see #93955 for details).
        // Perhaps it would work with finer-grained locks too, but we don't currently need to be fancy here.
        private static final Object mutex = new Object();

        static LockedFileChannel open(Path path) throws IOException {
            synchronized (mutex) {
                List<Closeable> resources = new ArrayList<>(2);
                try {
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
            synchronized (mutex) {
                IOUtils.close(fileLock, fileChannel);
            }
        }
    }
}
