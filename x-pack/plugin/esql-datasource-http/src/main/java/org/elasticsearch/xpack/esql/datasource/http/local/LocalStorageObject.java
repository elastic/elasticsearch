/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http.local;

import org.elasticsearch.xpack.esql.datasources.spi.AbstractMeteredStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;

/**
 * StorageObject implementation for local file system.
 *
 * Supports:
 * - Full file reads via FileInputStream
 * - Range reads via RandomAccessFile for columnar formats
 * - File metadata (size, last modified)
 */
public final class LocalStorageObject extends AbstractMeteredStorageObject {
    private final Path filePath;
    private final StoragePath storagePath;

    // Cached metadata to avoid repeated file system calls
    private Long cachedLength;
    private Instant cachedLastModified;
    private Boolean cachedExists;

    public LocalStorageObject(Path filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("filePath cannot be null");
        }
        this.filePath = filePath;
        // Use the shared canonical factory (not "file://" + toAbsolutePath) so the location is
        // normalized identically to every other producer of a local StoragePath (the directory
        // listing's toStoragePath, the query's location). On Windows toAbsolutePath() keeps
        // backslashes and yields a two-slash "file://C:\dir\file" form, whereas the factory
        // normalizes to "file:///C:/dir/file"; the mismatch made object.path() (the stats-capture
        // key) differ from the planning-side SchemaCacheKey canonicalPath, so
        // ExternalSourceCacheService.reconcileSourceStats silently dropped every captured
        // contribution and warm queries re-scanned. On POSIX both forms already coincide.
        this.storagePath = StoragePath.ofLocalPath(filePath);
    }

    public LocalStorageObject(Path filePath, long length) {
        this(filePath);
        this.cachedLength = length;
    }

    public LocalStorageObject(Path filePath, long length, Instant lastModified) {
        this(filePath, length);
        this.cachedLastModified = lastModified;
    }

    @Override
    public InputStream newStream() throws IOException {
        checkFileExists();
        if (Files.isRegularFile(filePath) == false) {
            throw new IOException("Path is not a regular file: " + filePath);
        }
        long startNanos = System.nanoTime();
        long bytes = 0L;
        try {
            InputStream stream = Files.newInputStream(filePath);
            if (cachedLength == null) {
                cachedLength = Files.size(filePath);
            }
            bytes = cachedLength;
            return stream;
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, bytes);
        }
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative, got: " + position);
        }
        if (length != READ_TO_END && length <= 0) {
            // Match the remote providers (S3/GCS/Azure/HTTP): a closed range must be positive; zero-length is
            // rejected rather than yielding an empty stream, so the precondition is uniform across providers.
            throw new IllegalArgumentException("length must be positive or READ_TO_END, got: " + length);
        }
        checkFileExists();
        if (Files.isRegularFile(filePath) == false) {
            throw new IOException("Path is not a regular file: " + filePath);
        }
        long startNanos = System.nanoTime();
        try {
            // READ_TO_END: read from position to the end of the file (no length() / size() lookup).
            return new RangeInputStream(filePath, position, length);
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, length < 0 ? 0L : length);
        }
    }

    /**
     * Reads directly into the target ByteBuffer using positional {@link FileChannel} I/O
     * via {@link org.elasticsearch.common.io.Channels#readFromFileChannel}.
     * Both heap and direct buffers are handled natively by the OS with no intermediate copies.
     */
    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        if (target.hasRemaining() == false) {
            return 0;
        }
        checkFileExists();
        long startNanos = System.nanoTime();
        long bytes = 0L;
        try (FileChannel ch = FileChannel.open(filePath, StandardOpenOption.READ)) {
            int startPos = target.position();
            org.elasticsearch.common.io.Channels.readFromFileChannel(ch, position, target);
            int bytesRead = target.position() - startPos;
            bytes = bytesRead;
            return bytesRead == 0 ? -1 : bytesRead;
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, bytes);
        }
    }

    @Override
    public long length() throws IOException {
        if (cachedLength == null) {
            fetchMetadata();
        }
        if (cachedExists == Boolean.FALSE) {
            throw new NoSuchFileException(filePath.toString());
        }
        return cachedLength;
    }

    @Override
    public Instant lastModified() throws IOException {
        if (cachedLastModified == null) {
            fetchMetadata();
        }
        if (cachedExists == Boolean.FALSE) {
            throw new NoSuchFileException(filePath.toString());
        }
        return cachedLastModified;
    }

    @Override
    public boolean exists() throws IOException {
        if (cachedExists == null) {
            fetchMetadata();
        }
        return cachedExists;
    }

    @Override
    public StoragePath path() {
        return storagePath;
    }

    private void checkFileExists() throws NoSuchFileException {
        if (Files.exists(filePath) == false) {
            throw new NoSuchFileException(filePath.toString());
        }
    }

    private void fetchMetadata() throws IOException {
        if (Files.exists(filePath)) {
            cachedExists = Boolean.TRUE;
            BasicFileAttributes attrs = Files.readAttributes(filePath, BasicFileAttributes.class);
            cachedLength = attrs.size();
            cachedLastModified = attrs.lastModifiedTime().toInstant();
        } else {
            cachedExists = Boolean.FALSE;
            cachedLength = 0L;
            cachedLastModified = null;
        }
    }

    /**
     * InputStream implementation for reading a specific range from a file.
     * Uses FileChannel for efficient seeking and reading (avoids forbidden RandomAccessFile).
     */
    private static final class RangeInputStream extends InputStream {
        private final FileChannel channel;
        private final InputStream delegate;
        // READ_TO_END (the only negative length newStream lets through) means read to EOF, no byte cap.
        private final boolean unbounded;
        private long remaining;

        RangeInputStream(Path filePath, long position, long length) throws IOException {
            this.unbounded = length < 0;
            this.remaining = length;
            boolean success = false;
            FileChannel ch = null;
            try {
                ch = FileChannel.open(filePath, StandardOpenOption.READ);
                ch.position(position);
                this.channel = ch;
                this.delegate = Channels.newInputStream(ch);
                success = true;
            } finally {
                if (success == false && ch != null) {
                    ch.close();
                }
            }
        }

        @Override
        public int read() throws IOException {
            if (unbounded == false && remaining <= 0) {
                return -1;
            }
            int b = delegate.read();
            if (b >= 0 && unbounded == false) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (unbounded == false && remaining <= 0) {
                return -1;
            }
            int toRead = unbounded ? len : (int) Math.min(len, remaining);
            int bytesRead = delegate.read(b, off, toRead);
            if (bytesRead > 0 && unbounded == false) {
                remaining -= bytesRead;
            }
            return bytesRead;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        @Override
        public long skip(long n) throws IOException {
            if (n <= 0) {
                return 0;
            }
            long toSkip = unbounded ? n : Math.min(n, remaining);
            long skipped = delegate.skip(toSkip);
            if (unbounded == false) {
                remaining -= skipped;
            }
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return unbounded ? delegate.available() : (int) Math.min(remaining, Integer.MAX_VALUE);
        }
    }
}
