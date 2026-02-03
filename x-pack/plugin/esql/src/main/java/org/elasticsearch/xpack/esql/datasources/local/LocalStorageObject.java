/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.local;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
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
public final class LocalStorageObject implements StorageObject {
    private final Path filePath;
    private final StoragePath storagePath;

    // Cached metadata to avoid repeated file system calls
    private Long cachedLength;
    private Instant cachedLastModified;
    private Boolean cachedExists;

    /**
     * Creates a LocalStorageObject without pre-known metadata.
     */
    public LocalStorageObject(Path filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("filePath cannot be null");
        }
        this.filePath = filePath;
        this.storagePath = StoragePath.of("file://" + filePath.toAbsolutePath());
    }

    /**
     * Creates a LocalStorageObject with pre-known length.
     */
    public LocalStorageObject(Path filePath, long length) {
        this(filePath);
        this.cachedLength = length;
    }

    /**
     * Creates a LocalStorageObject with pre-known length and last modified time.
     */
    public LocalStorageObject(Path filePath, long length, Instant lastModified) {
        this(filePath, length);
        this.cachedLastModified = lastModified;
    }

    @Override
    public InputStream newStream() throws IOException {
        if (Files.exists(filePath) == false) {
            throw new IOException("File does not exist: " + filePath);
        }

        if (Files.isRegularFile(filePath) == false) {
            throw new IOException("Path is not a regular file: " + filePath);
        }

        return new FileInputStream(filePath.toFile());
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative, got: " + position);
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative, got: " + length);
        }

        if (Files.exists(filePath) == false) {
            throw new IOException("File does not exist: " + filePath);
        }

        if (Files.isRegularFile(filePath) == false) {
            throw new IOException("Path is not a regular file: " + filePath);
        }

        // Use RandomAccessFile for efficient range reads
        return new RangeInputStream(filePath, position, length);
    }

    @Override
    public long length() throws IOException {
        if (cachedLength == null) {
            fetchMetadata();
        }
        return cachedLength;
    }

    @Override
    public Instant lastModified() throws IOException {
        if (cachedLastModified == null) {
            fetchMetadata();
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

    /**
     * Fetches file metadata and caches the results.
     */
    private void fetchMetadata() throws IOException {
        if (Files.exists(filePath)) {
            cachedExists = true;
            BasicFileAttributes attrs = Files.readAttributes(filePath, BasicFileAttributes.class);
            cachedLength = attrs.size();
            cachedLastModified = attrs.lastModifiedTime().toInstant();
        } else {
            cachedExists = false;
            cachedLength = 0L;
            cachedLastModified = null;
        }
    }

    /**
     * InputStream implementation for reading a specific range from a file.
     * Uses RandomAccessFile for efficient seeking and reading.
     */
    private static final class RangeInputStream extends InputStream {
        private final RandomAccessFile raf;
        private long remaining;

        RangeInputStream(Path filePath, long position, long length) throws IOException {
            this.raf = new RandomAccessFile(filePath.toFile(), "r");
            this.remaining = length;

            try {
                // Seek to the start position
                raf.seek(position);
            } catch (IOException e) {
                try {
                    raf.close();
                } catch (IOException closeEx) {
                    e.addSuppressed(closeEx);
                }
                throw e;
            }
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int b = raf.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int toRead = (int) Math.min(len, remaining);
            int bytesRead = raf.read(b, off, toRead);
            if (bytesRead > 0) {
                remaining -= bytesRead;
            }
            return bytesRead;
        }

        @Override
        public void close() throws IOException {
            raf.close();
        }

        @Override
        public long skip(long n) throws IOException {
            if (n <= 0) {
                return 0;
            }
            long toSkip = Math.min(n, remaining);
            long skipped = raf.skipBytes((int) toSkip);
            remaining -= skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return (int) Math.min(remaining, Integer.MAX_VALUE);
        }
    }
}
