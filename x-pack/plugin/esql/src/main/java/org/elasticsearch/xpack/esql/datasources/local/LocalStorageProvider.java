/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.local;

import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * StorageProvider implementation for local file system access.
 *
 * Features:
 * - Full file reads
 * - Range reads via RandomAccessFile
 * - Directory listing
 * - File metadata (size, last modified)
 *
 * This implementation is primarily for testing and development purposes.
 */
public final class LocalStorageProvider implements StorageProvider {

    /**
     * Creates a LocalStorageProvider.
     */
    public LocalStorageProvider() {
        // No configuration needed for local file system
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        validateFileScheme(path);
        return new LocalStorageObject(toFilePath(path));
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateFileScheme(path);
        return new LocalStorageObject(toFilePath(path), length);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateFileScheme(path);
        return new LocalStorageObject(toFilePath(path), length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath directory) throws IOException {
        validateFileScheme(directory);
        Path dirPath = toFilePath(directory);

        if (Files.exists(dirPath) == false) {
            throw new IOException("Directory does not exist: " + dirPath);
        }

        if (Files.isDirectory(dirPath) == false) {
            throw new IOException("Path is not a directory: " + dirPath);
        }

        return new LocalStorageIterator(dirPath);
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        validateFileScheme(path);
        Path filePath = toFilePath(path);
        return Files.exists(filePath);
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("file");
    }

    @Override
    public void close() throws IOException {
        // No resources to clean up for local file system
    }

    /**
     * Validates that the path uses the file:// scheme.
     */
    private void validateFileScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase();
        if (scheme.equals("file") == false) {
            throw new IllegalArgumentException("LocalStorageProvider only supports file:// scheme, got: " + scheme);
        }
    }

    /**
     * Converts a StoragePath to a java.nio.file.Path.
     * Handles both file://path and file:///path formats.
     */
    private Path toFilePath(StoragePath storagePath) {
        String pathStr = storagePath.path();

        // Handle file:// URLs - the path() method returns the path component after the scheme
        // For file:///absolute/path, path() returns "/absolute/path"
        // For file://relative/path, path() returns "relative/path"

        if (pathStr == null || pathStr.isEmpty()) {
            throw new IllegalArgumentException("Path cannot be empty for file:// scheme");
        }

        return Paths.get(pathStr);
    }

    @Override
    public String toString() {
        return "LocalStorageProvider{}";
    }

    /**
     * Iterator implementation for listing local directory contents.
     */
    private static final class LocalStorageIterator implements StorageIterator {
        private final Path directory;
        private final List<StorageEntry> entries;
        private final Iterator<StorageEntry> iterator;

        LocalStorageIterator(Path directory) throws IOException {
            this.directory = directory;
            this.entries = new ArrayList<>();

            // Read all entries into memory
            // For very large directories, this could be optimized to stream entries
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
                for (Path entry : stream) {
                    try {
                        BasicFileAttributes attrs = Files.readAttributes(entry, BasicFileAttributes.class);

                        // Skip directories, only include files
                        if (attrs.isRegularFile()) {
                            StoragePath storagePath = StoragePath.of("file://" + entry.toAbsolutePath());
                            long length = attrs.size();
                            Instant lastModified = attrs.lastModifiedTime().toInstant();

                            entries.add(new StorageEntry(storagePath, length, lastModified));
                        }
                    } catch (IOException e) {
                        // Skip entries that can't be read
                        // In production, consider logging this
                    }
                }
            }

            this.iterator = entries.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public StorageEntry next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void close() throws IOException {
            // No resources to clean up
        }
    }
}
