/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http.local;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
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

    private static final String FILE_SCHEME_PREFIX = "file" + StoragePath.SCHEME_SEPARATOR;

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
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        validateFileScheme(prefix);
        Path dirPath = toFilePath(prefix);

        if (Files.exists(dirPath) == false) {
            throw new IOException("Directory does not exist: " + dirPath);
        }

        if (Files.isDirectory(dirPath) == false) {
            throw new IOException("Path is not a directory: " + dirPath);
        }

        return new LocalStorageIterator(dirPath, recursive);
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
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("file") == false) {
            throw new IllegalArgumentException("LocalStorageProvider only supports file:// scheme, got: " + scheme);
        }
    }

    /**
     * Converts a StoragePath to a java.nio.file.Path.
     * Handles both file://path and file:///path formats.
     */
    @SuppressForbidden(reason = "LocalStorageProvider converts user-supplied file:// URIs to Path objects")
    private Path toFilePath(StoragePath storagePath) {
        String pathStr = storagePath.path();

        // Handle file:// URLs - the path() method returns the path component after the scheme
        // For file:///absolute/path, path() returns "/absolute/path"
        // For file://relative/path, path() returns "relative/path"

        if (pathStr == null || pathStr.isEmpty()) {
            throw new IllegalArgumentException("Path cannot be empty for file:// scheme");
        }

        return PathUtils.get(pathStr);
    }

    @Override
    public String toString() {
        return "LocalStorageProvider{}";
    }

    private static StoragePath toStoragePath(Path filePath) {
        return StoragePath.of(FILE_SCHEME_PREFIX + filePath.toAbsolutePath());
    }

    /**
     * Iterator implementation for listing local directory contents.
     */
    private static final class LocalStorageIterator implements StorageIterator {
        private final List<StorageEntry> entries;
        private final Iterator<StorageEntry> iterator;

        LocalStorageIterator(Path directory, boolean recursive) throws IOException {
            this.entries = new ArrayList<>();

            if (recursive) {
                Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (attrs.isRegularFile()) {
                            StoragePath storagePath = toStoragePath(file);
                            entries.add(new StorageEntry(storagePath, attrs.size(), attrs.lastModifiedTime().toInstant()));
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc) {
                        // Skip entries that can't be read
                        return FileVisitResult.CONTINUE;
                    }
                });
            } else {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
                    for (Path entry : stream) {
                        try {
                            BasicFileAttributes attrs = Files.readAttributes(entry, BasicFileAttributes.class);
                            if (attrs.isRegularFile()) {
                                StoragePath storagePath = toStoragePath(entry);
                                entries.add(new StorageEntry(storagePath, attrs.size(), attrs.lastModifiedTime().toInstant()));
                            }
                        } catch (IOException e) {
                            // Skip entries that can't be read
                        }
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
