/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.blobstore.fs;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.core.internal.io.Streams;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
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
import java.util.Collections;
import java.util.HashMap;
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
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, blobNamePrefix + "*")) {
            for (Path file : stream) {
                final BasicFileAttributes attrs;
                try {
                    attrs = Files.readAttributes(file, BasicFileAttributes.class);
                } catch (FileNotFoundException | NoSuchFileException e) {
                    // The file was concurrently deleted between listing files and trying to get its attributes so we skip it here
                    continue;
                }
                if (attrs.isRegularFile()) {
                    builder.put(file.getFileName().toString(), new PlainBlobMetadata(file.getFileName().toString(), attrs.size()));
                }
            }
        }
        return unmodifiableMap(builder);
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
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        IOUtils.rm(blobNames.stream().map(path::resolve).toArray(Path[]::new));
    }

    private InputStream bufferedInputStream(InputStream inputStream) {
        return new BufferedInputStream(inputStream, blobStore.bufferSizeInBytes());
    }

    @Override
    public InputStream readBlob(String name) throws IOException {
        final Path resolvedPath = path.resolve(name);
        try {
            return bufferedInputStream(Files.newInputStream(resolvedPath));
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
        return bufferedInputStream(org.elasticsearch.common.io.Streams.limitStream(Channels.newInputStream(channel), length));
    }

    @Override
    public long readBlobPreferredLength() {
        // This container returns streams that are cheap to close early, so we can tell consumers to request as much data as possible.
        return Long.MAX_VALUE;
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        if (failIfAlreadyExists == false) {
            deleteBlobsIgnoringIfNotExists(Collections.singletonList(blobName));
        }
        final Path file = path.resolve(blobName);
        try (OutputStream outputStream = Files.newOutputStream(file, StandardOpenOption.CREATE_NEW)) {
            Streams.copy(inputStream, outputStream);
        }
        IOUtils.fsync(file, false);
        IOUtils.fsync(path, true);
    }

    @Override
    public void writeBlobAtomic(final String blobName, final InputStream inputStream, final long blobSize, boolean failIfAlreadyExists)
        throws IOException {
        final String tempBlob = tempBlobName(blobName);
        final Path tempBlobPath = path.resolve(tempBlob);
        try {
            try (OutputStream outputStream = Files.newOutputStream(tempBlobPath, StandardOpenOption.CREATE_NEW)) {
                Streams.copy(inputStream, outputStream);
            }
            IOUtils.fsync(tempBlobPath, false);
            moveBlobAtomic(tempBlob, blobName, failIfAlreadyExists);
        } catch (IOException ex) {
            try {
                deleteBlobsIgnoringIfNotExists(Collections.singletonList(tempBlob));
            } catch (IOException e) {
                ex.addSuppressed(e);
            }
            throw ex;
        } finally {
            IOUtils.fsync(path, true);
        }
    }

    public void moveBlobAtomic(final String sourceBlobName, final String targetBlobName, final boolean failIfAlreadyExists)
        throws IOException {
        final Path sourceBlobPath = path.resolve(sourceBlobName);
        final Path targetBlobPath = path.resolve(targetBlobName);
        // If the target file exists then Files.move() behaviour is implementation specific
        // the existing file might be replaced or this method fails by throwing an IOException.
        if (Files.exists(targetBlobPath)) {
            if (failIfAlreadyExists) {
                throw new FileAlreadyExistsException("blob [" + targetBlobPath + "] already exists, cannot overwrite");
            } else {
                deleteBlobsIgnoringIfNotExists(Collections.singletonList(targetBlobName));
            }
        }
        Files.move(sourceBlobPath, targetBlobPath, StandardCopyOption.ATOMIC_MOVE);
    }

    public static String tempBlobName(final String blobName) {
        return "pending-" + blobName + "-" + UUIDs.randomBase64UUID();
    }

    /**
     * Returns true if the blob is a leftover temporary blob.
     *
     * The temporary blobs might be left after failed atomic write operation.
     */
    public static boolean isTempBlobName(final String blobName) {
        return blobName.startsWith(TEMP_FILE_PREFIX);
    }
}
