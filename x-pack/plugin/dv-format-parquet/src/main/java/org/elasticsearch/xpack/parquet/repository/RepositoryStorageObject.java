/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.repository;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;

/**
 * A Parquet file in the blob store of a registered repository, as the ES|QL reader sees a storage object. It reads through the
 * {@link BlobContainer} of the repository, so credentials, retries and client settings stay in the repository, and the file must be
 * below the base path of the repository.
 */
public final class RepositoryStorageObject implements StorageObject {

    private final RepositoryLocation location;
    private final BlobContainer container;
    private final String blobName;
    private final long length;
    /**
     * Footer cache entries are keyed by this identity, the path, the length and the version. One identity for each repository keeps
     * the files of different repositories apart, and the credentials are the repository's own.
     */
    private final StorageIdentity identity;

    private RepositoryStorageObject(RepositoryLocation location, BlobContainer container, String blobName, long length) {
        this.location = location;
        this.container = container;
        this.blobName = blobName;
        this.length = length;
        this.identity = new RepositoryIdentity(location.repository());
    }

    private record RepositoryIdentity(String repository) implements StorageIdentity {}

    /**
     * Opens a file whose length is known, for example from the segment that references it, without a request.
     *
     * @throws IllegalArgumentException if the repository does not exist or is not backed by a blob store
     */
    public static RepositoryStorageObject open(
        RepositoriesService repositories,
        ProjectId projectId,
        RepositoryLocation location,
        long knownLength
    ) {
        return open(repositories, projectId, location, OptionalLong.of(knownLength));
    }

    /**
     * Opens the file and finds out its length with one listing of the directory of the file.
     *
     * @throws IllegalArgumentException if the repository does not exist or is not backed by a blob store
     * @throws UncheckedIOException     around a {@link NoSuchFileException} if the file does not exist
     */
    public static RepositoryStorageObject open(RepositoriesService repositories, ProjectId projectId, RepositoryLocation location) {
        return open(repositories, projectId, location, OptionalLong.empty());
    }

    private static RepositoryStorageObject open(
        RepositoriesService repositories,
        ProjectId projectId,
        RepositoryLocation location,
        OptionalLong knownLength
    ) {
        Repository repository = repositories.repository(projectId, location.repository());
        if (repository instanceof BlobStoreRepository blobStoreRepository) {
            List<String> segments = location.segments();
            BlobPath directory = blobStoreRepository.basePath();
            for (String segment : segments.subList(0, segments.size() - 1)) {
                directory = directory.add(segment);
            }
            BlobContainer container = blobStoreRepository.blobStore().blobContainer(directory);
            String blobName = segments.get(segments.size() - 1);
            long length = knownLength.isPresent() ? knownLength.getAsLong() : listLength(container, blobName, location);
            return new RepositoryStorageObject(location, container, blobName, length);
        }
        throw new IllegalArgumentException("repository [" + location.repository() + "] is not backed by a blob store");
    }

    private static long listLength(BlobContainer container, String blobName, RepositoryLocation location) {
        try {
            BlobMetadata metadata = container.listBlobsByPrefix(OperationPurpose.SNAPSHOT_DATA, blobName).get(blobName);
            if (metadata == null) {
                throw new NoSuchFileException(location.toString());
            }
            return metadata.length();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public StorageIdentity storageIdentity() {
        return identity;
    }

    @Override
    public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

    @Override
    public InputStream newStream() throws IOException {
        return newStream(0, READ_TO_END);
    }

    @Override
    public InputStream newStream(long position, long readLength) throws IOException {
        long end = readLength == READ_TO_END ? length : Math.min(length, position + readLength);
        long count = end - position;
        if (position < 0 || count < 0) {
            throw new IOException("range [" + position + ", +" + readLength + ") is outside of [" + location + "] of length " + length);
        }
        return container.readBlob(OperationPurpose.SNAPSHOT_DATA, blobName, position, count);
    }

    @Override
    public long length() {
        return length;
    }

    /** The repository does not report a modification time through the blob container, and the length pins the file for the segment. */
    @Override
    public Instant lastModified() {
        return Instant.EPOCH;
    }

    /**
     * The reader caches parsed footers under a key that includes the version, and an object without one is never cached. The length
     * is what the segment pins the file with, so it is the version too.
     */
    @Override
    public String version() {
        return Long.toString(length);
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public StoragePath path() {
        return StoragePath.of(location.toString());
    }
}
