/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.OptionalLong;

/**
 * A Parquet file in the blob store of a registered repository, read through its {@link BlobContainer}. Credentials, retries and client
 * settings stay in the repository, and the file must be below the base path of the repository.
 */
public final class RepositoryParquetObject implements ParquetObject {

    private final RepositoryLocation location;
    private final BlobContainer container;
    private final String blobName;
    private final long length;

    private RepositoryParquetObject(RepositoryLocation location, BlobContainer container, String blobName, long length) {
        this.location = location;
        this.container = container;
        this.blobName = blobName;
        this.length = length;
    }

    /**
     * Opens a file whose length is known, for example from the segment that references it, without a request.
     *
     * @throws IllegalArgumentException if the repository does not exist or is not backed by a blob store
     */
    public static RepositoryParquetObject open(
        RepositoriesService repositories,
        ProjectId projectId,
        RepositoryLocation location,
        long knownLength
    ) {
        return open(repositories, projectId, location, OptionalLong.of(knownLength));
    }

    /**
     * Opens the file, which finds out its length with one listing of the directory of the file.
     *
     * @throws IllegalArgumentException if the repository does not exist or is not backed by a blob store
     * @throws NoSuchFileException if the file does not exist
     */
    public static RepositoryParquetObject open(RepositoriesService repositories, ProjectId projectId, RepositoryLocation location)
        throws IOException {
        return open(repositories, projectId, location, OptionalLong.empty());
    }

    private static RepositoryParquetObject open(
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
            return new RepositoryParquetObject(location, container, blobName, length);
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
    public String location() {
        return location.toString();
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte[] readRange(long position, int length) throws IOException {
        if (position < 0 || length < 0 || position + length > this.length) {
            throw new EOFException("range [" + position + ", +" + length + ") is outside of [" + location + "] of length " + this.length);
        }
        try (InputStream in = container.readBlob(OperationPurpose.SNAPSHOT_DATA, blobName, position, length)) {
            byte[] bytes = in.readNBytes(length);
            if (bytes.length != length) {
                throw new EOFException(
                    "read [" + bytes.length + "] bytes of [" + location + "] at [" + position + "], expected [" + length + "]"
                );
            }
            return bytes;
        }
    }
}
