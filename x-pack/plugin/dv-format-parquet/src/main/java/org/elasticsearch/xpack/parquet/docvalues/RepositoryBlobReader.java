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
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads byte ranges of a Parquet file through the {@link BlobContainer} of a registered repository, so that credentials, retries
 * and client settings stay in the repository and this plugin needs no cloud SDK of its own.
 */
public final class RepositoryBlobReader {

    private RepositoryBlobReader() {}

    /**
     * The container for a path of the named repository.
     *
     * @throws IllegalArgumentException if the repository does not exist or is not backed by a blob store
     */
    public static BlobContainer container(RepositoriesService repositories, ProjectId projectId, String repositoryName, BlobPath path) {
        Repository repository = repositories.repository(projectId, repositoryName);
        if (repository instanceof BlobStoreRepository blobStoreRepository) {
            return blobStoreRepository.blobStore().blobContainer(path);
        }
        throw new IllegalArgumentException("repository [" + repositoryName + "] is not backed by a blob store");
    }

    /** Reads exactly {@code length} bytes of the blob starting at {@code position}. */
    public static byte[] readRange(BlobContainer container, String blobName, long position, int length) throws IOException {
        try (InputStream in = container.readBlob(OperationPurpose.SNAPSHOT_DATA, blobName, position, length)) {
            return in.readNBytes(length);
        }
    }
}
