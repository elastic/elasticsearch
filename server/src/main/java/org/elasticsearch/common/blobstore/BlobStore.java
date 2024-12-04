/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.blobstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * An interface for storing blobs.
 *
 * Creates a {@link BlobContainer} for each given {@link BlobPath} on demand in {@link #blobContainer(BlobPath)}.
 * In implementation/practice, BlobStore often returns a BlobContainer seeded with a reference to the BlobStore.
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository} holds and manages a BlobStore.
 */
public interface BlobStore extends Closeable {

    /**
     * Get a blob container instance for storing blobs at the given {@link BlobPath}.
     */
    BlobContainer blobContainer(BlobPath path);

    /**
     * Delete all the provided blobs from the blob store. Each blob could belong to a different {@code BlobContainer}
     *
     * @param purpose   the purpose of the delete operation
     * @param blobNames the blobs to be deleted
     */
    void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) throws IOException;

    /**
     * Returns statistics on the count of operations that have been performed on this blob store
     */
    default Map<String, BlobStoreActionStats> stats() {
        return Collections.emptyMap();
    }
}
