/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.blobstore;

import org.elasticsearch.common.blobstore.BlobPath.Purpose;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * An interface for storing blobs.
 */
public interface BlobStore extends Closeable {

    /**
     * Get a blob container instance for storing blobs at the given {@link BlobPath}.
     */
    BlobContainer blobContainer(BlobPath path);

    /**
     * Delete all the provided blobs from the blob store. Each blob could belong to a different {@code BlobContainer}
     * @param blobNames the blobs to be deleted
     */
    @Deprecated(forRemoval = true)
    void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames) throws IOException;

    // TODO: Remove the default implementation and require each blob store to implement this method. Once it's done, remove the
    // the above overload version that does not take the Purpose parameter.
    /**
     * Delete all the provided blobs from the blob store. Each blob could belong to a different {@code BlobContainer}
     * @param blobNames the blobs to be deleted
     * @param purpose the purpose of the {@code BlobContainer} associated to the blobs to be deleted. It should be set
     *                to {@code Purpose.GENERIC}, if the blobs are from multiple {@code BlobContainer}s.
     */
    default void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames, Purpose purpose) throws IOException {
        if (purpose == Purpose.GENERIC) {
            deleteBlobsIgnoringIfNotExists(blobNames);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Returns statistics on the count of operations that have been performed on this blob store
     */
    default Map<String, Long> stats() {
        return Collections.emptyMap();
    }
}
