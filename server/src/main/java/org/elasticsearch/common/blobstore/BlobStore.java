/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.blobstore;

import org.elasticsearch.common.blobstore.BlobPath.Purpose;
import org.elasticsearch.core.Nullable;

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

    default void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames, @Nullable Purpose purpose) throws IOException {
        if (purpose == null) {
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
