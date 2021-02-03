/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.blobstore;

import java.io.Closeable;
import java.util.Collections;
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
     * Returns statistics on the count of operations that have been performed on this blob store
     */
    default Map<String, Long> stats() {
        return Collections.emptyMap();
    }
}
