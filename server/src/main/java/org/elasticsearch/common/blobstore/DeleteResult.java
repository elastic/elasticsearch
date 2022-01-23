/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore;

/**
 * The result of deleting multiple blobs from a {@link BlobStore}.
 */
public record DeleteResult(long blobsDeleted, long bytesDeleted) {

    public static final DeleteResult ZERO = new DeleteResult(0, 0);

    public DeleteResult add(DeleteResult other) {
        return new DeleteResult(blobsDeleted + other.blobsDeleted(), bytesDeleted + other.bytesDeleted());
    }

    public DeleteResult add(long blobs, long bytes) {
        return new DeleteResult(blobsDeleted + blobs, bytesDeleted + bytes);
    }
}
