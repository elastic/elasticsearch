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
public final class DeleteResult {

    public static final DeleteResult ZERO = new DeleteResult(0, 0);

    private final long blobsDeleted;
    private final long bytesDeleted;

    public DeleteResult(long blobsDeleted, long bytesDeleted) {
        this.blobsDeleted = blobsDeleted;
        this.bytesDeleted = bytesDeleted;
    }

    public long blobsDeleted() {
        return blobsDeleted;
    }

    public long bytesDeleted() {
        return bytesDeleted;
    }

    public DeleteResult add(DeleteResult other) {
        return new DeleteResult(blobsDeleted + other.blobsDeleted(), bytesDeleted + other.bytesDeleted());
    }

    public DeleteResult add(long blobs, long bytes) {
        return new DeleteResult(blobsDeleted + blobs, bytesDeleted + bytes);
    }
}
