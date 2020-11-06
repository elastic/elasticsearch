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
