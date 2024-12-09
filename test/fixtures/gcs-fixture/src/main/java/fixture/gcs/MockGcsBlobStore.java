/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.gcs;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestStatus;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockGcsBlobStore {

    private final ConcurrentMap<String, BlobVersion> blobs = new ConcurrentHashMap<>();

    record BlobVersion(String path, long generation, BytesReference contents, String uploadId) {}

    BlobVersion getBlob(String path, Long ifGenerationMatch) {
        final BlobVersion blob = blobs.get(path);
        if (blob == null) {
            throw new BlobNotFoundException(path);
        } else {
            if (ifGenerationMatch != null) {
                if (blob.generation != ifGenerationMatch) {
                    throw new GcsRestException(
                        RestStatus.PRECONDITION_FAILED,
                        "Generation mismatch, expected " + ifGenerationMatch + " but got " + blob.generation
                    );
                }
            }
            return blob;
        }
    }

    BlobVersion updateBlob(String path, Long ifGenerationMatch, BytesReference contents) {
        return updateBlob(path, ifGenerationMatch, contents, null);
    }

    BlobVersion updateBlob(String path, Long ifGenerationMatch, BytesReference contents, String uploadId) {
        return blobs.compute(path, (name, existing) -> {
            if (existing != null) {
                if (ifGenerationMatch != null) {
                    if (ifGenerationMatch == 0) {
                        throw new GcsRestException(
                            RestStatus.PRECONDITION_FAILED,
                            "Blob already exists at generation " + existing.generation
                        );
                    } else if (ifGenerationMatch != existing.generation) {
                        throw new GcsRestException(
                            RestStatus.PRECONDITION_FAILED,
                            "Generation mismatch, expected " + ifGenerationMatch + ", got" + existing.generation
                        );
                    }
                }
                return new BlobVersion(path, existing.generation + 1, contents, uploadId);
            } else {
                if (ifGenerationMatch != null && ifGenerationMatch != 0) {
                    throw new GcsRestException(
                        RestStatus.PRECONDITION_FAILED,
                        "Blob does not exist, expected generation " + ifGenerationMatch
                    );
                }
                return new BlobVersion(path, 1, contents, uploadId);
            }
        });
    }

    BlobVersion updateResumableBlob(String path, String uploadId, BytesReference newContents) {
        final BlobVersion blob = blobs.get(path);
        if (blob == null) {
            ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError("Tried to update a non-existent resumable: " + path));
        }
        if (Objects.equals(blob.uploadId, uploadId) == false) {
            ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError("Got a mismatched upload ID for resumable upload: " + path));
        }
        final BlobVersion updatedBlob = new BlobVersion(path, blob.generation, newContents, blob.uploadId);
        blobs.put(path, updatedBlob);
        return updatedBlob;
    }

    void deleteBlob(String path) {
        blobs.remove(path);
    }

    Map<String, BlobVersion> listBlobs() {
        return Map.copyOf(blobs);
    }

    static class BlobNotFoundException extends GcsRestException {

        BlobNotFoundException(String path) {
            super(RestStatus.NOT_FOUND, "Blob not found: " + path);
        }
    }

    static class GcsRestException extends RuntimeException {

        private final RestStatus status;

        GcsRestException(RestStatus status, String errorMessage) {
            super(errorMessage);
            this.status = status;
        }

        public RestStatus getStatus() {
            return status;
        }
    }
}
