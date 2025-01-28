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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.fixture.HttpHeaderParser;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class MockGcsBlobStore {

    private static final int RESUME_INCOMPLETE = 308;
    private final ConcurrentMap<String, BlobVersion> blobs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ResumableUpload> resumableUploads = new ConcurrentHashMap<>();

    record BlobVersion(String path, long generation, BytesReference contents) {}

    record ResumableUpload(String uploadId, String path, Long ifGenerationMatch, BytesReference contents, boolean completed) {

        public ResumableUpload update(BytesReference contents, boolean completed) {
            return new ResumableUpload(uploadId, path, ifGenerationMatch, contents, completed);
        }
    }

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
                return new BlobVersion(path, existing.generation + 1, contents);
            } else {
                if (ifGenerationMatch != null && ifGenerationMatch != 0) {
                    throw new GcsRestException(
                        RestStatus.PRECONDITION_FAILED,
                        "Blob does not exist, expected generation " + ifGenerationMatch
                    );
                }
                return new BlobVersion(path, 1, contents);
            }
        });
    }

    ResumableUpload createResumableUpload(String path, Long ifGenerationMatch) {
        final String uploadId = UUIDs.randomBase64UUID();
        final ResumableUpload value = new ResumableUpload(uploadId, path, ifGenerationMatch, BytesArray.EMPTY, false);
        resumableUploads.put(uploadId, value);
        return value;
    }

    /**
     * Update or query a resumable upload
     *
     * @see <a href="https://cloud.google.com/storage/docs/resumable-uploads">GCS Resumable Uploads</a>
     * @param uploadId The upload ID
     * @param contentRange The range being submitted
     * @param requestBody The data for that range
     * @return The response to the request
     */
    UpdateResponse updateResumableUpload(String uploadId, HttpHeaderParser.ContentRange contentRange, BytesReference requestBody) {
        final AtomicReference<UpdateResponse> updateResponse = new AtomicReference<>();
        resumableUploads.compute(uploadId, (uid, existing) -> {
            if (existing == null) {
                throw failAndThrow("Attempted to update a non-existent resumable: " + uid);
            }

            if (contentRange.hasRange() == false) {
                // Content-Range: */... is a status check https://cloud.google.com/storage/docs/performing-resumable-uploads#status-check
                if (existing.completed) {
                    updateResponse.set(new UpdateResponse(RestStatus.OK.getStatus(), calculateRangeHeader(blobs.get(existing.path))));
                } else {
                    final HttpHeaderParser.Range range = calculateRangeHeader(existing);
                    updateResponse.set(new UpdateResponse(RESUME_INCOMPLETE, range));
                }
                return existing;
            } else {
                if (contentRange.start() > contentRange.end()) {
                    throw failAndThrow("Invalid content range " + contentRange);
                }
                if (contentRange.start() > existing.contents.length()) {
                    throw failAndThrow(
                        "Attempted to append after the end of the current content: size="
                            + existing.contents.length()
                            + ", start="
                            + contentRange.start()
                    );
                }
                if (contentRange.end() < existing.contents.length()) {
                    throw failAndThrow("Attempted to upload no new data");
                }
                final int offset = Math.toIntExact(existing.contents.length() - contentRange.start());
                final BytesReference updatedContent = CompositeBytesReference.of(
                    existing.contents,
                    requestBody.slice(offset, requestBody.length())
                );
                // We just received the last chunk, update the blob and remove the resumable upload from the map
                if (contentRange.hasSize() && updatedContent.length() == contentRange.size()) {
                    updateBlob(existing.path(), existing.ifGenerationMatch, updatedContent);
                    updateResponse.set(new UpdateResponse(RestStatus.OK.getStatus(), null));
                    return existing.update(BytesArray.EMPTY, true);
                }
                final ResumableUpload updated = existing.update(updatedContent, false);
                updateResponse.set(new UpdateResponse(RESUME_INCOMPLETE, calculateRangeHeader(updated)));
                return updated;
            }
        });
        assert updateResponse.get() != null : "Should always produce an update response";
        return updateResponse.get();
    }

    private static HttpHeaderParser.Range calculateRangeHeader(ResumableUpload resumableUpload) {
        return resumableUpload.contents.length() > 0 ? new HttpHeaderParser.Range(0, resumableUpload.contents.length() - 1) : null;
    }

    private static HttpHeaderParser.Range calculateRangeHeader(BlobVersion blob) {
        return blob.contents.length() > 0 ? new HttpHeaderParser.Range(0, blob.contents.length() - 1) : null;
    }

    record UpdateResponse(int statusCode, HttpHeaderParser.Range rangeHeader) {}

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

    /**
     * Fail the test with an assertion error and throw an exception in-line
     *
     * @param message The message to use on the {@link Throwable}s
     * @return nothing, but claim to return an exception to help with static analysis
     */
    public static RuntimeException failAndThrow(String message) {
        ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError(message));
        throw new IllegalStateException(message);
    }
}
