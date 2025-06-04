/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.gcs;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.fixture.HttpHeaderParser;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

public class MockGcsBlobStore {

    private static final int RESUME_INCOMPLETE = 308;
    // we use skip-list map so we can do paging right
    private final ConcurrentMap<String, BlobVersion> blobs = new ConcurrentSkipListMap<>();
    private final ConcurrentMap<String, ResumableUpload> resumableUploads = new ConcurrentHashMap<>();

    record BlobVersion(String path, long generation, BytesReference contents) {}

    record ResumableUpload(
        String uploadId,
        String path,
        Long ifGenerationMatch,
        BytesReference contents,
        Integer finalLength,
        boolean completed
    ) {

        ResumableUpload(String uploadId, String path, Long ifGenerationMatch) {
            this(uploadId, path, ifGenerationMatch, BytesArray.EMPTY, null, false);
        }

        public ResumableUpload update(BytesReference contents) {
            if (completed) {
                throw new IllegalStateException("Blob already completed");
            }
            return new ResumableUpload(uploadId, path, ifGenerationMatch, contents, null, false);
        }

        /**
         * When we complete, we nullify our reference to the contents to allow it to be collected if it gets overwritten
         */
        public ResumableUpload complete() {
            if (completed) {
                throw new IllegalStateException("Blob already completed");
            }
            return new ResumableUpload(uploadId, path, ifGenerationMatch, null, contents.length(), true);
        }

        public HttpHeaderParser.Range getRange() {
            int length = length();
            if (length > 0) {
                return new HttpHeaderParser.Range(0, length - 1);
            } else {
                return null;
            }
        }

        public int length() {
            if (finalLength != null) {
                return finalLength;
            }
            if (contents != null) {
                return contents.length();
            }
            return 0;
        }
    }

    /**
     * Get the blob at the specified path
     *
     * @param path The path
     * @param ifGenerationMatch The ifGenerationMatch parameter value (if present)
     * @param generation The generation parameter value (if present)
     * @return The blob if it exists
     * @throws BlobNotFoundException if there is no blob at the path, or its generation does not match the generation parameter
     * @throws GcsRestException if the blob's generation does not match the ifGenerationMatch parameter
     */
    BlobVersion getBlob(String path, Long ifGenerationMatch, Long generation) {
        final BlobVersion blob = blobs.get(path);
        if (blob == null) {
            throw new BlobNotFoundException(path);
        }
        if (generation != null && generation != blob.generation) {
            throw new BlobNotFoundException(blob.path, blob.generation);
        }
        if (ifGenerationMatch != null && ifGenerationMatch != blob.generation) {
            throw new GcsRestException(
                RestStatus.PRECONDITION_FAILED,
                "Generation mismatch, expected " + ifGenerationMatch + " but got " + blob.generation
            );
        }
        return blob;
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
        final ResumableUpload value = new ResumableUpload(uploadId, path, ifGenerationMatch);
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

            ResumableUpload valueToReturn = existing;

            // Handle the request, a range indicates a chunk of data was submitted
            if (contentRange.hasRange()) {
                if (existing.completed) {
                    throw failAndThrow("Attempted to write more to a completed resumable upload");
                }

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
                valueToReturn = existing.update(updatedContent);
            }

            // Next we determine the response
            if (valueToReturn.completed) {
                updateResponse.set(new UpdateResponse(RestStatus.OK.getStatus(), valueToReturn.getRange(), valueToReturn.length()));
            } else if (contentRange.hasSize() && contentRange.size() == valueToReturn.contents.length()) {
                updateBlob(valueToReturn.path(), valueToReturn.ifGenerationMatch(), valueToReturn.contents);
                valueToReturn = valueToReturn.complete();
                updateResponse.set(new UpdateResponse(RestStatus.OK.getStatus(), valueToReturn.getRange(), valueToReturn.length()));
            } else {
                updateResponse.set(new UpdateResponse(RESUME_INCOMPLETE, valueToReturn.getRange(), valueToReturn.length()));
            }
            return valueToReturn;
        });
        assert updateResponse.get() != null : "Should always produce an update response";
        return updateResponse.get();
    }

    record UpdateResponse(int statusCode, HttpHeaderParser.Range rangeHeader, long storedContentLength) {}

    void deleteBlob(String path) {
        blobs.remove(path);
    }

    private String stripPrefixIfPresent(@Nullable String prefix, String toStrip) {
        if (prefix != null && toStrip.startsWith(prefix)) {
            return toStrip.substring(prefix.length());
        }
        return toStrip;
    }

    PageOfBlobs listBlobs(String pageToken) {
        final PageToken parsedToken = PageToken.fromString(pageToken);
        return calculatePageOfBlobs(parsedToken);
    }

    /**
     * Calculate the requested page of blobs taking into account the request parameters
     *
     * @see <a href="https://cloud.google.com/storage/docs/json_api/v1/objects/list">Description of prefix/delimiter</a>
     * @see <a href="https://cloud.google.com/storage/docs/json_api/v1/objects/list#parameters">List objects parameters</a>
     * @param pageToken The token containing the prefix/delimiter/maxResults/pageNumber parameters
     * @return The filtered list
     */
    private PageOfBlobs calculatePageOfBlobs(PageToken pageToken) {
        final String previousBlob = pageToken.previousBlob();
        final int maxResults = pageToken.maxResults();
        final String prefix = pageToken.prefix();
        final String delimiter = pageToken.delimiter();
        final SortedSet<String> prefixes = new TreeSet<>();
        final List<BlobVersion> matchingBlobs = new ArrayList<>();
        String lastBlobPath = null;
        for (BlobVersion blob : blobs.values()) {
            if (Strings.hasLength(previousBlob) && previousBlob.compareTo(blob.path()) >= 0) {
                continue;
            }
            if (blob.path().startsWith(prefix)) {
                final String pathWithoutPrefix = stripPrefixIfPresent(prefix, blob.path());
                if (Strings.hasLength(delimiter) && pathWithoutPrefix.contains(delimiter)) {
                    // This seems counter to what is described in the example at the top of
                    // https://cloud.google.com/storage/docs/json_api/v1/objects/list,
                    // but it's required to make the third party tests pass
                    prefixes.add(prefix + pathWithoutPrefix.substring(0, pathWithoutPrefix.indexOf(delimiter) + 1));
                } else {
                    matchingBlobs.add(blob);
                }
            }
            lastBlobPath = blob.path();
            if (prefixes.size() + matchingBlobs.size() == maxResults) {
                return new PageOfBlobs(
                    new PageToken(prefix, delimiter, maxResults, previousBlob),
                    new ArrayList<>(prefixes),
                    matchingBlobs,
                    lastBlobPath,
                    false
                );
            }
        }
        return new PageOfBlobs(
            new PageToken(prefix, delimiter, maxResults, previousBlob),
            new ArrayList<>(prefixes),
            matchingBlobs,
            lastBlobPath,
            true
        );
    }

    PageOfBlobs listBlobs(int maxResults, String delimiter, String prefix) {
        final PageToken pageToken = new PageToken(prefix, delimiter, maxResults, "");
        return calculatePageOfBlobs(pageToken);
    }

    List<BlobVersion> listBlobs() {
        return new ArrayList<>(blobs.values());
    }

    /**
     * We serialise this as a tuple with base64 encoded components so we don't need to escape the delimiter
     */
    record PageToken(String prefix, String delimiter, int maxResults, String previousBlob) {
        public static PageToken fromString(String pageToken) {
            final String[] parts = pageToken.split("\\.");
            assert parts.length == 4;
            return new PageToken(decode(parts[0]), decode(parts[1]), Integer.parseInt(decode(parts[2])), decode(parts[3]));
        }

        public String toString() {
            return encode(prefix) + "." + encode(delimiter) + "." + encode(String.valueOf(maxResults)) + "." + encode(previousBlob);
        }

        public PageToken nextPageToken(String previousBlob) {
            return new PageToken(prefix, delimiter, maxResults, previousBlob);
        }

        private static String encode(String value) {
            return Base64.getEncoder().encodeToString(value.getBytes());
        }

        private static String decode(String value) {
            return new String(Base64.getDecoder().decode(value));
        }
    }

    record PageOfBlobs(PageToken pageToken, List<String> prefixes, List<BlobVersion> blobs, String lastBlobIncluded, boolean lastPage) {

        public boolean isLastPage() {
            return lastPage;
        }

        public String nextPageToken() {
            return isLastPage() ? null : pageToken.nextPageToken(lastBlobIncluded).toString();
        }
    }

    static class BlobNotFoundException extends GcsRestException {

        BlobNotFoundException(String path) {
            super(RestStatus.NOT_FOUND, "Blob not found: " + path);
        }

        BlobNotFoundException(String path, long generation) {
            super(RestStatus.NOT_FOUND, "Blob not found: " + path + ", generation " + generation);
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
