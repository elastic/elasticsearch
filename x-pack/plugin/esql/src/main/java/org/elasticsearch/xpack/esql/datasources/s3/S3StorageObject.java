/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

/**
 * StorageObject implementation for S3 using AWS SDK v2.
 * Uses standard Java InputStream - no custom stream classes needed.
 *
 * Supports:
 * - Full object reads via GetObject
 * - Range reads via GetObject with Range header for columnar formats
 * - Metadata retrieval via HeadObject
 */
public final class S3StorageObject implements StorageObject {
    private final S3Client s3Client;
    private final String bucket;
    private final String key;
    private final StoragePath path;

    // Cached metadata to avoid repeated HeadObject requests
    private Long cachedLength;
    private Instant cachedLastModified;
    private Boolean cachedExists;

    /**
     * Creates an S3StorageObject without pre-known metadata.
     */
    public S3StorageObject(S3Client s3Client, String bucket, String key, StoragePath path) {
        if (s3Client == null) {
            throw new IllegalArgumentException("s3Client cannot be null");
        }
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalArgumentException("bucket cannot be null or empty");
        }
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
        this.path = path;
    }

    /**
     * Creates an S3StorageObject with pre-known length.
     */
    public S3StorageObject(S3Client s3Client, String bucket, String key, StoragePath path, long length) {
        this(s3Client, bucket, key, path);
        this.cachedLength = length;
    }

    /**
     * Creates an S3StorageObject with pre-known length and last modified time.
     */
    public S3StorageObject(S3Client s3Client, String bucket, String key, StoragePath path, long length, Instant lastModified) {
        this(s3Client, bucket, key, path, length);
        this.cachedLastModified = lastModified;
    }

    @Override
    public InputStream newStream() throws IOException {
        // Full object read - no Range header
        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();

            ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);

            // Cache metadata from response if not already cached
            if (cachedLength == null) {
                cachedLength = response.response().contentLength();
            }
            if (cachedLastModified == null) {
                cachedLastModified = response.response().lastModified();
            }

            return response;
        } catch (NoSuchKeyException e) {
            throw new IOException("Object not found: " + path, e);
        } catch (Exception e) {
            throw new IOException("Failed to read object from " + path, e);
        }
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative, got: " + position);
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative, got: " + length);
        }

        // Range read using S3 Range header: "bytes=start-end" (inclusive)
        // S3 Range uses inclusive end, so we need position + length - 1
        long endPosition = position + length - 1;
        String rangeHeader = String.format("bytes=%d-%d", position, endPosition);

        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).range(rangeHeader).build();

            ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);

            // Cache metadata from response if not already cached
            if (cachedLength == null && response.response().contentLength() != null) {
                // Note: For range requests, contentLength is the range size, not the full object size
                // We need to get the full size from Content-Range header or HeadObject
                String contentRange = response.response().contentRange();
                if (contentRange != null && contentRange.contains("/")) {
                    String[] parts = contentRange.split("/");
                    if (parts.length == 2 && !parts[1].equals("*")) {
                        try {
                            cachedLength = Long.parseLong(parts[1]);
                        } catch (NumberFormatException ignored) {
                            // Ignore parsing errors
                        }
                    }
                }
            }
            if (cachedLastModified == null) {
                cachedLastModified = response.response().lastModified();
            }

            return response;
        } catch (NoSuchKeyException e) {
            throw new IOException("Object not found: " + path, e);
        } catch (Exception e) {
            throw new IOException("Range request failed for " + path, e);
        }
    }

    @Override
    public long length() throws IOException {
        if (cachedLength == null) {
            fetchMetadata();
        }
        // Throw exception if file doesn't exist instead of returning 0
        // This provides clearer error messages when files are missing
        if (cachedExists != null && cachedExists == false) {
            throw new IOException("Object not found: " + path);
        }
        return cachedLength;
    }

    @Override
    public Instant lastModified() throws IOException {
        if (cachedLastModified == null) {
            fetchMetadata();
        }
        return cachedLastModified;
    }

    @Override
    public boolean exists() throws IOException {
        if (cachedExists == null) {
            fetchMetadata();
        }
        return cachedExists;
    }

    @Override
    public StoragePath path() {
        return path;
    }

    /**
     * Fetches metadata via HeadObject and caches the results.
     */
    private void fetchMetadata() throws IOException {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(key).build();

            HeadObjectResponse response = s3Client.headObject(request);

            cachedExists = true;
            cachedLength = response.contentLength();
            cachedLastModified = response.lastModified();
        } catch (NoSuchKeyException e) {
            cachedExists = false;
            cachedLength = 0L;
            cachedLastModified = null;
        } catch (Exception e) {
            throw new IOException("HeadObject request failed for " + path, e);
        }
    }

    /**
     * Returns the S3 bucket name.
     */
    public String bucket() {
        return bucket;
    }

    /**
     * Returns the S3 object key.
     */
    public String key() {
        return key;
    }

    @Override
    public String toString() {
        return "S3StorageObject{bucket=" + bucket + ", key=" + key + ", path=" + path + "}";
    }
}
