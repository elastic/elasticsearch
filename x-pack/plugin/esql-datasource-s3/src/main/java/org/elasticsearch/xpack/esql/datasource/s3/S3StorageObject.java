/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;

/**
 * StorageObject implementation for S3 using AWS SDK v2.
 * Supports full and range reads, metadata retrieval, and optional native async via S3AsyncClient.
 */
public final class S3StorageObject implements StorageObject {
    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final String bucket;
    private final String key;
    private final StoragePath path;

    private Long cachedLength;
    private Instant cachedLastModified;
    private Boolean cachedExists;

    public S3StorageObject(S3Client s3Client, String bucket, String key, StoragePath path) {
        this(s3Client, null, bucket, key, path);
    }

    public S3StorageObject(S3Client s3Client, S3AsyncClient s3AsyncClient, String bucket, String key, StoragePath path) {
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
        this.s3AsyncClient = s3AsyncClient;
        this.bucket = bucket;
        this.key = key;
        this.path = path;
    }

    public S3StorageObject(S3Client s3Client, String bucket, String key, StoragePath path, long length) {
        this(s3Client, bucket, key, path);
        this.cachedLength = length;
    }

    public S3StorageObject(S3Client s3Client, S3AsyncClient s3AsyncClient, String bucket, String key, StoragePath path, long length) {
        this(s3Client, s3AsyncClient, bucket, key, path);
        this.cachedLength = length;
    }

    public S3StorageObject(S3Client s3Client, String bucket, String key, StoragePath path, long length, Instant lastModified) {
        this(s3Client, bucket, key, path, length);
        this.cachedLastModified = lastModified;
    }

    public S3StorageObject(
        S3Client s3Client,
        S3AsyncClient s3AsyncClient,
        String bucket,
        String key,
        StoragePath path,
        long length,
        Instant lastModified
    ) {
        this(s3Client, s3AsyncClient, bucket, key, path, length);
        this.cachedLastModified = lastModified;
    }

    @Override
    public InputStream newStream() throws IOException {
        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
            ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);

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

        long endPosition = position + length - 1;
        String rangeHeader = Strings.format("bytes=%d-%d", position, endPosition);

        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).range(rangeHeader).build();
            ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);

            if (cachedLength == null && response.response().contentLength() != null) {
                String contentRange = response.response().contentRange();
                if (contentRange != null && contentRange.contains("/")) {
                    String[] parts = contentRange.split("/");
                    if (parts.length == 2 && parts[1].equals("*") == false) {
                        try {
                            cachedLength = Long.parseLong(parts[1]);
                        } catch (NumberFormatException ignored) {}
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

    public String bucket() {
        return bucket;
    }

    public String key() {
        return key;
    }

    @Override
    public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
        if (s3AsyncClient == null) {
            StorageObject.super.readBytesAsync(position, length, executor, listener);
            return;
        }

        if (position < 0) {
            listener.onFailure(new IllegalArgumentException("position must be non-negative, got: " + position));
            return;
        }
        if (length < 0) {
            listener.onFailure(new IllegalArgumentException("length must be non-negative, got: " + length));
            return;
        }

        long endPosition = position + length - 1;
        String rangeHeader = Strings.format("bytes=%d-%d", position, endPosition);

        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).range(rangeHeader).build();

        s3AsyncClient.getObject(request, AsyncResponseTransformer.toBytes()).whenComplete((responseBytes, throwable) -> {
            if (throwable != null) {
                Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;
                if (cause instanceof NoSuchKeyException) {
                    listener.onFailure(new IOException("Object not found: " + path, cause));
                } else {
                    listener.onFailure(cause instanceof Exception ex ? ex : new RuntimeException(cause));
                }
                return;
            }

            GetObjectResponse response = responseBytes.response();
            if (cachedLastModified == null) {
                cachedLastModified = response.lastModified();
            }
            if (cachedLength == null) {
                String contentRange = response.contentRange();
                if (contentRange != null && contentRange.contains("/")) {
                    String[] parts = contentRange.split("/");
                    if (parts.length == 2 && parts[1].equals("*") == false) {
                        try {
                            cachedLength = Long.parseLong(parts[1]);
                        } catch (NumberFormatException ignored) {}
                    }
                }
            }

            listener.onResponse(ByteBuffer.wrap(responseBytes.asByteArray()));
        });
    }

    @Override
    public boolean supportsNativeAsync() {
        return s3AsyncClient != null;
    }

    @Override
    public String toString() {
        return "S3StorageObject{bucket=" + bucket + ", key=" + key + ", path=" + path + "}";
    }
}
