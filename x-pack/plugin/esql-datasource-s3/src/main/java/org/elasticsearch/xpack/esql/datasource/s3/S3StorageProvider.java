/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * StorageProvider implementation for S3 using AWS SDK v2.
 */
public final class S3StorageProvider implements StorageProvider {
    private final S3Client s3Client;
    private final S3Configuration config;

    public S3StorageProvider(S3Configuration config) {
        this.config = config;
        this.s3Client = buildS3Client(config);
    }

    private static S3Client buildS3Client(S3Configuration config) {
        S3ClientBuilder builder = S3Client.builder();

        AwsCredentialsProvider credentialsProvider;
        if (config != null && config.hasCredentials()) {
            credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(config.accessKey(), config.secretKey()));
        } else {
            credentialsProvider = DefaultCredentialsProvider.create();
        }
        builder.credentialsProvider(credentialsProvider);

        if (config != null && config.region() != null) {
            builder.region(Region.of(config.region()));
        } else {
            builder.region(Region.US_EAST_1);
        }

        if (config != null && config.endpoint() != null) {
            builder.endpointOverride(URI.create(config.endpoint()));
            builder.forcePathStyle(true);
        }

        return builder.build();
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);
        return new S3StorageObject(s3Client, bucket, key, path);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);
        return new S3StorageObject(s3Client, bucket, key, path, length);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);
        return new S3StorageObject(s3Client, bucket, key, path, length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        validateS3Scheme(prefix);
        String bucket = prefix.host();
        String keyPrefix = extractKey(prefix);

        if (keyPrefix.isEmpty() == false && keyPrefix.endsWith(StoragePath.PATH_SEPARATOR) == false) {
            keyPrefix += StoragePath.PATH_SEPARATOR;
        }

        // S3 is a flat namespace — ListObjectsV2 is inherently prefix-based and recursive.
        // The recursive flag is effectively ignored.
        return new S3StorageIterator(s3Client, bucket, keyPrefix, prefix);
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);

        try {
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
            s3Client.headObject(request);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            throw new IOException("Failed to check existence of " + path, e);
        }
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("s3", "s3a", "s3n");
    }

    @Override
    public void close() throws IOException {
        s3Client.close();
    }

    private void validateS3Scheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("s3") == false && scheme.equals("s3a") == false && scheme.equals("s3n") == false) {
            throw new IllegalArgumentException("S3StorageProvider only supports s3://, s3a://, and s3n:// schemes, got: " + scheme);
        }
    }

    private String extractKey(StoragePath path) {
        String key = path.path();
        if (key.startsWith(StoragePath.PATH_SEPARATOR)) {
            key = key.substring(1);
        }
        return key;
    }

    public S3Client s3Client() {
        return s3Client;
    }

    public S3Configuration config() {
        return config;
    }

    @Override
    public String toString() {
        return "S3StorageProvider{config=" + config + "}";
    }

    /**
     * Iterator for S3 object listing with pagination support.
     */
    private static final class S3StorageIterator implements StorageIterator {
        private final S3Client s3Client;
        private final String bucket;
        private final String prefix;
        private final StoragePath baseDirectory;

        private Iterator<S3Object> currentBatch;
        private String continuationToken;
        private boolean hasMorePages;
        private boolean initialized;

        S3StorageIterator(S3Client s3Client, String bucket, String prefix, StoragePath baseDirectory) {
            this.s3Client = s3Client;
            this.bucket = bucket;
            this.prefix = prefix;
            this.baseDirectory = baseDirectory;
            this.hasMorePages = true;
            this.initialized = false;
        }

        @Override
        public boolean hasNext() {
            if (initialized == false) {
                fetchNextBatch();
                initialized = true;
            }

            if (currentBatch != null && currentBatch.hasNext()) {
                return true;
            }

            if (hasMorePages) {
                fetchNextBatch();
                return currentBatch != null && currentBatch.hasNext();
            }

            return false;
        }

        @Override
        public StorageEntry next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }

            S3Object s3Object = currentBatch.next();
            String fullPath = baseDirectory.scheme() + StoragePath.SCHEME_SEPARATOR + bucket + StoragePath.PATH_SEPARATOR + s3Object.key();
            StoragePath objectPath = StoragePath.of(fullPath);

            return new StorageEntry(objectPath, s3Object.size(), s3Object.lastModified());
        }

        @Override
        public void close() throws IOException {
            // No resources to close
        }

        private void fetchNextBatch() {
            try {
                ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix);

                if (continuationToken != null) {
                    requestBuilder.continuationToken(continuationToken);
                }

                ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());

                currentBatch = response.contents().iterator();
                continuationToken = response.nextContinuationToken();
                hasMorePages = response.isTruncated();
            } catch (Exception e) {
                throw new RuntimeException("Failed to list objects in bucket " + bucket + " with prefix " + prefix, e);
            }
        }
    }
}
