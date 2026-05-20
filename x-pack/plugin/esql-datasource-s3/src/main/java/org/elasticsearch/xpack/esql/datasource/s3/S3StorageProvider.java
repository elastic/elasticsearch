/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import io.netty.channel.ChannelOption;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import org.elasticsearch.xpack.esql.datasource.nettycommons.PooledRecvByteBufAllocator;
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
 * <p>
 * Maintains both a sync {@link S3Client} (Apache HTTP) and an async {@link S3AsyncClient}
 * (Netty NIO, via {@code netty-nio-client} at {@code ${versions.awsv2sdk}}). The two clients
 * serve different purposes:
 * <ul>
 *   <li><b>Sync client</b> — used for streaming reads ({@code newStream} returns a live
 *       {@code ResponseInputStream} that reads on demand), metadata ({@code headObject}),
 *       existence checks, and object listing. These operations are inherently blocking or
 *       return streaming results that cannot be efficiently expressed as futures.</li>
 *   <li><b>Async client</b> — used exclusively for {@code readBytesAsync} range reads in
 *       {@link S3StorageObject}. When {@code ConcurrencyLimitedStorageObject} dispatches
 *       multiple concurrent range reads, the Netty event loop handles them without blocking
 *       a thread per request, reducing thread-pool pressure under load.</li>
 * </ul>
 * <p>
 * Both clients share the same credentials, region, and endpoint configuration. The Netty
 * jars are bundled with this plugin (classloader-isolated from the server and other plugins)
 * at {@code ${versions.netty}}, matching the pattern used by the inference plugin.
 */
public final class S3StorageProvider implements StorageProvider {
    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final S3Configuration config;

    public S3StorageProvider(S3Configuration config) {
        this.config = config;
        this.s3Client = buildS3Client(config);
        this.s3AsyncClient = buildS3AsyncClient(config);
    }

    /** Test-only constructor that accepts pre-built clients. */
    S3StorageProvider(S3Client s3Client, S3AsyncClient s3AsyncClient) {
        this.config = null;
        this.s3Client = s3Client;
        this.s3AsyncClient = s3AsyncClient;
    }

    private static S3Client buildS3Client(S3Configuration config) {
        return configureCommon(S3Client.builder(), config).build();
    }

    private static S3AsyncClient buildS3AsyncClient(S3Configuration config) {
        // Install a pooled receive-buffer allocator so that socket reads on Netty channels reuse
        // pooled memory instead of allocating a fresh zero-filled byte[] per read. The AWS SDK's
        // Netty client unconditionally overrides ChannelOption.ALLOCATOR to UnpooledByteBufAllocator
        // for HTTPS channels using the JDK SSL provider, so configuring ALLOCATOR directly has no
        // effect; RCVBUF_ALLOCATOR is the closest knob the SDK leaves untouched as of
        // netty-nio-client 2.31.x. Re-verify this assumption when bumping the AWS SDK version. See
        // PooledRecvByteBufAllocator for the full rationale.
        NettyNioAsyncHttpClient.Builder httpClient = NettyNioAsyncHttpClient.builder()
            .putChannelOption(ChannelOption.RCVBUF_ALLOCATOR, PooledRecvByteBufAllocator.DEFAULT);
        return configureCommon(S3AsyncClient.builder(), config).httpClient(httpClient.build()).build();
    }

    /**
     * Applies credentials, region, endpoint, and profile settings common to both the sync and async S3 clients.
     */
    private static <B extends S3BaseClientBuilder<B, ?>> B configureCommon(B builder, S3Configuration config) {
        // Disable profile file loading to prevent the AWS SDK from reading ~/.aws/config
        // or the path set via AWS_CONFIG_FILE, which would be blocked by the entitlement system.
        ProfileFile emptyProfileFile = ProfileFile.aggregator().build();
        builder.overrideConfiguration(c -> {
            c.defaultProfileFile(emptyProfileFile);
            c.defaultProfileFileSupplier(() -> emptyProfileFile);
        });

        // Disable optional response checksum validation. The SDK default (WHEN_SUPPORTED) wraps
        // every GetObject response in a checksum-validating stream, adding ~6-7% CPU overhead.
        // TLS already provides in-transit integrity; this matches what other engines do.
        builder.responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);

        AwsCredentialsProvider credentialsProvider;
        if (config != null && config.isAnonymous()) {
            credentialsProvider = AnonymousCredentialsProvider.create();
        } else if (config != null && config.hasCredentials()) {
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

        return builder;
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);
        return new S3StorageObject(s3Client, s3AsyncClient, bucket, key, path);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);
        return new S3StorageObject(s3Client, s3AsyncClient, bucket, key, path, length);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);
        return new S3StorageObject(s3Client, s3AsyncClient, bucket, key, path, length, lastModified);
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
            if (e instanceof S3Exception s3e && s3e.statusCode() == 403) {
                return existsViaRangeGet(bucket, key, path);
            }
            throw new IOException("Failed to check existence of " + path + credentialHint(), e);
        }
    }

    private boolean existsViaRangeGet(String bucket, String key, StoragePath path) throws IOException {
        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=0-0").build();
            try (var response = s3Client.getObject(request)) {
                return true;
            }
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            throw new IOException("Failed to check existence of " + path + " (HEAD denied, range GET also failed)" + credentialHint(), e);
        }
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("s3", "s3a", "s3n");
    }

    @Override
    public void close() throws IOException {
        try {
            s3Client.close();
        } finally {
            s3AsyncClient.close();
        }
    }

    private String credentialHint() {
        if (config == null || (config.isAnonymous() == false && config.hasCredentials() == false)) {
            return ". If accessing a public bucket, use WITH (auth = 'none'). "
                + "Otherwise, provide credentials via WITH (access_key = '...', secret_key = '...') or set AWS environment variables";
        }
        return "";
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

            Instant lastModified = s3Object.lastModified();
            if (lastModified == null) {
                lastModified = Instant.EPOCH;
            }
            return new StorageEntry(objectPath, s3Object.size(), lastModified);
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
                String msg = (e instanceof S3Exception s3e && s3e.statusCode() == 403)
                    ? "Access denied listing objects in bucket ["
                        + bucket
                        + "] with prefix ["
                        + prefix
                        + "]. "
                        + "Verify that the configured credentials have s3:ListBucket permission on this bucket, "
                        + "or use exact file paths instead of glob patterns."
                    : "Failed to list objects in bucket [" + bucket + "] with prefix [" + prefix + "]";
                throw new RuntimeException(msg, e);
            }
        }
    }
}
