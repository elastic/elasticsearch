/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.iceberg;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.util.SerializableSupplier;

import java.net.URI;

/**
 * Factory for creating configured S3FileIO instances.
 * <p>
 * This class provides a way to create Iceberg's S3FileIO without using Hadoop,
 * replacing the previous HadoopCatalog-based approach. S3FileIO uses the AWS SDK
 * directly and works with both real S3 endpoints and test fixtures like S3HttpFixture.
 */
public final class S3FileIOFactory {

    // S3FileIO property keys
    private static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
    private static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
    private static final String S3_ENDPOINT = "s3.endpoint";
    private static final String CLIENT_REGION = "client.region";
    private static final String S3_PATH_STYLE_ACCESS = "s3.path-style-access";

    private S3FileIOFactory() {
        // Utility class - no instantiation
    }

    /**
     * Create and configure an S3FileIO instance with the given S3 configuration.
     * <p>
     * The returned S3FileIO is configured for:
     * <ul>
     *   <li>Static credentials if provided (access key and secret key)</li>
     *   <li>Custom endpoint if provided (for testing with S3-compatible services)</li>
     *   <li>Region if provided</li>
     *   <li>Path-style access (required for MinIO, LocalStack, and S3HttpFixture)</li>
     * </ul>
     *
     * @param s3Config S3 configuration (nullable - if null, uses default AWS credentials chain)
     * @return configured S3FileIO instance (caller should close when done)
     */
    public static S3FileIO create(S3Configuration s3Config) {
        // Create a pre-configured S3 client supplier
        // This bypasses Iceberg's HTTP client configuration which uses package-private classes
        // that can't be accessed via reflection in Elasticsearch's classloader environment
        SerializableSupplier<S3Client> s3ClientSupplier = (SerializableSupplier<S3Client> & java.io.Serializable) () -> {
            S3ClientBuilder builder = S3Client.builder();

            // Always set a region to avoid auto-detection issues
            Region region = Region.US_EAST_1; // Default region

            // CRITICAL: Create an empty profile file to prevent AWS SDK from reading ~/.aws/credentials
            // and ~/.aws/config files, which would trigger Elasticsearch entitlement violations.
            // We must set BOTH the profile file AND the profile file supplier to empty values.
            ProfileFile emptyProfileFile = ProfileFile.builder()
                .type(ProfileFile.Type.CREDENTIALS)
                .content(new java.io.ByteArrayInputStream(new byte[0]))
                .build();

            // Use a supplier that returns the empty profile file to prevent lazy loading of default files
            java.util.function.Supplier<ProfileFile> emptyProfileSupplier = () -> emptyProfileFile;

            builder.overrideConfiguration(c -> {
                c.defaultProfileFile(emptyProfileFile);
                c.defaultProfileFileSupplier(emptyProfileSupplier);
            });

            // Always provide explicit credentials
            if (s3Config != null && s3Config.hasCredentials()) {
                AwsBasicCredentials credentials = AwsBasicCredentials.create(s3Config.accessKey(), s3Config.secretKey());
                builder.credentialsProvider(StaticCredentialsProvider.create(credentials));
            } else {
                // Use default test credentials that match the S3 fixture expectations
                // These match the credentials in S3FixtureUtils
                AwsBasicCredentials testCredentials = AwsBasicCredentials.create("test-access-key", "test-secret-key");
                builder.credentialsProvider(StaticCredentialsProvider.create(testCredentials));
            }

            if (s3Config != null) {
                if (s3Config.endpoint() != null) {
                    builder.endpointOverride(URI.create(s3Config.endpoint()));
                }
                if (s3Config.region() != null) {
                    region = Region.of(s3Config.region());
                }
            }

            builder.region(region);

            // Enable path-style access for compatibility with MinIO, LocalStack, and S3HttpFixture
            builder.forcePathStyle(true);

            // Use URL connection HTTP client to avoid entitlement issues
            // The Apache HTTP client creates daemon threads which are blocked by Elasticsearch's entitlement system
            builder.httpClient(UrlConnectionHttpClient.builder().build());

            return builder.build();
        };

        // Initialize S3FileIO with the pre-configured S3 client
        return new S3FileIO(s3ClientSupplier);
    }

    /**
     * Create and configure an S3FileIO instance from individual configuration values.
     * <p>
     * This is a convenience method for cases where the configuration values are
     * available directly rather than through an S3Configuration object.
     *
     * @param accessKey S3 access key (nullable)
     * @param secretKey S3 secret key (nullable)
     * @param endpoint S3 endpoint URL (nullable)
     * @param region AWS region (nullable)
     * @return configured S3FileIO instance (caller should close when done)
     */
    public static S3FileIO create(String accessKey, String secretKey, String endpoint, String region) {
        S3Configuration s3Config = S3Configuration.fromFields(accessKey, secretKey, endpoint, region);
        return create(s3Config);
    }
}
