/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.iceberg;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;

/**
 * Cluster configuration for Iceberg integration tests.
 * Provides ES cluster setup with S3 repository plugin and Iceberg catalog configuration.
 */
public class Clusters {

    /**
     * Creates a test cluster configured for Iceberg integration testing.
     *
     * @param s3EndpointSupplier supplier for the S3 fixture endpoint URL
     * @param configProvider additional cluster configuration provider
     * @return configured ElasticsearchCluster
     */
    public static ElasticsearchCluster testCluster(Supplier<String> s3EndpointSupplier, LocalClusterConfigProvider configProvider) {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .shared(true)
            // Enable S3 repository plugin for S3 access
            .module("repository-s3")
            // Basic cluster settings
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            // Disable ML to avoid native code loading issues in some environments
            .setting("xpack.ml.enabled", "false")
            // S3 client configuration for accessing the S3HttpFixture
            .setting("s3.client.default.endpoint", s3EndpointSupplier)
            // S3 credentials must be stored in keystore, not as regular settings
            .keystore("s3.client.default.access_key", ACCESS_KEY)
            .keystore("s3.client.default.secret_key", SECRET_KEY)
            // Disable SSL for HTTP fixture
            .setting("s3.client.default.protocol", "http")
            // Disable AWS SDK profile file loading by pointing to non-existent files
            // This prevents the SDK from trying to read ~/.aws/credentials and ~/.aws/config
            // which would violate Elasticsearch entitlements
            .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
            .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
            // Arrow's unsafe memory allocator requires access to java.nio internals
            .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            // Configure Arrow to use unsafe memory allocator instead of netty
            // This must be set as a JVM arg to take effect before any Arrow classes are loaded
            .jvmArg("-Darrow.allocation.manager.type=Unsafe")
            // Apply any additional configuration
            .apply(() -> configProvider)
            .build();
    }

    /**
     * Creates a test cluster with default configuration.
     *
     * @param s3EndpointSupplier supplier for the S3 fixture endpoint URL
     * @return configured ElasticsearchCluster
     */
    public static ElasticsearchCluster testCluster(Supplier<String> s3EndpointSupplier) {
        return testCluster(s3EndpointSupplier, config -> {});
    }
}
