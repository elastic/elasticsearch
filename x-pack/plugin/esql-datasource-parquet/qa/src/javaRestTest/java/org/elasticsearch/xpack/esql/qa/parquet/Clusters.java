/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;

/**
 * Cluster configuration for Parquet integration tests.
 */
public class Clusters {

    private static final String ENCRYPTION_PASSWORD_ID = "test";
    private static final String ENCRYPTION_PASSWORD = "esql-test-encryption-password";

    /**
     * Installs the project-encryption-key (PEK) secure settings so data-source secrets can be encrypted
     * when a data source is registered via {@code PUT /_query/data_source}. Mirrors the single-node esql
     * qa datasource-CRUD cluster config. Applied only by {@link #testClusterWithEncryption}.
     */
    private static final LocalClusterConfigProvider DATASET_ENCRYPTION_CONFIG = builder -> builder.keystore(
        "cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID,
        ENCRYPTION_PASSWORD
    ).keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID);

    /**
     * Cluster for tests that only need HTTP access (e.g. downloading from public URLs).
     * No S3/GCS client settings are configured.
     */
    public static ElasticsearchCluster httpOnlyTestCluster() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .shared(true)
            .plugin("inference-service-test")
            .module("repository-s3")
            .module("repository-gcs")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.ml.enabled", "false")
            .setting("path.repo", FixtureUtils.pathRepoRootForIcebergFixtures(Clusters.class))
            .setting("esql.datasource.local_allowed_paths", FixtureUtils.pathRepoRootForIcebergFixtures(Clusters.class))
            .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .jvmArg("-Darrow.allocation.manager.type=Unsafe")
            .build();
    }

    public static ElasticsearchCluster testCluster(Supplier<String> s3EndpointSupplier, LocalClusterConfigProvider configProvider) {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .shared(true)
            .plugin("inference-service-test")
            // Enable S3 repository plugin for S3 access
            .module("repository-s3")
            // Enable GCS repository module for GCS access
            .module("repository-gcs")
            // Basic cluster settings
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            // Disable ML to avoid native code loading issues in some environments
            .setting("xpack.ml.enabled", "false")
            // Allow the LOCAL storage backend to read fixture files from the test resources directory.
            // The esql-datasource-http plugin's entitlement policy uses shared_repo for file read access.
            .setting("path.repo", FixtureUtils.pathRepoRootForFixtures(Clusters.class))
            .setting("esql.datasource.local_allowed_paths", FixtureUtils.pathRepoRootForFixtures(Clusters.class))
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

    public static ElasticsearchCluster testCluster(Supplier<String> s3EndpointSupplier) {
        return testCluster(s3EndpointSupplier, config -> {});
    }

    /**
     * Encryption-enabled variant of {@link #testCluster(Supplier)} for suites that register data sources
     * carrying secret settings and then read them back via {@code FROM <dataset>}. Registering such a data
     * source encrypts the secrets, which requires an installed project encryption key; without
     * {@link #DATASET_ENCRYPTION_CONFIG} the request fails with a 503 {@code encryption_key_not_yet_available_exception}.
     *
     * <p>Kept separate from {@link #testCluster(Supplier)} so the non-subquery spec suites that use the
     * inline {@code EXTERNAL "..." WITH {creds}} form keep their existing cluster configuration unchanged.
     */
    public static ElasticsearchCluster testClusterWithEncryption(Supplier<String> s3EndpointSupplier) {
        return testCluster(s3EndpointSupplier, DATASET_ENCRYPTION_CONFIG);
    }
}
