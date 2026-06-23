/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;

/**
 * Cluster configuration for CSV integration tests.
 */
public class Clusters {

    private static final String ENCRYPTION_PASSWORD_ID = "test";
    private static final String ENCRYPTION_PASSWORD = "esql-test-encryption-password";

    /**
     * Installs the project-encryption-key (PEK) secure settings + feature flag so data-source secrets
     * can be encrypted when a data source is registered via {@code PUT /_query/data_source}. Mirrors the
     * single-node esql qa datasource-CRUD cluster config. Applied only by {@link #testClusterWithEncryption}.
     */
    private static final LocalClusterConfigProvider DATASET_ENCRYPTION_CONFIG = builder -> builder.systemProperty(
        "es.project_encryption_key_feature_flag_enabled",
        "true"
    )
        .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, ENCRYPTION_PASSWORD)
        .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID);

    private static LocalClusterSpecBuilder<ElasticsearchCluster> baseBuilder(
        Supplier<String> s3EndpointSupplier,
        LocalClusterConfigProvider configProvider
    ) {
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
            .setting("path.repo", FixtureUtils.pathRepoRootForIcebergFixtures(Clusters.class))
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
            // Apply any additional configuration
            .apply(() -> configProvider);
    }

    public static ElasticsearchCluster testCluster(Supplier<String> s3EndpointSupplier, LocalClusterConfigProvider configProvider) {
        return baseBuilder(s3EndpointSupplier, configProvider).build();
    }

    public static ElasticsearchCluster testCluster(Supplier<String> s3EndpointSupplier) {
        return testCluster(s3EndpointSupplier, config -> {});
    }

    /**
     * Encryption-enabled variant of {@link #testCluster(Supplier)} for suites that register data sources
     * carrying secret settings (e.g. {@code access_key}/{@code secret_key}) and then read them back via
     * {@code FROM <dataset>}. Registering such a data source encrypts the secrets, which requires an
     * installed project encryption key; without {@link #DATASET_ENCRYPTION_CONFIG} the request fails with
     * a 503 {@code encryption_key_not_yet_available_exception}.
     *
     * <p>Kept as a separate factory (rather than folded into {@link #testCluster(Supplier)}) so the
     * non-subquery spec suites that use the inline {@code EXTERNAL "..." WITH {creds}} form — which never
     * persists secrets — keep their existing cluster configuration unchanged.
     */
    public static ElasticsearchCluster testClusterWithEncryption(Supplier<String> s3EndpointSupplier) {
        return testCluster(s3EndpointSupplier, DATASET_ENCRYPTION_CONFIG);
    }

    /**
     * A two-node cluster that splits roles so the coordinator and the data node are separate JVMs:
     * node 0 is <em>coordinating-only</em> (empty roles — no {@code data}) and node 1 is master+data.
     * The empty-roles coordinator is deliberate: a node that can also host the data phase makes ES|QL
     * collapse the plan to a single-node execution and run the external scan locally, which would not
     * exercise the cross-JVM return-flow. With no data role on node 0, ES|QL must dispatch the scan to
     * node 1, whose JVM-static {@code ExternalStats} is separate — so the only way node 0's
     * planning-time cache can be populated for the warm query is the {@code DriverCompletionInfo}
     * return-flow. A single-JVM {@code internalClusterTest} cannot exercise this because all nodes
     * share one static cache. Tests target node 0's HTTP endpoint for both the cold and warm query so
     * the per-coordinator schema cache is consulted consistently.
     */
    public static ElasticsearchCluster multiNodeTestCluster(Supplier<String> s3EndpointSupplier) {
        return baseBuilder(s3EndpointSupplier, config -> {}).withNode(node -> node.name("coordinator").setting("node.roles", "[]"))
            .withNode(node -> node.name("data-node").setting("node.roles", "[master, data]"))
            .build();
    }
}
