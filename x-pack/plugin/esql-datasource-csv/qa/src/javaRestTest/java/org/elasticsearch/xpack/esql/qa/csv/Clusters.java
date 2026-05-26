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
