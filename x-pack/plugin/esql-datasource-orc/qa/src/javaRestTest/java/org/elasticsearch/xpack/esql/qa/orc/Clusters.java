/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.orc;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;
import org.elasticsearch.xpack.esql.qa.rest.EsqlDataSourceMixedClusterTestSupport;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;

/**
 * Cluster configuration for ORC integration tests.
 */
public class Clusters {

    public static ElasticsearchCluster testCluster(Supplier<String> s3EndpointSupplier, LocalClusterConfigProvider configProvider) {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .shared(true)
            .plugin("inference-service-test")
            .module("repository-s3")
            .module("repository-gcs")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
            .setting("xpack.ml.enabled", "false")
            .setting("path.repo", FixtureUtils.pathRepoRootForIcebergFixtures(Clusters.class))
            .setting("esql.external.local_allowed_paths", FixtureUtils.pathRepoRootForIcebergFixtures(Clusters.class))
            .setting("s3.client.default.endpoint", s3EndpointSupplier)
            .keystore("s3.client.default.access_key", ACCESS_KEY)
            .keystore("s3.client.default.secret_key", SECRET_KEY)
            .setting("s3.client.default.protocol", "http")
            .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
            .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
            // Arrow's unsafe memory allocator requires access to java.nio internals
            .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .jvmArg("-Darrow.allocation.manager.type=Unsafe")
            .apply(() -> configProvider)
            .build();
    }

    public static ElasticsearchCluster testCluster(Supplier<String> s3EndpointSupplier) {
        return testCluster(s3EndpointSupplier, config -> {});
    }

    /**
     * Mixed old/current variant used only by the reusable data-source BWC tasks.
     */
    public static ElasticsearchCluster bwcTestCluster(Supplier<String> s3EndpointSupplier) {
        String fixturesPath = FixtureUtils.pathRepoRootForIcebergFixtures(Clusters.class);
        return EsqlDataSourceMixedClusterTestSupport.mixedCluster(
            () -> fixturesPath,
            builder -> builder.module("repository-s3")
                .module("repository-gcs")
                .setting("xpack.security.enabled", "false")
                .setting("xpack.license.self_generated.type", "trial")
                .setting("xpack.ml.enabled", "false")
                .setting("path.repo", fixturesPath)
                .setting("s3.client.default.endpoint", s3EndpointSupplier)
                .keystore("s3.client.default.access_key", ACCESS_KEY)
                .keystore("s3.client.default.secret_key", SECRET_KEY)
                .setting("s3.client.default.protocol", "http")
                .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
                .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
                .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
                .jvmArg("-Darrow.allocation.manager.type=Unsafe")
                .feature(FeatureFlag.ESQL_EXTERNAL_DATASOURCES_LOCAL)
                .feature(FeatureFlag.ESQL_EXTERNAL_DATASOURCES_HTTP)
                .feature(FeatureFlag.ESQL_EXTERNAL_GCS)
                .feature(FeatureFlag.ESQL_EXTERNAL_AZURE)
                .feature(FeatureFlag.ESQL_EXTERNAL_ORC),
            (node, version, current) -> {}
        );
    }
}
