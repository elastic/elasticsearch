/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;

/**
 * Cluster configuration for external source distributed integration tests.
 * Delegates to {@link Clusters#testCluster} so the serverless test infrastructure
 * can substitute its own cluster builder (which does not support {@code .nodes()}).
 */
public class ExternalDistributedClusters {

    private static final String ENCRYPTION_PASSWORD_ID = "test";
    private static final String ENCRYPTION_PASSWORD = "esql-test-encryption-password";

    static ElasticsearchCluster testCluster(Supplier<String> s3EndpointSupplier) {
        return Clusters.testCluster(spec -> {
            // This suite force-enables every storage backend / format flag so its external tests run even in
            // release builds: file:// and http(s):// scheme registration depends on their sub-flags (otherwise
            // those reads hit "No storage provider registered for scheme: …"), and GCS/Azure/ORC/parquet-rs must
            // be on to exercise every backend in release builds too.
            spec.feature(FeatureFlag.ESQL_EXTERNAL_DATASOURCES_LOCAL);
            spec.feature(FeatureFlag.ESQL_EXTERNAL_DATASOURCES_HTTP);
            spec.feature(FeatureFlag.ESQL_EXTERNAL_GCS);
            spec.feature(FeatureFlag.ESQL_EXTERNAL_AZURE);
            spec.feature(FeatureFlag.ESQL_EXTERNAL_ORC);
            spec.feature(FeatureFlag.ESQL_EXTERNAL_PARQUET_RS);
            spec.plugin("inference-service-test");
            spec.module("repository-s3");
            spec.module("repository-gcs");
            spec.setting("xpack.ml.enabled", "false");
            spec.setting("path.repo", FixtureUtils.pathRepoRootForIcebergFixtures(ExternalDistributedClusters.class));
            // file:// fixtures live under the iceberg-fixtures root (not csvDataPath), so the allowlist must point here.
            spec.setting(
                "esql.datasource.local_allowed_paths",
                FixtureUtils.pathRepoRootForIcebergFixtures(ExternalDistributedClusters.class)
            );
            spec.setting("s3.client.default.endpoint", s3EndpointSupplier);
            spec.keystore("s3.client.default.access_key", ACCESS_KEY);
            spec.keystore("s3.client.default.secret_key", SECRET_KEY);
            spec.setting("s3.client.default.protocol", "http");
            spec.environment("AWS_CONFIG_FILE", "/dev/null/aws/config");
            spec.environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials");
            // Project-encryption-key so the s3 data source's secret settings (access_key/secret_key) can be
            // encrypted at registration time; without it PUT /_query/data_source fails with a 503
            // encryption_key_not_yet_available_exception. Mirrors the csv qa testClusterWithEncryption config.
            spec.keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, ENCRYPTION_PASSWORD);
            spec.keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID);
            spec.jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED");
            spec.jvmArg("-Darrow.allocation.manager.type=Unsafe");
        });
    }
}
