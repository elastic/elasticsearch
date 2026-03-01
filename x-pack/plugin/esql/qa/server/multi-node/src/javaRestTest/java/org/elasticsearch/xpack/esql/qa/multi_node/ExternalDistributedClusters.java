/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;

/**
 * Cluster configuration for external source distributed integration tests.
 * Delegates to {@link Clusters#testCluster} so the serverless test infrastructure
 * can substitute its own cluster builder (which does not support {@code .nodes()}).
 */
public class ExternalDistributedClusters {

    static ElasticsearchCluster testCluster(Supplier<String> s3EndpointSupplier) {
        return Clusters.testCluster(spec -> {
            spec.module("repository-s3");
            spec.module("repository-gcs");
            spec.setting("xpack.ml.enabled", "false");
            spec.setting("path.repo", fixturesPath());
            spec.setting("s3.client.default.endpoint", s3EndpointSupplier);
            spec.keystore("s3.client.default.access_key", ACCESS_KEY);
            spec.keystore("s3.client.default.secret_key", SECRET_KEY);
            spec.setting("s3.client.default.protocol", "http");
            spec.environment("AWS_CONFIG_FILE", "/dev/null/aws/config");
            spec.environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials");
            spec.jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED");
            spec.jvmArg("-Darrow.allocation.manager.type=Unsafe");
        });
    }

    private static String fixturesPath() {
        URL resourceUrl = ExternalDistributedClusters.class.getResource("/iceberg-fixtures");
        if (resourceUrl != null && resourceUrl.getProtocol().equals("file")) {
            try {
                return PathUtils.get(resourceUrl.toURI()).toAbsolutePath().toString();
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Failed to resolve fixtures path", e);
            }
        }
        return "/tmp";
    }
}
