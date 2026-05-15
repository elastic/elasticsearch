/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;

import java.util.function.Supplier;

public class Clusters {
    public static final int HEAP_SIZE_IN_MB = 512;

    static ElasticsearchCluster buildCluster() {
        return baseClusterSpec().build();
    }

    /**
     * Cluster variant for {@link HeapAttackExternalIT}: same constrained heap as the default
     * heap-attack cluster, plus the ESQL external-datasources feature flag and the cluster-side
     * S3 client configuration pointing at the test's in-memory S3 fixture. The {@code
     * repository-s3} module and the {@code esql-datasource-*} plugins already ship with the
     * default distribution, so they don't need to be added explicitly.
     *
     * @param s3EndpointSupplier called lazily when the cluster starts; returns the address of
     *                           the (already-started) in-memory S3 fixture
     */
    static ElasticsearchCluster buildExternalCluster(Supplier<String> s3EndpointSupplier) {
        var spec = baseClusterSpec().feature(FeatureFlag.ESQL_EXTERNAL_DATASOURCES)
            // S3 client wiring — endpoint discovered at startup from the fixture rule.
            .setting("s3.client.default.endpoint", s3EndpointSupplier)
            .setting("s3.client.default.protocol", "http")
            .keystore("s3.client.default.access_key", S3FixtureUtils.ACCESS_KEY)
            .keystore("s3.client.default.secret_key", S3FixtureUtils.SECRET_KEY)
            // Pin the AWS SDK away from any host-side AWS config files.
            .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
            .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
            // Parquet decoder uses direct byte buffers / Unsafe under the hood.
            .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .jvmArg("-Darrow.allocation.manager.type=Unsafe");
        return spec.build();
    }

    private static LocalClusterSpecBuilder<ElasticsearchCluster> baseClusterSpec() {
        var spec = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .nodes(2)
            .module("test-esql-heap-attack")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("esql.query.allow_partial_results", "false")
            .setting("logger.org.elasticsearch.compute.lucene.read", "DEBUG")
            .jvmArg("-Xmx" + HEAP_SIZE_IN_MB + "m");
        String javaVersion = JvmInfo.jvmInfo().version();
        if (javaVersion.equals("20") || javaVersion.equals("21")) {
            // see https://github.com/elastic/elasticsearch/issues/99592
            spec.jvmArg("-XX:+UnlockDiagnosticVMOptions -XX:+G1UsePreventiveGC");
        }
        return spec;
    }
}
