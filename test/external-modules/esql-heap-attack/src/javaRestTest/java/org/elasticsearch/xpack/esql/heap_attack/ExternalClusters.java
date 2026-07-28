/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;

import java.util.function.Supplier;

/**
 * Cluster factory for {@link HeapAttackExternalIT}: builds on top of the base cluster spec from
 * {@link Clusters#buildClusterSpec()} and wires in the S3 client configuration for the in-memory
 * test fixture. Kept separate from {@link Clusters} so that environment-specific overrides of
 * {@code Clusters} (e.g. serverless) only need to expose the cluster shape via
 * {@code buildClusterSpec()} without duplicating the S3 wiring.
 */
class ExternalClusters {

    /**
     * Request-breaker limit for the external heap-attack cluster, expressed as a percentage of
     * heap. Kept intentionally low so that the breaker trips before the untracked overhead from
     * the S3 async client's Netty infrastructure can push the node into an OOM.
     */
    static final int BREAKER_LIMIT_PERCENT = 30;

    /**
     * @param s3EndpointSupplier called lazily when the cluster starts; returns the address of
     *                           the (already-started) in-memory S3 fixture
     */
    static ElasticsearchCluster buildExternalCluster(Supplier<String> s3EndpointSupplier) {
        return Clusters.buildClusterSpec()
            // Pin the request-breaker limit so the suite reliably trips it regardless of the
            // base cluster's heap size (which varies between the standard and serverless configs).
            // Kept low to leave headroom for untracked S3/Netty allocations.
            .setting("indices.breaker.request.limit", BREAKER_LIMIT_PERCENT + "%")
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
            .jvmArg("-Darrow.allocation.manager.type=Unsafe")
            .build();
    }
}
