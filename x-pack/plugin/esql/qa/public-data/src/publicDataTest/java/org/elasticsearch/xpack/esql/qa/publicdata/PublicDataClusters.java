/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xpack.esql.datasources.Federation;

/**
 * The three-node, all-datasource cluster every public-data suite runs against (plan section 2): two
 * {@code data,ingest,master} nodes plus one coordinating-only node, with REST traffic directed at the
 * coordinator so every query exercises the same distributed/exchange path a real deployment would.
 * <p>
 * Every {@code esql-datasource-*} reader/codec plugin (parquet, parquet-rs, ORC, CSV, NDJSON, gzip,
 * snappy, zstd, brotli, bzip2, lz4, compression-libs, netty-commons) ships bundled, unconditionally, in
 * the {@code DEFAULT} distribution -- no explicit {@code .module(...)}/{@code .plugin(...)} request is
 * needed for them, mirroring every other ES|QL external-source suite in this repository. Only the
 * HTTP/GCS/Azure/ORC/parquet-rs <em>readers</em> are gated behind a release-build {@link FeatureFlag}; S3
 * needs none. This suite never registers a snapshot repository, so it needs neither the
 * {@code repository-s3} nor the {@code repository-gcs} <em>module</em> that the Iceberg-holdout suites
 * request for that unrelated feature.
 */
public final class PublicDataClusters {

    /** {@code -Dtests.public_data.heap}: JVM heap for every node, forwarded by the Gradle task. */
    private static final String HEAP = System.getProperty("tests.public_data.heap", "8g");

    private PublicDataClusters() {}

    public static ElasticsearchCluster build() {
        LocalClusterSpecBuilder<ElasticsearchCluster> cluster = ElasticsearchCluster.local()
            .name("public-data")
            // Every esql-datasource-* format/codec plugin (and the esql plugin's own federation settings)
            // ships only in the DEFAULT distribution, not the minimal INTEG_TEST one the builder defaults
            // to; without this, node startup fails fast with "unknown setting [esql.federation.enabled]".
            .distribution(DistributionType.DEFAULT)
            .nodes(3)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            // Federation is on by default only in snapshot builds; this suite reads external sources
            // unconditionally regardless of build type.
            .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
            // Force-enable every reader this suite exercises so it runs in a release build too (S3 needs
            // no flag). LOCAL is deliberately omitted: this suite never reads a file:// resource.
            .feature(FeatureFlag.ESQL_EXTERNAL_DATASOURCES_HTTP)
            .feature(FeatureFlag.ESQL_EXTERNAL_GCS)
            .feature(FeatureFlag.ESQL_EXTERNAL_AZURE)
            .feature(FeatureFlag.ESQL_EXTERNAL_ORC)
            .feature(FeatureFlag.ESQL_EXTERNAL_PARQUET_RS)
            .jvmArg("-Xms" + HEAP)
            .jvmArg("-Xmx" + HEAP)
            // Needed by the parquet-rs (Arrow) native reader's off-heap buffer access; mirrors every other
            // ES|QL external-source suite's cluster configuration.
            .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .jvmArg("-Darrow.allocation.manager.type=Unsafe")
            // Node 2 is the coordinating-only node every REST request targets (see PublicDataRestTestCase);
            // nodes 0 and 1 keep the framework's implicit default roles (master, data, ingest).
            .node(2, node -> node.name("public-data-coordinator").setting("node.roles", "[]"));
        return cluster.build();
    }
}
