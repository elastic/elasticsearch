/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import org.elasticsearch.Version;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.qa.rest.EsqlDataSourceMixedClusterTestSupport;

import java.nio.file.Path;
import java.util.List;

public class Clusters {

    public static final String OLD_CLUSTER_VERSION_PROPERTY = "tests.old_cluster_version";

    public static ElasticsearchCluster testCluster(LocalClusterConfigProvider configProvider) {
        return testCluster(CsvTestUtils.createCsvDataDirectory(), configProvider, false);
    }

    public static ElasticsearchCluster testCluster(Path csvDataPath, LocalClusterConfigProvider configProvider, boolean shared) {
        LocalClusterSpecBuilder<ElasticsearchCluster> cluster = baseCluster(csvDataPath).nodes(2)
            .setting("esql.external.local_allowed_paths", csvDataPath::toString)
            // Federation is only on by default in snapshot builds; the EXTERNAL and dataset specs run here need it on.
            .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
            .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS)
            .apply(() -> configProvider);
        if (shared) {
            cluster.shared(true);
        }
        return cluster.build();
    }

    /**
     * The cluster {@link GenerativeIT} runs against: the current build, or the version in {@value #OLD_CLUSTER_VERSION_PROPERTY}.
     * The {@code v<version>#generativeTests} tasks set that property to test a release branch with this branch's query
     * generator. A node refuses to start on a setting it doesn't know, so older nodes get only the settings their version
     * has, and {@code currentVersionConfig} applies only to the current build.
     */
    public static ElasticsearchCluster generativeCluster(LocalClusterConfigProvider currentVersionConfig) {
        String oldVersionString = System.getProperty(OLD_CLUSTER_VERSION_PROPERTY);
        if (oldVersionString == null) {
            return testCluster(spec -> {
                encryptionKeystore(spec);
                currentVersionConfig.apply(spec);
            });
        }
        Version oldVersion = EsqlDataSourceMixedClusterTestSupport.bwcVersion();
        Path csvDataPath = CsvTestUtils.createCsvDataDirectory();
        LocalClusterSpecBuilder<ElasticsearchCluster> cluster = baseCluster(csvDataPath);
        for (String nodeName : List.of("old-node-0", "old-node-1")) {
            cluster.withNode(
                node -> EsqlDataSourceMixedClusterTestSupport.configureOldNode(
                    node,
                    nodeName,
                    oldVersionString,
                    oldVersion,
                    EsqlDataSourceMixedClusterTestSupport.isDetachedBwcBuild(),
                    csvDataPath::toString,
                    (configuredNode, version, current) -> {}
                )
            );
        }
        if (oldVersion.onOrAfter(Version.V_9_5_0)) {
            encryptionKeystore(cluster);
        }
        return cluster.build();
    }

    private static LocalClusterSpecBuilder<ElasticsearchCluster> baseCluster(Path csvDataPath) {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("path.repo", csvDataPath::toString)
            .configFile("user-agent/custom-regexes.yml", Resource.fromClasspath("custom-regexes.yml"))
            .configFile("ingest-geoip/GeoLite2-City.mmdb", Resource.fromClasspath("GeoLite2-City.mmdb"))
            .configFile("ingest-geoip/GeoLite2-Country.mmdb", Resource.fromClasspath("GeoLite2-Country.mmdb"))
            .configFile("ingest-geoip/GeoLite2-ASN.mmdb", Resource.fromClasspath("GeoLite2-ASN.mmdb"))
            .setting("ingest.geoip.downloader.enabled", "false");
    }

    /**
     * {@code PUT /_query/data_source} stores access_key/secret_key as secure cluster-state settings;
     * encryption must be enabled or the registration returns 503.
     */
    private static void encryptionKeystore(LocalClusterSpecBuilder<?> spec) {
        spec.keystore("cluster.state.encryption.password.test", "esql-generative-encryption-password");
        spec.keystore("cluster.state.encryption.active_password_id", "test");
    }
}
