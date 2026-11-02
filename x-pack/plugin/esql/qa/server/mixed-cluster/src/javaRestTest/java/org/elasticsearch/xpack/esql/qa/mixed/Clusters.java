/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.qa.rest.EsqlDataSourceMixedClusterTestSupport;

import java.nio.file.Path;

public class Clusters {

    public static ElasticsearchCluster mixedVersionCluster() {
        return mixedVersionCluster(CsvTestUtils.createCsvDataDirectory(), false);
    }

    public static ElasticsearchCluster mixedVersionCluster(Path csvDataPath, boolean shared) {
        String oldVersionString = System.getProperty("tests.old_cluster_version");
        Version oldVersion = Version.fromString(oldVersionString);
        org.elasticsearch.Version oldServerVersion = org.elasticsearch.Version.fromString(oldVersionString.replace("-SNAPSHOT", ""));
        boolean isDetachedVersion = System.getProperty("tests.bwc.refspec.main") != null;
        var cluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .withNode(
                node -> EsqlDataSourceMixedClusterTestSupport.configureOldNode(
                    node,
                    "old-node-0",
                    oldVersionString,
                    oldServerVersion,
                    isDetachedVersion,
                    csvDataPath::toString,
                    (configuredNode, version, current) -> {}
                )
            )
            .withNode(
                node -> EsqlDataSourceMixedClusterTestSupport.configureCurrentNode(
                    node,
                    "current-node-0",
                    csvDataPath::toString,
                    (configuredNode, version, current) -> {}
                )
            )
            .withNode(
                node -> EsqlDataSourceMixedClusterTestSupport.configureOldNode(
                    node,
                    "old-node-1",
                    oldVersionString,
                    oldServerVersion,
                    isDetachedVersion,
                    csvDataPath::toString,
                    (configuredNode, version, current) -> {}
                )
            )
            .withNode(
                node -> EsqlDataSourceMixedClusterTestSupport.configureCurrentNode(
                    node,
                    "current-node-1",
                    csvDataPath::toString,
                    (configuredNode, version, current) -> {}
                )
            )
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("path.repo", csvDataPath::toString)
            .configFile("user-agent/custom-regexes.yml", Resource.fromClasspath("custom-regexes.yml"))
            .configFile("ingest-geoip/GeoLite2-City.mmdb", Resource.fromClasspath("GeoLite2-City.mmdb"))
            .configFile("ingest-geoip/GeoLite2-Country.mmdb", Resource.fromClasspath("GeoLite2-Country.mmdb"))
            .configFile("ingest-geoip/GeoLite2-ASN.mmdb", Resource.fromClasspath("GeoLite2-ASN.mmdb"))
            .setting("ingest.geoip.downloader.enabled", "false");
        if (supportRetryOnShardFailures(oldVersion) == false) {
            cluster.setting("cluster.routing.rebalance.enable", "none");
        }
        if (oldVersion.before(Version.fromString("8.18.0"))) {
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper");
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
        }
        if (shared) {
            cluster.shared(true);
        }
        return cluster.build();
    }

    private static boolean supportRetryOnShardFailures(Version version) {
        return version.onOrAfter(Version.fromString("9.1.0"))
            || (version.onOrAfter(Version.fromString("8.19.0")) && version.before(Version.fromString("9.0.0")));
    }
}
