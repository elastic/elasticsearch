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

import java.nio.file.Path;

public class Clusters {
    public static ElasticsearchCluster mixedVersionCluster() {
        return mixedVersionCluster(CsvTestUtils.createCsvDataDirectory(), false);
    }

    public static ElasticsearchCluster mixedVersionCluster(Path csvDataPath, boolean shared) {
        String oldVersionString = System.getProperty("tests.old_cluster_version");
        Version oldVersion = Version.fromString(oldVersionString);
        boolean isDetachedVersion = System.getProperty("tests.bwc.refspec.main") != null;
        var cluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .withNode(node -> node.version(oldVersionString, isDetachedVersion))
            .withNode(node -> node.version(Version.CURRENT).setting("esql.datasource.local_allowed_paths", csvDataPath::toString))
            .withNode(node -> node.version(oldVersionString, isDetachedVersion))
            .withNode(node -> node.version(Version.CURRENT).setting("esql.datasource.local_allowed_paths", csvDataPath::toString))
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("path.repo", csvDataPath::toString)
            .configFile("user-agent/custom-regexes.yml", Resource.fromClasspath("custom-regexes.yml"))
            .configFile("ingest-geoip/GeoLite2-City.mmdb", Resource.fromClasspath("GeoLite2-City.mmdb"))
            .configFile("ingest-geoip/GeoLite2-Country.mmdb", Resource.fromClasspath("GeoLite2-Country.mmdb"))
            .configFile("ingest-geoip/GeoLite2-ASN.mmdb", Resource.fromClasspath("GeoLite2-ASN.mmdb"))
            .setting("ingest.geoip.downloader.enabled", "false")
            // DLM frozen tier serialization is gated on both a feature flag and a transport version, so nodes in a mixed cluster can
            // disagree on the wire format when their build types differ (snapshot vs release). Disable the flag on every node so
            // serialization is consistent regardless of build type. See https://github.com/elastic/elasticsearch/issues/153679.
            .systemProperty("es.dlm_searchable_snapshots_feature_flag_enabled", "false");
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
