/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalNodeSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.datasources.Federation;

import java.nio.file.Path;

public class Clusters {

    private static final String FEDERATION_ENABLED_SETTING = Federation.FEDERATION_ENABLED.getKey();
    /**
     * The version {@link #FEDERATION_ENABLED_SETTING} exists as of. Node specs take a {@link Version}, which carries no
     * release constants, so the boundary is converted from the server constant instead of being spelled out again. The
     * qualified name disambiguates the two {@code Version} types this file needs.
     */
    private static final Version FEDERATION_SETTING_VERSION = Version.fromString(org.elasticsearch.Version.V_9_6_0.toString());

    public static ElasticsearchCluster mixedVersionCluster() {
        return mixedVersionCluster(CsvTestUtils.createCsvDataDirectory(), false);
    }

    public static ElasticsearchCluster mixedVersionCluster(Path csvDataPath, boolean shared) {
        String oldVersionString = System.getProperty("tests.old_cluster_version");
        Version oldVersion = Version.fromString(oldVersionString);
        boolean isDetachedVersion = System.getProperty("tests.bwc.refspec.main") != null;
        var cluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .withNode(node -> oldVersionNode(node, oldVersionString, isDetachedVersion, oldVersion))
            .withNode(node -> currentVersionNode(node, csvDataPath))
            .withNode(node -> oldVersionNode(node, oldVersionString, isDetachedVersion, oldVersion))
            .withNode(node -> currentVersionNode(node, csvDataPath))
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

    /**
     * Configures a current-version node with the settings that do not exist on every version in the mixed cluster: a node
     * that does not know a setting rejects it and fails to start, so the local-disk allowlist and the federation opt-in
     * are set per node rather than cluster-wide.
     */
    private static void currentVersionNode(LocalNodeSpecBuilder node, Path csvDataPath) {
        node.version(Version.CURRENT)
            .setting("esql.datasource.local_allowed_paths", csvDataPath::toString)
            .setting(FEDERATION_ENABLED_SETTING, "true");
    }

    /**
     * Configures an old-version node, opting it into federation when its version knows the setting. Any node can
     * coordinate a query and federation has to agree across the cluster, so every node that reads the setting gets it;
     * a version that predates the setting registers federation unconditionally and has it on without one.
     */
    private static void oldVersionNode(LocalNodeSpecBuilder node, String versionString, boolean detached, Version version) {
        node.version(versionString, detached);
        if (knowsFederationSetting(version)) {
            node.setting(FEDERATION_ENABLED_SETTING, "true");
        }
    }

    /**
     * Whether a node of this version accepts the federation setting. An older node rejects it as an unknown setting and
     * never starts.
     */
    private static boolean knowsFederationSetting(Version version) {
        return version.onOrAfter(FEDERATION_SETTING_VERSION);
    }

    private static boolean supportRetryOnShardFailures(Version version) {
        return version.onOrAfter(Version.fromString("9.1.0"))
            || (version.onOrAfter(Version.fromString("8.19.0")) && version.before(Version.fromString("9.0.0")));
    }
}
