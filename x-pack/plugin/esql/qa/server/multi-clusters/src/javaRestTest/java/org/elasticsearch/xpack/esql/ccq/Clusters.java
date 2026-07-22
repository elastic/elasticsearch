/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.datasources.Federation;

import java.nio.file.Path;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

public class Clusters {

    static final String REMOTE_CLUSTER_NAME = "remote_cluster";
    static final String LOCAL_CLUSTER_NAME = "local_cluster";

    static ElasticsearchCluster remoteCluster(Path csvDataPath, Map<String, String> additionalSettings, boolean shared) {
        return remoteCluster(csvDataPath, additionalSettings, shared, null);
    }

    /**
     * @param registerFederationFeature supplier for the ES|QL federation kill-switch system property
     *        ({@value org.elasticsearch.xpack.esql.datasources.Federation#REGISTER_PROPERTY}), re-read on every
     *        (re)start so a test can create federation state while enabled and then bounce the remote with the
     *        switch off. {@code null} leaves the property unset (federation enabled by default).
     */
    static ElasticsearchCluster remoteCluster(
        Path csvDataPath,
        Map<String, String> additionalSettings,
        boolean shared,
        Supplier<String> registerFederationFeature
    ) {
        Version version = distributionVersion("tests.version.remote_cluster");
        var cluster = ElasticsearchCluster.local()
            .name(REMOTE_CLUSTER_NAME)
            .distribution(DistributionType.DEFAULT)
            .version(version)
            .nodes(2)
            .setting("node.roles", "[data,ingest,master]")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("path.repo", csvDataPath::toString)
            .configFile("user-agent/custom-regexes.yml", Resource.fromClasspath("custom-regexes.yml"))
            .configFile("ingest-geoip/GeoLite2-City.mmdb", Resource.fromClasspath("GeoLite2-City.mmdb"))
            .configFile("ingest-geoip/GeoLite2-Country.mmdb", Resource.fromClasspath("GeoLite2-Country.mmdb"))
            .configFile("ingest-geoip/GeoLite2-ASN.mmdb", Resource.fromClasspath("GeoLite2-ASN.mmdb"));
        if (supportRetryOnShardFailures(version) == false) {
            cluster.setting("cluster.routing.rebalance.enable", "none");
        }
        // The local-disk allowlist setting is new in 9.5.0; older BWC nodes reject unknown settings and fail to start,
        // so only set it on nodes that know it. file:// EXTERNAL reads run on the local (coordinating) cluster anyway.
        if (remoteClusterVersion().onOrAfter(org.elasticsearch.Version.V_9_5_0)) {
            cluster.setting("esql.datasource.local_allowed_paths", csvDataPath.toString());
        }
        if (registerFederationFeature != null) {
            cluster.systemProperty(Federation.REGISTER_PROPERTY, registerFederationFeature);
        }
        for (Map.Entry<String, String> entry : additionalSettings.entrySet()) {
            cluster.setting(entry.getKey(), entry.getValue());
        }
        if (shared) {
            cluster.shared(true);
        }
        return cluster.build();
    }

    static ElasticsearchCluster remoteCluster(Map<String, String> additionalSettings) {
        return remoteCluster(CsvTestUtils.createCsvDataDirectory(), additionalSettings, false);
    }

    public static ElasticsearchCluster remoteCluster() {
        return remoteCluster(emptyMap());
    }

    /**
     * A remote cluster whose ES|QL federation kill switch is driven by {@code registerFederationFeature}, re-read on
     * every (re)start. Used by the federation kill-switch tests to create dataset state while enabled and then bounce
     * the remote with the switch engaged.
     */
    public static ElasticsearchCluster remoteCluster(Supplier<String> registerFederationFeature) {
        return remoteCluster(CsvTestUtils.createCsvDataDirectory(), emptyMap(), false, registerFederationFeature);
    }

    public static ElasticsearchCluster localCluster(ElasticsearchCluster remoteCluster) {
        return localCluster(remoteCluster, emptyMap());
    }

    public static ElasticsearchCluster localCluster(ElasticsearchCluster remoteCluster, Map<String, String> additionalSettings) {
        return localCluster(remoteCluster, true, additionalSettings);
    }

    public static ElasticsearchCluster localCluster(
        Path csvDataPath,
        ElasticsearchCluster remoteCluster,
        Map<String, String> additionalSettings,
        boolean shared
    ) {
        return localCluster(csvDataPath, remoteCluster, true, additionalSettings, shared);
    }

    public static ElasticsearchCluster localCluster(ElasticsearchCluster remoteCluster, Boolean skipUnavailable) {
        return localCluster(remoteCluster, skipUnavailable, null);
    }

    public static ElasticsearchCluster localCluster(
        ElasticsearchCluster remoteCluster,
        Boolean skipUnavailable,
        Map<String, String> additionalSettings
    ) {
        return localCluster(CsvTestUtils.createCsvDataDirectory(), remoteCluster, skipUnavailable, additionalSettings, false);
    }

    public static ElasticsearchCluster localCluster(
        Path csvDataPath,
        ElasticsearchCluster remoteCluster,
        Boolean skipUnavailable,
        Map<String, String> additionalSettings,
        boolean shared
    ) {
        Version version = distributionVersion("tests.version.local_cluster");
        var cluster = ElasticsearchCluster.local()
            .name(LOCAL_CLUSTER_NAME)
            .distribution(DistributionType.DEFAULT)
            .version(version)
            .nodes(2)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("node.roles", "[data,ingest,master,remote_cluster_client]")
            .setting("cluster.remote.remote_cluster.seeds", () -> "\"" + remoteCluster.getTransportEndpoint(0) + "\"")
            .setting("cluster.remote.connections_per_cluster", "1")
            .setting("cluster.remote." + REMOTE_CLUSTER_NAME + ".skip_unavailable", skipUnavailable.toString())
            .setting("path.repo", csvDataPath::toString)
            .configFile("user-agent/custom-regexes.yml", Resource.fromClasspath("custom-regexes.yml"))
            .configFile("ingest-geoip/GeoLite2-City.mmdb", Resource.fromClasspath("GeoLite2-City.mmdb"))
            .configFile("ingest-geoip/GeoLite2-Country.mmdb", Resource.fromClasspath("GeoLite2-Country.mmdb"))
            .configFile("ingest-geoip/GeoLite2-ASN.mmdb", Resource.fromClasspath("GeoLite2-ASN.mmdb"));
        if (supportRetryOnShardFailures(version) == false) {
            cluster.setting("cluster.routing.rebalance.enable", "none");
        }
        // The local-disk allowlist setting is new in 9.5.0; older BWC nodes reject unknown settings and fail to start.
        if (localClusterVersion().onOrAfter(org.elasticsearch.Version.V_9_5_0)) {
            cluster.setting("esql.datasource.local_allowed_paths", csvDataPath.toString());
        }
        if (localClusterSupportsInferenceTestService()) {
            cluster.plugin("inference-service-test");
        }
        if (additionalSettings != null && additionalSettings.isEmpty() == false) {
            for (Map.Entry<String, String> entry : additionalSettings.entrySet()) {
                cluster.setting(entry.getKey(), entry.getValue());
            }
        }
        if (shared) {
            cluster.shared(true);
        }
        return cluster.build();
    }

    /**
     * A single-node local cluster with the {@code remote_cluster_client} role but <em>no</em> {@code cluster.remote.*}
     * settings in its config: the remote connection is configured by the test through the cluster settings API. This
     * is required when the remote is restarted mid-test on new ports, because a seed pinned in {@code elasticsearch.yml}
     * cannot be updated through the API, whereas an API-managed seed can be re-pointed after the bounce.
     */
    public static ElasticsearchCluster localClusterForDynamicRemote(Path csvDataPath) {
        Version version = distributionVersion("tests.version.local_cluster");
        var cluster = ElasticsearchCluster.local()
            .name(LOCAL_CLUSTER_NAME)
            .distribution(DistributionType.DEFAULT)
            .version(version)
            .nodes(1)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("node.roles", "[data,ingest,master,remote_cluster_client]")
            .setting("path.repo", csvDataPath::toString)
            .configFile("user-agent/custom-regexes.yml", Resource.fromClasspath("custom-regexes.yml"))
            .configFile("ingest-geoip/GeoLite2-City.mmdb", Resource.fromClasspath("GeoLite2-City.mmdb"))
            .configFile("ingest-geoip/GeoLite2-Country.mmdb", Resource.fromClasspath("GeoLite2-Country.mmdb"))
            .configFile("ingest-geoip/GeoLite2-ASN.mmdb", Resource.fromClasspath("GeoLite2-ASN.mmdb"));
        if (supportRetryOnShardFailures(version) == false) {
            cluster.setting("cluster.routing.rebalance.enable", "none");
        }
        if (localClusterVersion().onOrAfter(org.elasticsearch.Version.V_9_5_0)) {
            cluster.setting("esql.datasource.local_allowed_paths", csvDataPath.toString());
        }
        return cluster.build();
    }

    public static org.elasticsearch.Version localClusterVersion() {
        String prop = System.getProperty("tests.version.local_cluster");
        return prop != null ? org.elasticsearch.Version.fromString(prop) : org.elasticsearch.Version.CURRENT;
    }

    public static org.elasticsearch.Version remoteClusterVersion() {
        String prop = System.getProperty("tests.version.remote_cluster");
        return prop != null ? org.elasticsearch.Version.fromString(prop) : org.elasticsearch.Version.CURRENT;
    }

    public static org.elasticsearch.Version bwcVersion() {
        org.elasticsearch.Version local = localClusterVersion();
        org.elasticsearch.Version remote = remoteClusterVersion();
        return local.before(remote) ? local : remote;
    }

    public static boolean localClusterSupportsInferenceTestService() {
        return isNewToOld();
    }

    /**
     * Returns true if the current task is a "newToOld" BWC test.
     * Checks the tests.task system property to determine the task type.
     */
    private static boolean isNewToOld() {
        String taskName = System.getProperty("tests.task");
        if (taskName == null) {
            return false;
        }
        return taskName.endsWith("#newToOld");
    }

    private static Version distributionVersion(String key) {
        final String val = System.getProperty(key);
        if (val == null) {
            throw new IllegalStateException("System property [" + key + "] is required but not set");
        }
        return Version.fromString(val);
    }

    private static boolean supportRetryOnShardFailures(Version version) {
        return version.onOrAfter(Version.fromString("9.1.0"))
            || (version.onOrAfter(Version.fromString("8.19.0")) && version.before(Version.fromString("9.0.0")));
    }
}
