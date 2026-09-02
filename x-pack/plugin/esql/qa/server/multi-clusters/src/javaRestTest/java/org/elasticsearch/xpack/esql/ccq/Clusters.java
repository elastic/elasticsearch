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
     * @param federationEnabled supplier for the ES|QL federation setting, re-read on every (re)start so a test can
     *        create federation state while enabled and then bounce the remote with federation off. {@code null}
     *        enables federation, which the dataset-bearing suites need.
     */
    static ElasticsearchCluster remoteCluster(
        Path csvDataPath,
        Map<String, String> additionalSettings,
        boolean shared,
        Supplier<String> federationEnabled
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
        // so only set it on nodes that know it, under the name their version knows (see localAllowedPathsSetting).
        // file:// EXTERNAL reads run on the local (coordinating) cluster anyway.
        if (remoteClusterVersion().onOrAfter(org.elasticsearch.Version.V_9_5_0)) {
            cluster.setting(localAllowedPathsSetting(remoteClusterVersion()), csvDataPath.toString());
        }
        if (knowsFederationSetting(remoteClusterVersion())) {
            cluster.setting(Federation.FEDERATION_ENABLED.getKey(), federationEnabled == null ? () -> "true" : federationEnabled);
        }
        if (remoteClusterSupportsInferenceTestService()) {
            cluster.plugin("inference-service-test");
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
     * A remote cluster whose ES|QL federation setting is driven by {@code federationEnabled}, re-read on every
     * (re)start. Used by the federation gate tests to create dataset state while enabled and then bounce the remote
     * with federation off.
     */
    public static ElasticsearchCluster remoteCluster(Supplier<String> federationEnabled) {
        return remoteCluster(CsvTestUtils.createCsvDataDirectory(), emptyMap(), false, federationEnabled);
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
        return localCluster(csvDataPath, remoteCluster, skipUnavailable, additionalSettings, shared, "true");
    }

    /**
     * @param federationEnabled value for the ES|QL federation setting. It is only written on a version that has the
     *        setting, so a suite that depends on the value it passes has to skip when the cluster does not report the
     *        {@code FEDERATION_ENABLED_SETTING} capability.
     */
    public static ElasticsearchCluster localCluster(
        Path csvDataPath,
        ElasticsearchCluster remoteCluster,
        Boolean skipUnavailable,
        Map<String, String> additionalSettings,
        boolean shared,
        String federationEnabled
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
            cluster.setting(localAllowedPathsSetting(localClusterVersion()), csvDataPath.toString());
        }
        if (knowsFederationSetting(localClusterVersion())) {
            cluster.setting(Federation.FEDERATION_ENABLED.getKey(), federationEnabled);
        }
        if (localClusterSupportsInferenceTestService()) {
            cluster.plugin("inference-service-test");
        }
        // Applied to whichever version this cluster runs, so a caller may only pass settings that every BWC version
        // in the matrix knows; anything newer has to go behind a version check like the block above.
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
            cluster.setting(localAllowedPathsSetting(localClusterVersion()), csvDataPath.toString());
        }
        if (knowsFederationSetting(localClusterVersion())) {
            // The local coordinator only asks its remotes to resolve datasets when federation is available here.
            cluster.setting(Federation.FEDERATION_ENABLED.getKey(), "true");
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

    /**
     * The local-disk allowlist setting under the name a cluster of this version knows: it shipped in 9.5.0 as
     * {@code esql.datasource.local_allowed_paths} and was renamed to {@code esql.external.local_allowed_paths} in
     * 9.6.0. A node rejects an unknown setting and fails to start, so each cluster gets its own version's spelling.
     */
    private static String localAllowedPathsSetting(org.elasticsearch.Version version) {
        return version.onOrAfter(org.elasticsearch.Version.V_9_6_0)
            ? "esql.external.local_allowed_paths"
            : "esql.datasource.local_allowed_paths";
    }

    /**
     * Whether a cluster of this version accepts the ES|QL federation setting, which exists as of 9.5.0. A node that
     * predates it rejects an unknown setting and never starts, and it has federation registered unconditionally, so
     * leaving the setting off matches how it behaves in production. Suites that depend on driving the setting skip
     * against such a cluster on the {@code FEDERATION_ENABLED_SETTING} capability.
     */
    private static boolean knowsFederationSetting(org.elasticsearch.Version version) {
        return version.onOrAfter(org.elasticsearch.Version.V_9_5_0);
    }

    public static org.elasticsearch.Version bwcVersion() {
        org.elasticsearch.Version local = localClusterVersion();
        org.elasticsearch.Version remote = remoteClusterVersion();
        return local.before(remote) ? local : remote;
    }

    /**
     * Whether a cluster of this version can host the {@code inference-service-test} plugin. The plugin is built for
     * the current version only, so installing it on a BWC node fails with "was built for Elasticsearch version X but
     * version Y is running" and the node never starts. See
     * <a href="https://github.com/elastic/elasticsearch/issues/115166">#115166</a>.
     */
    private static boolean supportsInferenceTestService(org.elasticsearch.Version version) {
        return version.equals(org.elasticsearch.Version.CURRENT);
    }

    public static boolean localClusterSupportsInferenceTestService() {
        return supportsInferenceTestService(localClusterVersion());
    }

    public static boolean remoteClusterSupportsInferenceTestService() {
        return supportsInferenceTestService(remoteClusterVersion());
    }

    /**
     * Whether both clusters can host the inference test service. Datasets whose mappings reference an inference
     * endpoint by id - {@code semantic_text} above all - can be bulk-loaded into either cluster depending on
     * {@code dataLocation}, so they need the endpoint to exist on both.
     */
    public static boolean bothClustersSupportInferenceTestService() {
        return localClusterSupportsInferenceTestService() && remoteClusterSupportsInferenceTestService();
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
