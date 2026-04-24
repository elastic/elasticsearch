/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.http.HttpHost;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.List;

/**
 * JUnit {@link ElasticsearchCluster} wiring for {@code qa/multi-cluster-search}: two clusters with CCS,
 * forward (current local + BWC remote) or reverse (BWC local + current remote), matching the former Gradle
 * testclusters layout.
 */
public final class MultiClusterSearchClusters {

    private static final String BWC_VERSION_PROP = "tests.multi_cluster.bwc_version";
    private static final String REVERSE_PROP = "tests.multi_cluster.reverse";

    /** Shared across IT classes in the same JVM so clusters start once (builder {@code .shared(true)}). */
    public static final ElasticsearchCluster REMOTE = buildRemote();

    public static final ElasticsearchCluster LOCAL = buildLocal(REMOTE);

    public static final TestRule CLUSTER_RULE = RuleChain.outerRule(REMOTE).around(LOCAL);

    private MultiClusterSearchClusters() {}

    /**
     * Populates legacy {@code tests.rest.*} system properties and seeds the remote with {@code remote_cluster/10_basic.yml}.
     * Call from each IT class {@code @BeforeClass} (after {@code @ClassRule} has started clusters).
     */
    public static void beforeSuite() throws Exception {
        installDynamicTestClusterProperties();
        MultiClusterRemoteYamlSeed.ensureSeeded();
    }

    public static void installDynamicTestClusterProperties() {
        System.setProperty("tests.rest.cluster", LOCAL.getHttpAddresses());
        System.setProperty("tests.rest.remote_cluster", REMOTE.getHttpAddresses());
        System.setProperty("tests.rest.remote_cluster_version", remoteSemanticVersionForYamlSkips().toString());
        System.setProperty("tests.rest.suite", "multi_cluster");
    }

    private static Version remoteSemanticVersionForYamlSkips() {
        return reverse() ? Version.CURRENT : bwcVersion();
    }

    private static boolean reverse() {
        return Boolean.parseBoolean(System.getProperty(REVERSE_PROP, "false"));
    }

    private static Version bwcVersion() {
        return Version.fromString(requiredProp(BWC_VERSION_PROP));
    }

    private static Version remoteVersion() {
        return reverse() ? Version.CURRENT : bwcVersion();
    }

    private static Version localVersion() {
        return reverse() ? bwcVersion() : Version.CURRENT;
    }

    private static ElasticsearchCluster buildRemote() {
        var builder = ElasticsearchCluster.local()
            .name("multi_cluster_remote")
            .distribution(DistributionType.DEFAULT)
            .nodes(2)
            .setting("node.roles", "[data,ingest,master]")
            .setting("xpack.security.enabled", "false")
            .module("aggregations")
            .module("parent-join")
            .feature(FeatureFlag.TIME_SERIES_MODE)
            .shared(true);
        applyVersionIfNotCurrent(builder, remoteVersion());
        return builder.build();
    }

    private static ElasticsearchCluster buildLocal(ElasticsearchCluster remote) {
        var builder = ElasticsearchCluster.local()
            .name("multi_cluster_local")
            .distribution(DistributionType.DEFAULT)
            .nodes(1)
            .setting("node.roles", "[data,ingest,master,remote_cluster_client]")
            .setting("cluster.remote.connections_per_cluster", "1")
            .setting("cluster.remote.my_remote_cluster.seeds", () -> "\"" + remote.getTransportEndpoint(0) + "\"")
            .setting("cluster.remote.my_remote_cluster.skip_unavailable", "false")
            .setting("xpack.security.enabled", "false")
            .module("aggregations")
            .module("parent-join")
            .feature(FeatureFlag.TIME_SERIES_MODE)
            .shared(true);
        applyVersionIfNotCurrent(builder, localVersion());
        return builder.build();
    }

    private static <T extends ElasticsearchCluster> void applyVersionIfNotCurrent(LocalClusterSpecBuilder<T> builder, Version version) {
        if (version.equals(Version.CURRENT) == false) {
            builder.version(version);
            if (supportRetryOnShardFailures(version) == false) {
                builder.setting("cluster.routing.rebalance.enable", "none");
            }
        }
    }

    private static boolean supportRetryOnShardFailures(Version version) {
        // Retry on shard failures was introduced in 8.19 (backport) and 9.1. Older nodes cannot handle
        // shard rebalancing during CCS tests, so we disable it for them to avoid spurious failures.
        return version.onOrAfter(Version.fromString("9.1.0"))
            || (version.onOrAfter(Version.fromString("8.19.0")) && version.before(Version.fromString("9.0.0")));
    }

    /**
     * Parses the {@code tests.rest.remote_cluster} system property into a list of {@link HttpHost} objects.
     */
    public static List<HttpHost> remoteClusterHosts() {
        String address = System.getProperty("tests.rest.remote_cluster");
        if (address == null || address.isBlank()) {
            throw new IllegalStateException("Required system property [tests.rest.remote_cluster] is not set");
        }
        String[] parts = address.split(",");
        List<HttpHost> hosts = new ArrayList<>(parts.length);
        for (String part : parts) {
            int portSep = part.lastIndexOf(':');
            if (portSep < 0) {
                throw new IllegalArgumentException("Illegal cluster address [" + part + "]");
            }
            hosts.add(new HttpHost(part.substring(0, portSep), Integer.parseInt(part.substring(portSep + 1))));
        }
        return hosts;
    }

    private static String requiredProp(String key) {
        String v = System.getProperty(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Required system property [" + key + "] is not set");
        }
        return v;
    }
}
