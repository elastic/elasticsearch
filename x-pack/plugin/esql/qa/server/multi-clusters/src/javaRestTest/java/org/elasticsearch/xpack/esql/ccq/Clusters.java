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

import java.util.Map;

import static java.util.Collections.emptyMap;

public class Clusters {

    static final String REMOTE_CLUSTER_NAME = "remote_cluster";
    static final String LOCAL_CLUSTER_NAME = "local_cluster";

    static ElasticsearchCluster remoteCluster(Map<String, String> additionalSettings) {
        Version version = distributionVersion("tests.version.remote_cluster");
        var cluster = ElasticsearchCluster.local()
            .name(REMOTE_CLUSTER_NAME)
            .distribution(DistributionType.DEFAULT)
            .version(version)
            .nodes(2)
            .setting("node.roles", "[data,ingest,master]")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .shared(true);
        if (supportRetryOnShardFailures(version) == false) {
            cluster.setting("cluster.routing.rebalance.enable", "none");
        }
        for (Map.Entry<String, String> entry : additionalSettings.entrySet()) {
            cluster.setting(entry.getKey(), entry.getValue());
        }
        return cluster.build();
    }

    public static ElasticsearchCluster remoteCluster() {
        return remoteCluster(emptyMap());
    }

    public static ElasticsearchCluster localCluster(ElasticsearchCluster remoteCluster) {
        return localCluster(remoteCluster, emptyMap());
    }

    public static ElasticsearchCluster localCluster(ElasticsearchCluster remoteCluster, Map<String, String> additionalSettings) {
        return localCluster(remoteCluster, true, additionalSettings);
    }

    public static ElasticsearchCluster localCluster(ElasticsearchCluster remoteCluster, Boolean skipUnavailable) {
        return localCluster(remoteCluster, skipUnavailable, null);
    }

    public static ElasticsearchCluster localCluster(
        ElasticsearchCluster remoteCluster,
        Boolean skipUnavailable,
        Map<String, String> additionalSettings
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
            .shared(true);
        if (supportRetryOnShardFailures(version) == false) {
            cluster.setting("cluster.routing.rebalance.enable", "none");
        }
        if (localClusterSupportsInferenceTestService()) {
            cluster.plugin("inference-service-test");
        }
        if (additionalSettings != null && additionalSettings.isEmpty() == false) {
            for (Map.Entry<String, String> entry : additionalSettings.entrySet()) {
                cluster.setting(entry.getKey(), entry.getValue());
            }
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
        return val != null ? Version.fromString(val) : Version.CURRENT;
    }

    private static boolean supportRetryOnShardFailures(Version version) {
        return version.onOrAfter(Version.fromString("9.1.0"))
            || (version.onOrAfter(Version.fromString("8.19.0")) && version.before(Version.fromString("9.0.0")));
    }
}
