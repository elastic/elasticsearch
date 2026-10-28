/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;

import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.inFipsJvm;

public class Clusters {

    public static ElasticsearchCluster oldVersionCluster(String user, String pass) {
        return clusterBuilder(user, pass).build();
    }

    public static ElasticsearchCluster oldVersionClusterWithLogsDisabled(String user, String pass, Supplier<Boolean> useTrialLicense) {
        var cluster = clusterBuilder(user, pass);

        // FIPS mode requires at least a trial license (basic license is not supported in FIPS mode)
        boolean useTrial = inFipsJvm() || useTrialLicense.get();

        // LogsDB is enabled by default for data streams matching the logs-*-* pattern, and since we upgrade from standard to logsdb,
        // we need to start with logsdb disabled, then later enable it and rollover
        cluster.setting("cluster.logsdb.enabled", "false")
            .setting("stack.templates.enabled", "false")
            .module("constant-keyword")
            .module("data-streams")
            .module("mapper-extras")
            .module("x-pack-aggregate-metric")
            .module("x-pack-stack")
            .setting("xpack.security.autoconfiguration.enabled", "false")
            .setting("xpack.license.self_generated.type", useTrial ? "trial" : "basic");

        return cluster.build();
    }

    private static LocalClusterSpecBuilder<ElasticsearchCluster> clusterBuilder(String user, String pass) {
        // define versions
        String oldVersionString = System.getProperty("tests.old_cluster_version");
        Version oldVersion = Version.fromString(oldVersionString);
        boolean isDetachedVersion = System.getProperty("tests.bwc.refspec.main") != null;

        // define cluster
        var cluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", "true")
            .user(user, pass)
            .keystore("bootstrap.password", pass)
            .jvmArg("-da:org.elasticsearch.index.translog.TranslogWriter")
            .setting("xpack.license.self_generated.type", "trial");

        // add nodes
        int numNodes = Integer.parseInt(System.getProperty("tests.num_nodes", "3"));
        for (int i = 0; i < numNodes; i++) {
            cluster.withNode(node -> node.version(oldVersionString, isDetachedVersion));
        }

        if (supportRetryOnShardFailures(oldVersion) == false) {
            cluster.setting("cluster.routing.rebalance.enable", "none");
        }

        return cluster;
    }

    private static boolean supportRetryOnShardFailures(Version version) {
        return version.onOrAfter(Version.fromString("9.1.0"))
            || (version.onOrAfter(Version.fromString("8.19.0")) && version.before(Version.fromString("9.0.0")));
    }
}
