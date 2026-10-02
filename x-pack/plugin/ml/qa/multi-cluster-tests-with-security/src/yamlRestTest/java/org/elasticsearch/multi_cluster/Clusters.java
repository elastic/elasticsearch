/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.multi_cluster;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.function.Supplier;

/**
 * Cluster definitions for the ML multi-cluster-with-security tests.
 *
 * <p>Two security-enabled clusters are used to exercise machine learning over cross-cluster search:
 * a {@code remote} cluster that holds the source data and a {@code mixed} cluster that queries it via a
 * configured remote connection. To preserve the cross-cluster-search compatibility coverage of the original
 * Gradle {@code testClusters} definition, the remote cluster runs on the minimum wire-compatible version while the
 * mixed (querying) cluster runs on the current version.
 */
class Clusters {

    static final String TEST_USER = "test_user";
    static final String TEST_PASSWORD = "x-pack-test-password";

    static final String REMOTE_CLUSTER_ALIAS = "my_remote_cluster";

    private static final String CCS_COMPAT_VERSION = System.getProperty("tests.ccs_compat_version");

    static ElasticsearchCluster remoteCluster() {
        boolean detached = ESRestTestCase.isOldClusterDetachedVersion();
        return ElasticsearchCluster.local()
            .name("remote-cluster")
            // The default distribution is required both for security/trial licensing and because a cluster pinned to a
            // prior version cannot use the integ-test distribution (only published for the current version).
            .distribution(DistributionType.DEFAULT)
            // The whole remote cluster runs on the minimum wire-compatible version to exercise cross-cluster search
            // from a current-version cluster against an older remote, matching the original `versions = [ccsCompatVersion,
            // project.version]` topology which started every node on the first (old) version and never upgraded.
            .version(CCS_COMPAT_VERSION, detached)
            .nodes(2)
            .setting("node.roles", "[data,ingest,master,ml]")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.watcher.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .user(TEST_USER, TEST_PASSWORD)
            .build();
    }

    static ElasticsearchCluster mixedCluster(Supplier<String> remoteSeeds) {
        return ElasticsearchCluster.local()
            .name("mixed-cluster")
            .distribution(DistributionType.DEFAULT)
            .nodes(2)
            .setting("node.roles", "[data,ingest,master,ml]")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.watcher.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".seeds", remoteSeeds)
            .setting("cluster.remote.connections_per_cluster", "1")
            .user(TEST_USER, TEST_PASSWORD)
            .build();
    }

}
