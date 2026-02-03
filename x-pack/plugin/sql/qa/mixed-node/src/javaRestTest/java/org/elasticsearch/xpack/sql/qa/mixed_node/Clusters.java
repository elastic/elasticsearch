/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.mixed_node;

import org.apache.http.HttpHost;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;

import java.util.stream.IntStream;

public class Clusters {

    private static final String OLD_CLUSTER_VERSION_STRING = System.getProperty("tests.old_cluster_version");

    /**
     * The old cluster version with -SNAPSHOT suffix stripped (for version comparison).
     */
    static final String OLD_CLUSTER_VERSION = OLD_CLUSTER_VERSION_STRING.replace("-SNAPSHOT", "");

    static HttpHost[] oldNodeAddresses(ElasticsearchCluster cluster) {
        return nodeAddresses(cluster, 0, 1);
    }

    static HttpHost[] newNodeAddresses(ElasticsearchCluster cluster) {
        return nodeAddresses(cluster, 2);
    }

    private static HttpHost[] nodeAddresses(ElasticsearchCluster cluster, int... indices) {
        return IntStream.of(indices).mapToObj(i -> HttpHost.create(cluster.getHttpAddress(i))).toArray(HttpHost[]::new);
    }

    /**
     * Creates a mixed-version cluster with 3 nodes: 2 old version nodes (indices 0, 1) and 1 current version node (index 2).
     */
    public static ElasticsearchCluster mixedVersionCluster() {
        boolean isDetachedVersion = System.getProperty("tests.bwc.refspec.main") != null;
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .withNode(node -> node.version(OLD_CLUSTER_VERSION_STRING, isDetachedVersion)) // index 0
            .withNode(node -> node.version(OLD_CLUSTER_VERSION_STRING, isDetachedVersion)) // index 1
            .withNode(node -> node.version(Version.CURRENT)) // index 2
            .setting("xpack.security.enabled", "false")
            .setting("xpack.watcher.enabled", "false")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .build();
    }
}
