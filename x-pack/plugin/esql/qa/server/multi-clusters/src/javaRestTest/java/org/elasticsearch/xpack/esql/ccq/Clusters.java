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

public class Clusters {

    static final String REMOTE_CLUSTER_NAME = "remote_cluster";
    static final String LOCAL_CLUSTER_NAME = "local_cluster";

    public static ElasticsearchCluster remoteCluster() {
        return ElasticsearchCluster.local()
            .name(REMOTE_CLUSTER_NAME)
            .distribution(DistributionType.DEFAULT)
            .version(distributionVersion("tests.version.remote_cluster"))
            .nodes(2)
            .setting("node.roles", "[data,ingest,master]")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .shared(true)
            .setting("cluster.routing.rebalance.enable", "none")
            .build();
    }

    public static ElasticsearchCluster localCluster(ElasticsearchCluster remoteCluster) {
        return ElasticsearchCluster.local()
            .name(LOCAL_CLUSTER_NAME)
            .distribution(DistributionType.DEFAULT)
            .version(distributionVersion("tests.version.local_cluster"))
            .nodes(2)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("node.roles", "[data,ingest,master,remote_cluster_client]")
            .setting("cluster.remote.remote_cluster.seeds", () -> "\"" + remoteCluster.getTransportEndpoint(0) + "\"")
            .setting("cluster.remote.connections_per_cluster", "1")
            .shared(true)
            .setting("cluster.routing.rebalance.enable", "none")
            .build();
    }

    public static org.elasticsearch.Version localClusterVersion() {
        String prop = System.getProperty("tests.version.local_cluster");
        return prop != null ? org.elasticsearch.Version.fromString(prop) : org.elasticsearch.Version.CURRENT;
    }

    public static org.elasticsearch.Version remoteClusterVersion() {
        String prop = System.getProperty("tests.version.remote_cluster");
        return prop != null ? org.elasticsearch.Version.fromString(prop) : org.elasticsearch.Version.CURRENT;
    }

    private static Version distributionVersion(String key) {
        final String val = System.getProperty(key);
        return val != null ? Version.fromString(val) : Version.CURRENT;
    }
}
