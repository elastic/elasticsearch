/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

import java.util.StringJoiner;

public class RemoteClusterTestUtils {
    public static final String REMOTE_CLUSTER_NAME = "my_remote_cluster";

    public static final ElasticsearchCluster REMOTE_CLUSTER = ElasticsearchCluster.local()
        .name(REMOTE_CLUSTER_NAME)
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .setting("node.roles", "[data,ingest,master]")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .user("test_user", "x-pack-test-password")
        .shared(true)
        .build();

    public static final ElasticsearchCluster LOCAL_CLUSTER = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("cluster.remote.my_remote_cluster.seeds", () -> "\"" + REMOTE_CLUSTER.getTransportEndpoint(0) + "\"")
        .setting("cluster.remote.connections_per_cluster", "1")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .user("test_user", "x-pack-test-password")
        .shared(true)
        .build();

    public static String remoteClusterIndex(String indexName) {
        return REMOTE_CLUSTER_NAME + ":" + indexName;
    }

    public static String remoteClusterPattern(String pattern) {
        StringJoiner sj = new StringJoiner(",");
        for (String index : pattern.split(",")) {
            sj.add(remoteClusterIndex(index));
        }
        return sj.toString();
    }
}
