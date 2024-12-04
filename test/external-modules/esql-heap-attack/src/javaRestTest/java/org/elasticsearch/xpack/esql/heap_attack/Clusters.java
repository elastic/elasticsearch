/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

public class Clusters {
    static ElasticsearchCluster remoteCluster() {
        var spec = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("remote_cluster")
            .nodes(2)
            .module("test-esql-heap-attack")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .jvmArg("-Xmx512m");
        String javaVersion = JvmInfo.jvmInfo().version();
        if (javaVersion.equals("20") || javaVersion.equals("21")) {
            // see https://github.com/elastic/elasticsearch/issues/99592
            spec.jvmArg("-XX:+UnlockDiagnosticVMOptions -XX:+G1UsePreventiveGC");
        }
        return spec.build();
    }

    static ElasticsearchCluster localCluster(ElasticsearchCluster remoteCluster) {
        var spec = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("local_cluster")
            .nodes(2)
            .module("test-esql-heap-attack")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("node.roles", "[data,ingest,master,remote_cluster_client]")
            .setting("cluster.remote.remote_cluster.seeds", () -> "\"" + remoteCluster.getTransportEndpoint(0) + "\"")
            .setting("cluster.remote.connections_per_cluster", "1")
            .setting("cluster.routing.rebalance.enable", "none")
            .jvmArg("-Xmx512m");
        String javaVersion = JvmInfo.jvmInfo().version();
        if (javaVersion.equals("20") || javaVersion.equals("21")) {
            // see https://github.com/elastic/elasticsearch/issues/99592
            spec.jvmArg("-XX:+UnlockDiagnosticVMOptions -XX:+G1UsePreventiveGC");
        }
        return spec.build();
    }
}
