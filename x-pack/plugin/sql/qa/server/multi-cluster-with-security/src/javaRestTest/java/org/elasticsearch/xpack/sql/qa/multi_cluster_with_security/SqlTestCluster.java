/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.multi_cluster_with_security;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

public class SqlTestCluster {
    public static ElasticsearchCluster getCluster(String remoteAddress) {

        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("javaRestTest")
            .setting("cluster.name", "javaRestTest")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.watcher.enabled", "false")
            .setting("cluster.remote.my_remote_cluster.seeds", remoteAddress)
            .setting("cluster.remote.connections_per_cluster", "1")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.autoconfiguration.enabled", "false")
            .user("test_user", "x-pack-test-password")
            .plugin(":x-pack:qa:freeze-plugin")
            .build();
    }
}
