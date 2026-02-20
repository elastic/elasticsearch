/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Cluster definitions for multi-cluster search security tests.
 */
class Clusters {

    static final String TEST_USER = "test_user";
    static final String TEST_PASSWORD = "x-pack-test-password";

    static final String REMOTE_CLUSTER_ALIAS = "my_remote_cluster";

    static ElasticsearchCluster fulfillingCluster(String licenseType) {
        return ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", "true")
            .setting("xpack.license.self_generated.type", licenseType)
            .setting("xpack.ml.enabled", "false")
            .user(TEST_USER, TEST_PASSWORD)
            .build();
    }

    static ElasticsearchCluster queryingCluster(String licenseType, BooleanSupplier proxyMode, Supplier<String> remoteEndpoint) {
        return ElasticsearchCluster.local()
            .name("querying-cluster")
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", "true")
            .setting("xpack.license.self_generated.type", licenseType)
            .setting("xpack.ml.enabled", "false")
            .setting("cluster.remote.connections_per_cluster", "1")
            .setting("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".skip_unavailable", "false")
            .settings(spec -> {
                final Map<String, String> settings = new HashMap<>();
                if (proxyMode.getAsBoolean()) {
                    settings.put("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".mode", "proxy");
                    settings.put("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".proxy_address", "\"" + remoteEndpoint.get() + "\"");
                } else {
                    settings.put("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".mode", "sniff");
                    settings.put("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".seeds", "\"" + remoteEndpoint.get() + "\"");
                }
                return settings;
            })
            .user(TEST_USER, TEST_PASSWORD)
            .build();
    }

}
