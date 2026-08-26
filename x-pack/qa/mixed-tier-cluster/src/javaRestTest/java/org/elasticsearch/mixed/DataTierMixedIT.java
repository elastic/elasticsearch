/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.mixed;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

public class DataTierMixedIT extends ESRestTestCase {

    private static final String OLD_CLUSTER_VERSION = System.getProperty("tests.old_cluster_version");

    @ClassRule
    public static ElasticsearchCluster cluster = buildCluster();

    private static ElasticsearchCluster buildCluster() {
        Version oldVersion = Version.fromString(OLD_CLUSTER_VERSION);
        // data_* roles were introduced in 7.10.0, so use 'data' for older versions
        String dataNodeRoles = oldVersion.before("7.10.0") ? "[\"data\"]" : "[\"data_content\", \"data_hot\"]";

        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.watcher.enabled", "false")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            // Node 0 runs the current version from the start, forming a mixed-version cluster with nodes 1 and 2.
            .withNode(node -> node.version(Version.CURRENT).setting("node.roles", "[\"master\"]"))
            .withNode(node -> node.version(oldVersion).setting("node.roles", dataNodeRoles))
            .withNode(node -> node.version(oldVersion).setting("node.roles", "[\"master\"]"))
            .build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testMixedTierCompatibility() throws Exception {
        createIndex("test-index", indexSettings(1, 0).build());
        ensureGreen("test-index");
    }
}
