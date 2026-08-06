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

        var builder = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
            .setting("xpack.security.enabled", "false")
            .setting("xpack.watcher.enabled", "false")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .withNode(node -> node.setting("node.roles", "[\"master\"]"))
            .withNode(node -> node.setting("node.roles", dataNodeRoles))
            .withNode(node -> node.setting("node.roles", "[\"master\"]"));

        return builder.build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testMixedTierCompatibility() throws Exception {
        // Take the first node to the current version to create a mixed-version cluster.
        cluster.upgradeNodeToVersion(0, Version.CURRENT);

        createIndex("test-index", indexSettings(1, 0).build());
        ensureGreen("test-index");
    }
}
