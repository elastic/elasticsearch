/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;

public class Clusters {
    public static ElasticsearchCluster mixedVersionCluster() {
        Version oldVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .withNode(node -> node.version(oldVersion))
            .withNode(node -> node.version(Version.CURRENT))
            .withNode(node -> node.version(oldVersion))
            .withNode(node -> node.version(Version.CURRENT))
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("cluster.routing.rebalance.enable", "none") // disable relocation until we have retry in ESQL
            .build();
    }
}
