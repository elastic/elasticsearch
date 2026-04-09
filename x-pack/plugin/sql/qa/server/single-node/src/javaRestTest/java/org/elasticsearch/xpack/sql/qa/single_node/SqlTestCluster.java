/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.single_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

public class SqlTestCluster {
    public static String CLUSTER_NAME = "javaRestTest";

    public static ElasticsearchCluster getCluster() {
        var settings = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name(CLUSTER_NAME)
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.watcher.enabled", "false")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial");

        return settings.build();
    }
}
