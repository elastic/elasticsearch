/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.multi_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

public class SqlTestCluster {
    public static ElasticsearchCluster getCluster() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .nodes(2)
            .name("javaRestTest")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.watcher.enabled", "false")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .build();
    }
}
