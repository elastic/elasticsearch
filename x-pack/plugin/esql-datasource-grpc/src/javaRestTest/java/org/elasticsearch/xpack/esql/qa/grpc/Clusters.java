/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.grpc;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

/**
 * Cluster configuration for Arrow Flight (gRPC) integration tests.
 */
class Clusters {

    static ElasticsearchCluster testCluster() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .shared(true)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.ml.enabled", "false")
            .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .jvmArg("-Darrow.allocation.manager.type=Unsafe")
            .build();
    }
}
