/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;

public class EqlSecurityTestCluster {
    public static ElasticsearchCluster getCluster() {
        return ElasticsearchCluster.local()
            .nodes(2)
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.license.self_generated.type", "basic")
            .setting("xpack.monitoring.collection.enabled", "true")
            .setting("xpack.security.enabled", "true")
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user("test-admin", "x-pack-test-password", "test-admin", false)
            .user("user1", "x-pack-test-password", "user1", false)
            .user("user2", "x-pack-test-password", "user2", false)
            .build();
    }
}
