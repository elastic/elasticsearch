/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;

class Clusters {

    private Clusters() {}

    static ElasticsearchCluster create() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user("x_pack_rest_user", "x-pack-test-password")
            .user("ml_admin", "x-pack-test-password", "minimal,machine_learning_admin,ingest_admin", false)
            .user("ml_user", "x-pack-test-password", "minimal,machine_learning_user", false)
            .user("no_ml", "x-pack-test-password", "minimal", false)
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "true")
            .systemProperty("es.queryable_built_in_roles_enabled", "false")
            .build();
    }
}
