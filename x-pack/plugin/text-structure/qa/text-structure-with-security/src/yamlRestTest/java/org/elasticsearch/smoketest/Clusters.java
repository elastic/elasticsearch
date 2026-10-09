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

/**
 * Builds the security-enabled cluster shared by the text structure YAML suites. Each concrete test class declares
 * its own {@code @ClassRule} using this factory so that the suites remain independent even though they run the same
 * YAML tests under different user credentials.
 */
class Clusters {

    private Clusters() {}

    static ElasticsearchCluster create() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user("x_pack_rest_user", "x-pack-test-password")
            .user("text_structure_user", "x-pack-test-password", "minimal,monitor_text_structure", false)
            .user("no_text_structure", "x-pack-test-password", "minimal", false)
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "true")
            .build();
    }
}
