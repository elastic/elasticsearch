/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject.action;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

public class DeleteProjectActionIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster CLUSTER = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return CLUSTER.getHttpAddresses();
    }

    public void testDeleteProject() throws Exception {
        client().performRequest(new Request("PUT", "/_project/foo"));
        var response = client().performRequest(new Request("DELETE", "/_project/foo"));
        assertOK(response);
        assertAcknowledged(response);
        // TODO: this should assert that the project is actually deleted from the metadata and routing table once the cluster state action
        // is updated.
    }
}
