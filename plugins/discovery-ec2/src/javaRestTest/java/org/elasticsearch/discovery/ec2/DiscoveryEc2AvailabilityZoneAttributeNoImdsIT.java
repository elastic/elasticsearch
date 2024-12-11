/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

public class DiscoveryEc2AvailabilityZoneAttributeNoImdsIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .plugin("discovery-ec2")
        .setting(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testAvailabilityZoneAttribute() throws IOException {
        final var nodesInfoResponse = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_nodes/_all/_none")));
        for (final var nodeId : nodesInfoResponse.evaluateMapKeys("nodes")) {
            assertNull(nodesInfoResponse.evaluateExact("nodes", nodeId, "attributes", "aws_availability_zone"));
        }
    }
}
