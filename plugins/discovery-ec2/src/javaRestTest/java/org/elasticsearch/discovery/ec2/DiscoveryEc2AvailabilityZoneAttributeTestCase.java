/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import fixture.aws.imds.Ec2ImdsHttpFixture;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public abstract class DiscoveryEc2AvailabilityZoneAttributeTestCase extends ESRestTestCase {

    private static final Set<String> createdAvailabilityZones = ConcurrentCollections.newConcurrentSet();

    protected static String getAvailabilityZone() {
        final var zoneName = randomIdentifier();
        createdAvailabilityZones.add(zoneName);
        return zoneName;
    }

    protected static ElasticsearchCluster buildCluster(Supplier<String> imdsFixtureAddressSupplier) {
        return ElasticsearchCluster.local()
            .plugin("discovery-ec2")
            .setting(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), "true")
            .systemProperty(Ec2ImdsHttpFixture.ENDPOINT_OVERRIDE_SYSPROP_NAME_SDK2, imdsFixtureAddressSupplier)
            .build();
    }

    public void testAvailabilityZoneAttribute() throws IOException {
        final var nodesInfoResponse = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_nodes/_all/_none")));
        for (final var nodeId : nodesInfoResponse.evaluateMapKeys("nodes")) {
            assertThat(
                createdAvailabilityZones,
                Matchers.hasItem(
                    Objects.requireNonNull(nodesInfoResponse.<String>evaluateExact("nodes", nodeId, "attributes", "aws_availability_zone"))
                )
            );
        }
    }
}
