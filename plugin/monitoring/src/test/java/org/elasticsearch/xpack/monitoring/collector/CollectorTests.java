/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class CollectorTests extends ESTestCase {

    public void testConvertNullNode() {
        assertEquals(null, Collector.convertNode(randomNonNegativeLong(), null));
    }

    public void testConvertNode() {
        final String name = randomBoolean() ? randomAlphaOfLength(5) : "";
        final String nodeId = randomAlphaOfLength(5);
        final TransportAddress address = buildNewFakeTransportAddress();
        final Version version = randomFrom(Version.V_5_0_1, Version.V_5_3_0, Version.CURRENT);
        final long timestamp = randomNonNegativeLong();

        final Set<DiscoveryNode.Role> roles = new HashSet<>();
        if (randomBoolean()) {
            roles.addAll(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values())));
        }

        final MonitoringDoc.Node expectedNode = new MonitoringDoc.Node(nodeId, address.address().getHostString(), address.toString(),
                                                                       address.getAddress(), name, timestamp);

        DiscoveryNode discoveryNode = new DiscoveryNode(name, nodeId, address, Collections.emptyMap(), roles, version);
        assertEquals(expectedNode, Collector.convertNode(timestamp, discoveryNode));
    }
}
