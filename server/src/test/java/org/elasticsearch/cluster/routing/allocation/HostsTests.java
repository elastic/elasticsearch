/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;

public class HostsTests extends ESTestCase {

    public void testBuildHosts() {
        DiscoveryNode dn1 = new DiscoveryNode(
            "node1",
            "node1",
            "node1",
            "host1",
            "host1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        DiscoveryNode dn2 = new DiscoveryNode(
            "node2",
            "node2",
            "node2",
            "host1",
            "host1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        List<RoutingNode> nodes = List.of(new RoutingNode(dn1.getId(), dn1), new RoutingNode(dn2.getId(), dn2));
        Hosts hosts = new Hosts(nodes);
        assertNotNull(hosts.getHost("node1"));
        int nodeCount = 0;
        for (RoutingNode node : hosts.getHost("node1")) {
            nodeCount++;
            assertThat(node.nodeId(), anyOf(equalTo("node1"), equalTo("node2")));
        }
        assertEquals(2, nodeCount);
    }

    public void testBuildHostsWithoutAddressAndName() {
        DiscoveryNode dn1 = new DiscoveryNode(
            "node1",
            "node1",
            "node1",
            "",
            "",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        DiscoveryNode dn2 = new DiscoveryNode(
            "node2",
            "node2",
            "node2",
            "",
            "",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        List<RoutingNode> nodes = List.of(new RoutingNode(dn1.getId(), dn1), new RoutingNode(dn2.getId(), dn2));
        Hosts hosts = new Hosts(nodes);
        assertNull(hosts.getHost("node1"));
    }

}
