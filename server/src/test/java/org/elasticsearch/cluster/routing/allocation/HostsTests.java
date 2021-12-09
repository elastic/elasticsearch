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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

public class HostsTests extends ESTestCase {

    public void testBuildHosts() {
        final int nodeSize = randomIntBetween(2, 20);
        List<RoutingNode> nodes = new ArrayList<>(nodeSize);
        int nodeSizeOnHost = randomIntBetween(1, 3);
        int nodeCountOnHost = 0;
        int hostId = 0;
        Map<Integer, Set<String>> host2NodesMap = new HashMap<>();
        Map<Integer, Integer> node2HostMap = new HashMap<>();

        for (int nodeId = 0; nodeId < nodeSize; nodeId++) {
            String nodeName = "node" + nodeId;
            String hostName = "host" + hostId;
            String hostAddress = "0.0.0." + hostId;
            DiscoveryNode dn = new DiscoveryNode(
                nodeName,
                nodeName,
                nodeName,
                hostName,
                hostAddress,
                buildNewFakeTransportAddress(),
                emptyMap(),
                Set.of(DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            RoutingNode rn = new RoutingNode(dn.getId(), dn);
            nodes.add(rn);
            nodeCountOnHost++;
            node2HostMap.put(nodeId, hostId);
            host2NodesMap.computeIfAbsent(hostId, k -> new HashSet<>()).add(nodeName);
            if (nodeCountOnHost == nodeSizeOnHost) {
                nodeCountOnHost = 0;
                nodeSizeOnHost = randomIntBetween(1, 3);
            }
        }

        Hosts hosts = new Hosts(nodes);
        assertEquals(host2NodesMap.size(), hosts.getHostCount());
        for (int nodeId = 0; nodeId < nodeSize; nodeId++) {
            Host host = hosts.getHost("node" + nodeId);
            int checkHostId = node2HostMap.get(nodeId);
            assertNotNull(host);
            Set<String> nodeSet = host2NodesMap.get(checkHostId);
            assertEquals(nodeSet.size(), host.getNodeSize());
            for (RoutingNode node : host) {
                nodeSet.contains(node.nodeId());
            }
        }
    }

}
