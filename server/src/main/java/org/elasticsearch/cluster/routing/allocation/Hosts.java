/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.Strings;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * {@link Hosts} hosts records the mapping relationship between nodes and hosts,
 * which makes it easy to find nodes on the same host
 */
public class Hosts {
    private final Map<String, Host> node2Host;
    private final int hostCount;

    public Hosts(Iterable<RoutingNode> nodes) {
        Set<HostBuilder> hostBuilderSet = new HashSet<>();
        Map<String, HostBuilder> hostAddressMap = new HashMap<>();
        for (RoutingNode checkNode : nodes) {
            HostBuilder hostBuilder = getOrCreateHost(checkNode, hostAddressMap);
            if (hostBuilder != null) {
                hostBuilder.addNode(checkNode);
                hostBuilderSet.add(hostBuilder);
            }
        }
        this.hostCount = hostBuilderSet.size();
        Map<String, Host> tmpNode2Host = new HashMap<>();
        for (HostBuilder hostBuilder : hostBuilderSet) {
            Host host = hostBuilder.build();
            for (RoutingNode node : host) {
                tmpNode2Host.put(node.nodeId(), host);
            }
        }
        this.node2Host = Collections.unmodifiableMap(tmpNode2Host);
    }

    private HostBuilder getOrCreateHost(RoutingNode node, Map<String, HostBuilder> hostAddressMap) {
        // skip if the DiscoveryNode of a deleted RoutingNode is empty
        if (node.node() == null) {
            return null;
        }

        String hostAddress = node.node().getHostAddress();
        assert Strings.hasLength(node.node().getHostAddress()) : node;
        HostBuilder hostBuilder = hostAddressMap.get(hostAddress);

        if (hostBuilder != null) {
            return hostBuilder;
        } else {
            HostBuilder newHostBuilder = new HostBuilder();
            hostAddressMap.put(hostAddress, newHostBuilder);
            return newHostBuilder;
        }
    }

    public Host getHost(String nodeId) {
        return node2Host.get(nodeId);
    }

    public int getHostCount() {
        return hostCount;
    }
}
