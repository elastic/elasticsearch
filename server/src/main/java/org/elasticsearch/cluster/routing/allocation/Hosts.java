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

import java.util.HashMap;
import java.util.Map;

/**
 * {@link Hosts} hosts records the mapping relationship between nodes and hosts,
 * which makes it easy to find nodes on the same host
 */
public class Hosts {
    private final Map<String, ModelHost> node2Host = new HashMap<>();
    private final Map<String, ModelHost> hostNameMap = new HashMap<>();
    private final Map<String, ModelHost> hostAddressMap = new HashMap<>();

    public Hosts(Iterable<RoutingNode> nodesToShards) {
        for (RoutingNode checkNode : nodesToShards) {
            ModelHost host = getOrCreateHost(checkNode);
            if (host != null) {
                host.addNode(checkNode);
                node2Host.put(checkNode.nodeId(), host);
            }
        }
    }

    private ModelHost getOrCreateHost(RoutingNode node) {
        // skip if the DiscoveryNode of a deleted RoutingNode is empty
        if (node.node() == null) {
            return null;
        }
        String hostName = node.node().getHostName();
        String hostAddress = node.node().getHostAddress();
        ModelHost host = null;
        if (Strings.hasLength(hostAddress)) {
            host = hostAddressMap.get(hostAddress);
        } else if (Strings.hasLength(hostName)) {
            host = hostNameMap.get(hostName);
        }
        if (host != null) {
            return host;
        } else {
            ModelHost newHost = new ModelHost();
            hostAddressMap.put(hostAddress, newHost);
            hostNameMap.put(hostName, newHost);
            return newHost;
        }
    }

    public ModelHost getHost(String nodeId) {
        return node2Host.get(nodeId);
    }
}
