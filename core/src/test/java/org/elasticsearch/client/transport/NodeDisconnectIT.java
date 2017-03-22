/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.MockTransportClient;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.client.transport.TransportClient.CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL;

public class NodeDisconnectIT  extends ESIntegTestCase {

    public void testNotifyOnDisconnect() throws IOException {
        internalCluster().ensureAtLeastNumDataNodes(2);

        final Set<DiscoveryNode> disconnectedNodes = Collections.synchronizedSet(new HashSet<>());
        try (TransportClient client = new MockTransportClient(Settings.builder()
            .put("cluster.name", internalCluster().getClusterName())
            .put(CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL.getKey(), "1h") // disable sniffing for better control
            .build(),
            Collections.emptySet(), (n, e) -> disconnectedNodes.add(n))) {
            for (TransportService service : internalCluster().getInstances(TransportService.class)) {
                client.addTransportAddress(service.boundAddress().publishAddress());
            }
            internalCluster().stopRandomDataNode();
            for (int i = 0; i < 20; i++) { // fire up requests such that we hit the node and pass it to the listener
                client.admin().cluster().prepareState().get();
            }
            assertEquals(1, disconnectedNodes.size());
        }
        assertEquals(1, disconnectedNodes.size());
    }

    public void testNotifyOnDisconnectInSniffer() throws IOException {
        internalCluster().ensureAtLeastNumDataNodes(2);

        final Set<DiscoveryNode> disconnectedNodes = Collections.synchronizedSet(new HashSet<>());
        try (TransportClient client = new MockTransportClient(Settings.builder()
            .put("cluster.name", internalCluster().getClusterName()).build(), Collections.emptySet(), (n, e) -> disconnectedNodes.add(n))) {
            int numNodes = 0;
            for (TransportService service : internalCluster().getInstances(TransportService.class)) {
                numNodes++;
                client.addTransportAddress(service.boundAddress().publishAddress());
            }
            Set<TransportAddress> discoveryNodes = client.connectedNodes().stream().map(n -> n.getAddress()).collect(Collectors.toSet());
            assertEquals(numNodes, discoveryNodes.size());
            assertEquals(0, disconnectedNodes.size());
            internalCluster().stopRandomDataNode();
            client.getNodesService().doSample();
            assertEquals(1, disconnectedNodes.size());
            assertTrue(discoveryNodes.contains(disconnectedNodes.stream().findAny().get().getAddress()));
        }
        assertEquals(1, disconnectedNodes.size());
    }
}
