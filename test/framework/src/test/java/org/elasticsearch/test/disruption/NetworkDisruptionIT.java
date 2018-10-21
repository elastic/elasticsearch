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

package org.elasticsearch.test.disruption;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class NetworkDisruptionIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    public void testNetworkPartitionWithNodeShutdown() throws IOException {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String[] nodeNames = internalCluster().getNodeNames();
        NetworkDisruption networkDisruption =
                new NetworkDisruption(new TwoPartitions(nodeNames[0], nodeNames[1]), new NetworkDisruption.NetworkUnresponsive());
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeNames[0]));
        internalCluster().clearDisruptionScheme();
    }

    public void testNetworkPartitionRemovalRestoresConnections() throws IOException {
        Set<String> nodes = new HashSet<>();
        nodes.addAll(Arrays.asList(internalCluster().getNodeNames()));
        nodes.remove(internalCluster().getMasterName());
        if (nodes.size() <= 2) {
            internalCluster().ensureAtLeastNumDataNodes(3 - nodes.size());
            nodes.addAll(Arrays.asList(internalCluster().getNodeNames()));
            nodes.remove(internalCluster().getMasterName());
        }
        Set<String> side1 = new HashSet<>(randomSubsetOf(randomIntBetween(1, nodes.size() - 1), nodes));
        Set<String> side2 = new HashSet<>(nodes);
        side2.removeAll(side1);
        assertThat(side2.size(), greaterThanOrEqualTo(1));
        NetworkDisruption networkDisruption = new NetworkDisruption(new TwoPartitions(side1, side2),
            new NetworkDisruption.NetworkDisconnect());
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        // sends some requests
        client(randomFrom(side1)).admin().cluster().prepareNodesInfo().get();
        client(randomFrom(side2)).admin().cluster().prepareNodesInfo().get();
        internalCluster().clearDisruptionScheme();
        // check all connections are restore
        for (String nodeA : side1) {
            for (String nodeB : side2) {
                TransportService serviceA = internalCluster().getInstance(TransportService.class, nodeA);
                TransportService serviceB = internalCluster().getInstance(TransportService.class, nodeB);
                assertTrue(nodeA + " is not connected to " + nodeB, serviceA.nodeConnected(serviceB.getLocalNode()));
                assertTrue(nodeB + " is not connected to " + nodeA, serviceB.nodeConnected(serviceA.getLocalNode()));
            }
        }
    }

}
