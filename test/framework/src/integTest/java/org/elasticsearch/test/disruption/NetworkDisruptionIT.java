/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.disruption;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class NetworkDisruptionIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    private static final Settings DISRUPTION_TUNED_SETTINGS = Settings.builder()
        .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "2s")
        .build();

    /**
     * Creates 3 to 5 mixed-node cluster and splits it into 2 parts.
     * The first part is guaranteed to have at least the majority of the nodes,
     * so that master could be elected on this side.
     */
    private Tuple<Set<String>, Set<String>> prepareDisruptedCluster() {
        int numOfNodes = randomIntBetween(3, 5);
        internalCluster().setBootstrapMasterNodeIndex(numOfNodes - 1);
        Set<String> nodes = new HashSet<>(internalCluster().startNodes(numOfNodes, DISRUPTION_TUNED_SETTINGS));
        ensureGreen();
        assertThat(nodes.size(), greaterThanOrEqualTo(3));
        int majority = nodes.size() / 2 + 1;
        Set<String> side1 = new HashSet<>(randomSubsetOf(randomIntBetween(majority, nodes.size() - 1), nodes));
        assertThat(side1.size(), greaterThanOrEqualTo(majority));
        Set<String> side2 = new HashSet<>(nodes);
        side2.removeAll(side1);
        assertThat(side2.size(), greaterThanOrEqualTo(1));
        NetworkDisruption networkDisruption = new NetworkDisruption(new TwoPartitions(side1, side2), NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        return Tuple.tuple(side1, side2);
    }

    public void testClearDisruptionSchemeWhenNodeIsDown() throws IOException {
        Tuple<Set<String>, Set<String>> sides = prepareDisruptedCluster();

        internalCluster().stopNode(randomFrom(sides.v2()));
        internalCluster().clearDisruptionScheme();
    }

    public void testNetworkPartitionRemovalRestoresConnections() throws Exception {
        Tuple<Set<String>, Set<String>> sides = prepareDisruptedCluster();
        Set<String> side1 = sides.v1();
        Set<String> side2 = sides.v2();

        // sends some requests to the majority side part
        client(randomFrom(side1)).admin().cluster().prepareNodesInfo().get();
        internalCluster().clearDisruptionScheme();
        // check all connections are restored
        for (String nodeA : side1) {
            for (String nodeB : side2) {
                TransportService serviceA = internalCluster().getInstance(TransportService.class, nodeA);
                TransportService serviceB = internalCluster().getInstance(TransportService.class, nodeB);
                // TODO assertBusy should not be here, see https://github.com/elastic/elasticsearch/issues/38348
                assertBusy(() -> {
                    assertTrue(nodeA + " is not connected to " + nodeB, serviceA.nodeConnected(serviceB.getLocalNode()));
                    assertTrue(nodeB + " is not connected to " + nodeA, serviceB.nodeConnected(serviceA.getLocalNode()));
                });
            }
        }
    }

    public void testTransportRespondsEventually() throws InterruptedException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().ensureAtLeastNumDataNodes(randomIntBetween(3, 5));
        final NetworkDisruption.DisruptedLinks disruptedLinks;
        if (randomBoolean()) {
            disruptedLinks = TwoPartitions.random(random(), internalCluster().getNodeNames());
        } else {
            disruptedLinks = NetworkDisruption.Bridge.random(random(), internalCluster().getNodeNames());
        }

        NetworkDisruption networkDisruption = new NetworkDisruption(
            disruptedLinks,
            randomFrom(NetworkDisruption.UNRESPONSIVE, NetworkDisruption.DISCONNECT, NetworkDisruption.NetworkDelay.random(random()))
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        networkDisruption.startDisrupting();

        int requests = randomIntBetween(1, 200);
        CountDownLatch latch = new CountDownLatch(requests);
        for (int i = 0; i < requests - 1; ++i) {
            sendRequest(
                internalCluster().getInstance(TransportService.class),
                internalCluster().getInstance(TransportService.class),
                latch
            );
        }

        // send a request that is guaranteed disrupted.
        Tuple<TransportService, TransportService> disruptedPair = findDisruptedPair(disruptedLinks);
        sendRequest(disruptedPair.v1(), disruptedPair.v2(), latch);

        // give a bit of time to send something under disruption.
        assertFalse(
            latch.await(500, TimeUnit.MILLISECONDS) && networkDisruption.getNetworkLinkDisruptionType() != NetworkDisruption.DISCONNECT
        );
        networkDisruption.stopDisrupting();

        latch.await(30, TimeUnit.SECONDS);
        assertEquals("All requests must respond, requests: " + requests, 0, latch.getCount());
    }

    private static Tuple<TransportService, TransportService> findDisruptedPair(NetworkDisruption.DisruptedLinks disruptedLinks) {
        Optional<Tuple<TransportService, TransportService>> disruptedPair = disruptedLinks.nodes()
            .stream()
            .flatMap(n1 -> disruptedLinks.nodes().stream().map(n2 -> Tuple.tuple(n1, n2)))
            .filter(pair -> disruptedLinks.disrupt(pair.v1(), pair.v2()))
            .map(
                pair -> Tuple.tuple(
                    internalCluster().getInstance(TransportService.class, pair.v1()),
                    internalCluster().getInstance(TransportService.class, pair.v2())
                )
            )
            .findFirst();
        // since we have 3+ nodes, we are sure to find a disrupted pair, also for bridge disruptions.
        assertTrue(disruptedPair.isPresent());
        return disruptedPair.get();
    }

    private static void sendRequest(TransportService source, TransportService target, CountDownLatch latch) {
        source.sendRequest(target.getLocalNode(), ClusterHealthAction.NAME, new ClusterHealthRequest(), new TransportResponseHandler<>() {
            private AtomicBoolean responded = new AtomicBoolean();

            @Override
            public void handleResponse(TransportResponse response) {
                assertTrue(responded.compareAndSet(false, true));
                latch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                assertTrue(responded.compareAndSet(false, true));
                latch.countDown();
            }

            @Override
            public TransportResponse read(StreamInput in) throws IOException {
                return ClusterHealthResponse.readResponseFrom(in);
            }
        });
    }
}
