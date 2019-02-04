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

import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.JoinHelper;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.FaultDetection;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoMinMasterNodes = false)
public class NetworkDisruptionIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    // TODO this settings are the same as in AbstractDisruptionTestCase, can we avoid copy-paste?
    private static final Settings DISRUPTION_TUNED_SETTINGS = Settings.builder()
            .put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), "1s") // for hitting simulated network failures quickly
            .put(FaultDetection.PING_RETRIES_SETTING.getKey(), "1") // for hitting simulated network failures quickly
            .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s") // for hitting simulated network failures quickly
            .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 1) // for hitting simulated network failures quickly
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "1s") // for hitting simulated network failures quickly
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1) // for hitting simulated network failures quickly
            .put("discovery.zen.join_timeout", "10s")  // still long to induce failures but to long so test won't time out
            .put(JoinHelper.JOIN_TIMEOUT_SETTING.getKey(), "10s") // still long to induce failures but to long so test won't time out
            .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "1s") // <-- for hitting simulated network failures quickly
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s") // <-- for hitting simulated network failures quickly
            .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "10s") // Network delay disruption waits for the min between this
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "2s") // ensure quick node reconnects
            // value and the time of disruption and does not recover immediately
            // when disruption is stop. We should make sure we recover faster
            // then the default of 30s, causing ensureGreen and friends to time out
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
        NetworkDisruption networkDisruption = new NetworkDisruption(new TwoPartitions(side1, side2),
                new NetworkDisruption.NetworkUnresponsive());
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        return Tuple.tuple(side1, side2);
    }

    public void testClearDisruptionSchemeWhenNodeIsDown() throws IOException {
        Tuple<Set<String>, Set<String>> sides = prepareDisruptedCluster();

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(randomFrom(sides.v2())));
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

}
