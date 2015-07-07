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
package org.elasticsearch.discovery.zen;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.membership.MembershipAction;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class NodeJoinControllerTests extends ElasticsearchTestCase {


    public void testNormalJoin() {
        final TestClusterService clusterService = new TestClusterService();
        NodeJoinController nodeJoinController = new NodeJoinController(clusterService, null, new DiscoverySettings(Settings.EMPTY, new NodeSettingsService(Settings.EMPTY)), logger);

    }

    public void testNewClusterStateOnExistingNodeJoin() throws InterruptedException {
        final TestClusterService clusterService = new TestClusterService();
        NodeJoinController nodeJoinController = new NodeJoinController(clusterService, null, new DiscoverySettings(Settings.EMPTY, new NodeSettingsService(Settings.EMPTY)), logger);
        ClusterState state = clusterService.state();
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(state.nodes());
        nodesBuilder.masterNodeId(state.nodes().localNodeId());
        final DiscoveryNode other_node = new DiscoveryNode("other_node", DummyTransportAddress.INSTANCE, Version.CURRENT);
        nodesBuilder.put(other_node);
        clusterService.setState(ClusterState.builder(state).nodes(nodesBuilder));

        state = clusterService.state();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        nodeJoinController.handleJoinRequest(other_node, new MembershipAction.JoinCallback() {
            @Override
            public void onSuccess() {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                exception.set(t);
                logger.error("unexpected exception during join", t);
                latch.countDown();
            }
        });

        latch.await();
        if (exception.get() != null) {
            fail("unexpected exception during join: " + exception.get().getMessage());
        }

        assertTrue("failed to publish a new state upon existing join", clusterService.state() != state);
    }

}
