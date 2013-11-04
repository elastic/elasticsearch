/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.discovery.zen.ping.unicast;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty.NettyTransport;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class UnicastZenPingTests extends ElasticsearchTestCase {

    @Test
    public void testSimplePings() {
        Settings settings = ImmutableSettings.EMPTY;
        int startPort = 11000 + randomIntBetween(0, 1000);
        int endPort = startPort + 10;
        settings = ImmutableSettings.builder().put(settings).put("transport.tcp.port", startPort + "-" + endPort).build();

        ThreadPool threadPool = new ThreadPool();
        ClusterName clusterName = new ClusterName("test");
        NetworkService networkService = new NetworkService(settings);

        NettyTransport transportA = new NettyTransport(settings, threadPool, networkService, Version.CURRENT);
        final TransportService transportServiceA = new TransportService(transportA, threadPool).start();
        final DiscoveryNode nodeA = new DiscoveryNode("UZP_A", transportServiceA.boundAddress().publishAddress(), Version.CURRENT);

        InetSocketTransportAddress addressA = (InetSocketTransportAddress) transportA.boundAddress().publishAddress();

        NettyTransport transportB = new NettyTransport(settings, threadPool, networkService, Version.CURRENT);
        final TransportService transportServiceB = new TransportService(transportB, threadPool).start();
        final DiscoveryNode nodeB = new DiscoveryNode("UZP_B", transportServiceA.boundAddress().publishAddress(), Version.CURRENT);

        InetSocketTransportAddress addressB = (InetSocketTransportAddress) transportB.boundAddress().publishAddress();

        Settings hostsSettings = ImmutableSettings.settingsBuilder().putArray("discovery.zen.ping.unicast.hosts",
                addressA.address().getAddress().getHostAddress() + ":" + addressA.address().getPort(),
                addressB.address().getAddress().getHostAddress() + ":" + addressB.address().getPort())
                .build();

        UnicastZenPing zenPingA = new UnicastZenPing(hostsSettings, threadPool, transportServiceA, clusterName, Version.CURRENT, null);
        zenPingA.setNodesProvider(new DiscoveryNodesProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().put(nodeA).localNodeId("UZP_A").build();
            }

            @Override
            public NodeService nodeService() {
                return null;
            }
        });
        zenPingA.start();

        UnicastZenPing zenPingB = new UnicastZenPing(hostsSettings, threadPool, transportServiceB, clusterName, Version.CURRENT, null);
        zenPingB.setNodesProvider(new DiscoveryNodesProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().put(nodeB).localNodeId("UZP_B").build();
            }

            @Override
            public NodeService nodeService() {
                return null;
            }
        });
        zenPingB.start();

        try {
            ZenPing.PingResponse[] pingResponses = zenPingA.pingAndWait(TimeValue.timeValueSeconds(1));
            assertThat(pingResponses.length, equalTo(1));
            assertThat(pingResponses[0].target().id(), equalTo("UZP_B"));
        } finally {
            zenPingA.close();
            zenPingB.close();
            transportServiceA.close();
            transportServiceB.close();
            threadPool.shutdown();
        }
    }
}
