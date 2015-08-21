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

package org.elasticsearch.discovery.zen.ping.unicast;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.ping.PingContextProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty.NettyTransport;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.hamcrest.Matchers.equalTo;

public class UnicastZenPingIT extends ESTestCase {

    @Test
    public void testSimplePings() throws InterruptedException {
        Settings settings = Settings.EMPTY;
        int startPort = 11000 + randomIntBetween(0, 1000);
        int endPort = startPort + 10;
        settings = Settings.builder().put(settings).put("transport.tcp.port", startPort + "-" + endPort).build();

        ThreadPool threadPool = new ThreadPool(getClass().getName());
        ClusterName clusterName = new ClusterName("test");
        NetworkService networkService = new NetworkService(settings);
        ElectMasterService electMasterService = new ElectMasterService(settings, Version.CURRENT);

        NettyTransport transportA = new NettyTransport(settings, threadPool, networkService, BigArrays.NON_RECYCLING_INSTANCE, Version.CURRENT, new NamedWriteableRegistry());
        final TransportService transportServiceA = new TransportService(transportA, threadPool).start();
        final DiscoveryNode nodeA = new DiscoveryNode("UZP_A", transportServiceA.boundAddress().publishAddress(), Version.CURRENT);

        InetSocketTransportAddress addressA = (InetSocketTransportAddress) transportA.boundAddress().publishAddress();

        NettyTransport transportB = new NettyTransport(settings, threadPool, networkService, BigArrays.NON_RECYCLING_INSTANCE, Version.CURRENT, new NamedWriteableRegistry());
        final TransportService transportServiceB = new TransportService(transportB, threadPool).start();
        final DiscoveryNode nodeB = new DiscoveryNode("UZP_B", transportServiceA.boundAddress().publishAddress(), Version.CURRENT);

        InetSocketTransportAddress addressB = (InetSocketTransportAddress) transportB.boundAddress().publishAddress();

        Settings hostsSettings = Settings.settingsBuilder().putArray("discovery.zen.ping.unicast.hosts",
                NetworkAddress.formatAddress(new InetSocketAddress(addressA.address().getAddress(), addressA.address().getPort())),
                NetworkAddress.formatAddress(new InetSocketAddress(addressB.address().getAddress(), addressB.address().getPort())))
                .build();

        UnicastZenPing zenPingA = new UnicastZenPing(hostsSettings, threadPool, transportServiceA, clusterName, Version.CURRENT, electMasterService, null);
        zenPingA.setPingContextProvider(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().put(nodeA).localNodeId("UZP_A").build();
            }

            @Override
            public NodeService nodeService() {
                return null;
            }

            @Override
            public boolean nodeHasJoinedClusterOnce() {
                return false;
            }
        });
        zenPingA.start();

        UnicastZenPing zenPingB = new UnicastZenPing(hostsSettings, threadPool, transportServiceB, clusterName, Version.CURRENT, electMasterService, null);
        zenPingB.setPingContextProvider(new PingContextProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().put(nodeB).localNodeId("UZP_B").build();
            }

            @Override
            public NodeService nodeService() {
                return null;
            }

            @Override
            public boolean nodeHasJoinedClusterOnce() {
                return true;
            }
        });
        zenPingB.start();

        try {
            logger.info("ping from UZP_A");
            ZenPing.PingResponse[] pingResponses = zenPingA.pingAndWait(TimeValue.timeValueSeconds(10));
            assertThat(pingResponses.length, equalTo(1));
            assertThat(pingResponses[0].node().id(), equalTo("UZP_B"));
            assertTrue(pingResponses[0].hasJoinedOnce());

            // ping again, this time from B,
            logger.info("ping from UZP_B");
            pingResponses = zenPingB.pingAndWait(TimeValue.timeValueSeconds(10));
            assertThat(pingResponses.length, equalTo(1));
            assertThat(pingResponses[0].node().id(), equalTo("UZP_A"));
            assertFalse(pingResponses[0].hasJoinedOnce());

        } finally {
            zenPingA.close();
            zenPingB.close();
            transportServiceA.close();
            transportServiceB.close();
            terminate(threadPool);
        }
    }
}
