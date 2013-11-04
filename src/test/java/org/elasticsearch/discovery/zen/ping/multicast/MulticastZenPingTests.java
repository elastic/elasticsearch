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

package org.elasticsearch.discovery.zen.ping.multicast;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.local.LocalTransport;
import org.junit.Test;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class MulticastZenPingTests extends ElasticsearchTestCase {

    private Settings buildRandomMulticast(Settings settings) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(settings);
        builder.put("discovery.zen.ping.multicast.group", "224.2.3." + randomIntBetween(0, 255));
        builder.put("discovery.zen.ping.multicast.port", randomIntBetween(55000, 56000));
        return builder.build();
    }

    @Test
    public void testSimplePings() {
        Settings settings = ImmutableSettings.EMPTY;
        settings = buildRandomMulticast(settings);

        ThreadPool threadPool = new ThreadPool();
        ClusterName clusterName = new ClusterName("test");
        final TransportService transportServiceA = new TransportService(new LocalTransport(settings, threadPool, Version.CURRENT), threadPool).start();
        final DiscoveryNode nodeA = new DiscoveryNode("A", transportServiceA.boundAddress().publishAddress(), Version.CURRENT);

        final TransportService transportServiceB = new TransportService(new LocalTransport(settings, threadPool, Version.CURRENT), threadPool).start();
        final DiscoveryNode nodeB = new DiscoveryNode("B", transportServiceA.boundAddress().publishAddress(), Version.CURRENT);

        MulticastZenPing zenPingA = new MulticastZenPing(threadPool, transportServiceA, clusterName, Version.CURRENT);
        zenPingA.setNodesProvider(new DiscoveryNodesProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().put(nodeA).localNodeId("A").build();
            }

            @Override
            public NodeService nodeService() {
                return null;
            }
        });
        zenPingA.start();

        MulticastZenPing zenPingB = new MulticastZenPing(threadPool, transportServiceB, clusterName, Version.CURRENT);
        zenPingB.setNodesProvider(new DiscoveryNodesProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().put(nodeB).localNodeId("B").build();
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
            assertThat(pingResponses[0].target().id(), equalTo("B"));
        } finally {
            zenPingA.close();
            zenPingB.close();
            transportServiceA.close();
            transportServiceB.close();
            threadPool.shutdown();
        }
    }

    @Test
    public void testExternalPing() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        settings = buildRandomMulticast(settings);

        ThreadPool threadPool = new ThreadPool();
        ClusterName clusterName = new ClusterName("test");
        final TransportService transportServiceA = new TransportService(new LocalTransport(settings, threadPool, Version.CURRENT), threadPool).start();
        final DiscoveryNode nodeA = new DiscoveryNode("A", transportServiceA.boundAddress().publishAddress(), Version.CURRENT);

        MulticastZenPing zenPingA = new MulticastZenPing(threadPool, transportServiceA, clusterName, Version.CURRENT);
        zenPingA.setNodesProvider(new DiscoveryNodesProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return DiscoveryNodes.builder().put(nodeA).localNodeId("A").build();
            }

            @Override
            public NodeService nodeService() {
                return null;
            }
        });
        zenPingA.start();

        MulticastSocket multicastSocket = null;
        try {
            Loggers.getLogger(MulticastZenPing.class).setLevel("TRACE");
            multicastSocket = new MulticastSocket(54328);
            multicastSocket.setReceiveBufferSize(2048);
            multicastSocket.setSendBufferSize(2048);
            multicastSocket.setSoTimeout(60000);

            DatagramPacket datagramPacket = new DatagramPacket(new byte[2048], 2048, InetAddress.getByName("224.2.2.4"), 54328);
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("request").field("cluster_name", "test").endObject().endObject();
            datagramPacket.setData(builder.bytes().toBytes());
            multicastSocket.send(datagramPacket);
            Thread.sleep(100);
        } finally {
            Loggers.getLogger(MulticastZenPing.class).setLevel("INFO");
            if (multicastSocket != null) multicastSocket.close();
            zenPingA.close();
            threadPool.shutdown();
        }
    }
}
