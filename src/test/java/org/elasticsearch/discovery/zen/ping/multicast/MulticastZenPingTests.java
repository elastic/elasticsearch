/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.local.LocalTransport;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class MulticastZenPingTests {

    @Test public void testSimplePings() {
        ThreadPool threadPool = new ThreadPool();
        ClusterName clusterName = new ClusterName("test");
        final TransportService transportServiceA = new TransportService(new LocalTransport(threadPool), threadPool).start();
        final DiscoveryNode nodeA = new DiscoveryNode("A", transportServiceA.boundAddress().publishAddress());

        final TransportService transportServiceB = new TransportService(new LocalTransport(threadPool), threadPool).start();
        final DiscoveryNode nodeB = new DiscoveryNode("B", transportServiceA.boundAddress().publishAddress());

        MulticastZenPing zenPingA = new MulticastZenPing(threadPool, transportServiceA, clusterName);
        zenPingA.setNodesProvider(new DiscoveryNodesProvider() {
            @Override public DiscoveryNodes nodes() {
                return DiscoveryNodes.newNodesBuilder().put(nodeA).localNodeId("A").build();
            }
        });
        zenPingA.start();

        MulticastZenPing zenPingB = new MulticastZenPing(threadPool, transportServiceB, clusterName);
        zenPingB.setNodesProvider(new DiscoveryNodesProvider() {
            @Override public DiscoveryNodes nodes() {
                return DiscoveryNodes.newNodesBuilder().put(nodeB).localNodeId("B").build();
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
}
