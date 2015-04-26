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
package org.elasticsearch.transport.netty;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 */
public class NettyScheduledPingTests extends ElasticsearchTestCase {

    @Test
    public void testScheduledPing() throws Exception {
        ThreadPool threadPool = new ThreadPool(getClass().getName());

        int startPort = 11000 + randomIntBetween(0, 255);
        int endPort = startPort + 10;
        Settings settings = ImmutableSettings.builder().put(NettyTransport.PING_SCHEDULE, "5ms").put("transport.tcp.port", startPort + "-" + endPort).build();

        final NettyTransport nettyA = new NettyTransport(settings, threadPool, new NetworkService(settings), BigArrays.NON_RECYCLING_INSTANCE, Version.CURRENT);
        MockTransportService serviceA = new MockTransportService(settings, nettyA, threadPool);
        serviceA.start();

        final NettyTransport nettyB = new NettyTransport(settings, threadPool, new NetworkService(settings), BigArrays.NON_RECYCLING_INSTANCE, Version.CURRENT);
        MockTransportService serviceB = new MockTransportService(settings, nettyB, threadPool);
        serviceB.start();

        DiscoveryNode nodeA = new DiscoveryNode("TS_A", "TS_A", serviceA.boundAddress().publishAddress(), ImmutableMap.<String, String>of(), Version.CURRENT);
        DiscoveryNode nodeB = new DiscoveryNode("TS_B", "TS_B", serviceB.boundAddress().publishAddress(), ImmutableMap.<String, String>of(), Version.CURRENT);

        serviceA.connectToNode(nodeB);
        serviceB.connectToNode(nodeA);

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(nettyA.scheduledPing.successfulPings.count(), greaterThan(100l));
                assertThat(nettyB.scheduledPing.successfulPings.count(), greaterThan(100l));
            }
        });
        assertThat(nettyA.scheduledPing.failedPings.count(), equalTo(0l));
        assertThat(nettyB.scheduledPing.failedPings.count(), equalTo(0l));

        serviceA.registerRequestHandler("sayHello", TransportRequest.Empty.class, ThreadPool.Names.GENERIC, new TransportRequestHandler<TransportRequest.Empty>() {
            @Override
            public void messageReceived(TransportRequest.Empty request, TransportChannel channel) {
                try {
                    channel.sendResponse(TransportResponse.Empty.INSTANCE, TransportResponseOptions.options());
                } catch (IOException e) {
                    e.printStackTrace();
                    assertThat(e.getMessage(), false, equalTo(true));
                }
            }
        });

        // send some messages while ping requests are going around
        int rounds = scaledRandomIntBetween(100, 5000);
        for (int i = 0; i < rounds; i++) {
            serviceB.submitRequest(nodeA, "sayHello",
                    TransportRequest.Empty.INSTANCE, TransportRequestOptions.options().withCompress(randomBoolean()), new BaseTransportResponseHandler<TransportResponse.Empty>() {
                        @Override
                        public TransportResponse.Empty newInstance() {
                            return TransportResponse.Empty.INSTANCE;
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.GENERIC;
                        }

                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            exp.printStackTrace();
                            assertThat("got exception instead of a response: " + exp.getMessage(), false, equalTo(true));
                        }
                    }).txGet();
        }

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(nettyA.scheduledPing.successfulPings.count(), greaterThan(200l));
                assertThat(nettyB.scheduledPing.successfulPings.count(), greaterThan(200l));
            }
        });
        assertThat(nettyA.scheduledPing.failedPings.count(), equalTo(0l));
        assertThat(nettyB.scheduledPing.failedPings.count(), equalTo(0l));

        Releasables.close(serviceA, serviceB);
        terminate(threadPool);
    }
}
