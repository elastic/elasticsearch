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

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.cache.recycler.MockPageCacheRecycler;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty.NettyTransport;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.is;

/**
 * This test checks, if a HTTP look-alike request (starting with a HTTP method and a space)
 * actually returns text response instead of just dropping the connection
 */
public class NettySizeHeaderFrameDecoderTests extends ESTestCase {

    private final Settings settings = settingsBuilder()
                                        .put("name", "foo")
                                        .put("transport.host", "127.0.0.1")
                                        .put("transport.tcp.port", "0")
                                        .build();

    private ThreadPool threadPool;
    private NettyTransport nettyTransport;
    private int port;
    private InetAddress host;

    @Before
    public void startThreadPool() {
        threadPool = new ThreadPool(settings);
        threadPool.setNodeSettingsService(new NodeSettingsService(settings));
        NetworkService networkService = new NetworkService(settings);
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(settings, threadPool), new NoneCircuitBreakerService());
        nettyTransport = new NettyTransport(settings, threadPool, networkService, bigArrays, Version.CURRENT, new NamedWriteableRegistry());
        nettyTransport.start();
        TransportService transportService = new TransportService(nettyTransport, threadPool);
        nettyTransport.transportServiceAdapter(transportService.createAdapter());

        InetSocketTransportAddress transportAddress = (InetSocketTransportAddress) randomFrom(nettyTransport.boundAddress().boundAddresses());
        port = transportAddress.address().getPort();
        host = transportAddress.address().getAddress();

    }

    @After
    public void terminateThreadPool() throws InterruptedException {
        nettyTransport.stop();
        terminate(threadPool);
    }

    public void testThatTextMessageIsReturnedOnHTTPLikeRequest() throws Exception {
        String randomMethod = randomFrom("GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH");
        String data = randomMethod + " / HTTP/1.1";

        try (Socket socket = new Socket(host, port)) {
            socket.getOutputStream().write(data.getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                assertThat(reader.readLine(), is("This is not a HTTP port"));
            }
        }
    }

    public void testThatNothingIsReturnedForOtherInvalidPackets() throws Exception {
        try (Socket socket = new Socket(host, port)) {
            socket.getOutputStream().write("FOOBAR".getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();

            // end of stream
            assertThat(socket.getInputStream().read(), is(-1));
        }
    }
}
