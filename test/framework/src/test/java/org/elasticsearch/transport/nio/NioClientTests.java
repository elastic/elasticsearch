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

package org.elasticsearch.transport.nio;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.channel.ChannelFactory;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.function.Supplier;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NioClientTests extends ESTestCase {

    private NioClient client;
    private SocketSelector selector;
    private ChannelFactory channelFactory;
    private OpenChannels openChannels = new OpenChannels(logger);
    private InetSocketAddress address;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpClient() {
        channelFactory = mock(ChannelFactory.class);
        selector = mock(SocketSelector.class);
        ArrayList<SocketSelector> selectors = new ArrayList<>();
        selectors.add(selector);
        Supplier<SocketSelector> selectorSupplier = new RoundRobinSelectorSupplier(selectors);
        client = new NioClient(openChannels, selectorSupplier, channelFactory);
        address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    public void testCreateConnection() throws IOException, InterruptedException {
        NioSocketChannel channel1 = mock(NioSocketChannel.class);

        when(channelFactory.openNioChannel(eq(address), eq(selector))).thenReturn(channel1);

        NioSocketChannel nioSocketChannel = client.initiateConnection(address);

        assertEquals(channel1, nioSocketChannel);
    }

    public void testConnectionException() throws IOException, InterruptedException {
        IOException ioException = new IOException();

        when(channelFactory.openNioChannel(eq(address), eq(selector))).thenThrow(ioException);

        expectThrows(IOException.class, () -> client.initiateConnection(address));
    }

    public void testCloseDoesNotAllowConnections() throws IOException {
        client.close();

        assertNull(client.initiateConnection(address));
    }
}
