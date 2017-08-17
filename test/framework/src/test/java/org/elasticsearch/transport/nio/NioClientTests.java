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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.nio.channel.ChannelFactory;
import org.elasticsearch.transport.nio.channel.CloseFuture;
import org.elasticsearch.transport.nio.channel.ConnectFuture;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NioClientTests extends ESTestCase {

    private NioClient client;
    private SocketSelector selector;
    private ChannelFactory channelFactory;
    private OpenChannels openChannels = new OpenChannels(logger);
    private NioSocketChannel[] channels;
    private DiscoveryNode node;
    private Consumer<NioChannel> listener;
    private TransportAddress address;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpClient() {
        channelFactory = mock(ChannelFactory.class);
        selector = mock(SocketSelector.class);
        listener = mock(Consumer.class);

        ArrayList<SocketSelector> selectors = new ArrayList<>();
        selectors.add(selector);
        Supplier<SocketSelector> selectorSupplier = new RoundRobinSelectorSupplier(selectors);
        client = new NioClient(logger, openChannels, selectorSupplier, TimeValue.timeValueMillis(5), channelFactory);

        channels = new NioSocketChannel[2];
        address = new TransportAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        node = new DiscoveryNode("node-id", address, Version.CURRENT);
    }

    public void testCreateConnections() throws IOException, InterruptedException {
        NioSocketChannel channel1 = mock(NioSocketChannel.class);
        ConnectFuture connectFuture1 = mock(ConnectFuture.class);
        NioSocketChannel channel2 = mock(NioSocketChannel.class);
        ConnectFuture connectFuture2 = mock(ConnectFuture.class);

        when(channelFactory.openNioChannel(address.address(), selector, listener)).thenReturn(channel1, channel2);
        when(channel1.getConnectFuture()).thenReturn(connectFuture1);
        when(channel2.getConnectFuture()).thenReturn(connectFuture2);
        when(connectFuture1.awaitConnectionComplete(5, TimeUnit.MILLISECONDS)).thenReturn(true);
        when(connectFuture2.awaitConnectionComplete(5, TimeUnit.MILLISECONDS)).thenReturn(true);

        client.connectToChannels(node, channels,  TimeValue.timeValueMillis(5), listener);

        assertEquals(channel1, channels[0]);
        assertEquals(channel2, channels[1]);
    }

    public void testWithADifferentConnectTimeout() throws IOException, InterruptedException {
        NioSocketChannel channel1 = mock(NioSocketChannel.class);
        ConnectFuture connectFuture1 = mock(ConnectFuture.class);

        when(channelFactory.openNioChannel(address.address(), selector, listener)).thenReturn(channel1);
        when(channel1.getConnectFuture()).thenReturn(connectFuture1);
        when(connectFuture1.awaitConnectionComplete(3, TimeUnit.MILLISECONDS)).thenReturn(true);

        channels = new NioSocketChannel[1];
        client.connectToChannels(node, channels,  TimeValue.timeValueMillis(3), listener);

        assertEquals(channel1, channels[0]);
    }

    public void testConnectionTimeout() throws IOException, InterruptedException {
        NioSocketChannel channel1 = mock(NioSocketChannel.class);
        ConnectFuture connectFuture1 = mock(ConnectFuture.class);
        CloseFuture closeFuture1 = mock(CloseFuture.class);
        NioSocketChannel channel2 = mock(NioSocketChannel.class);
        ConnectFuture connectFuture2 = mock(ConnectFuture.class);
        CloseFuture closeFuture2 = mock(CloseFuture.class);

        when(channelFactory.openNioChannel(address.address(), selector, listener)).thenReturn(channel1, channel2);
        when(channel1.getCloseFuture()).thenReturn(closeFuture1);
        when(channel1.getConnectFuture()).thenReturn(connectFuture1);
        when(channel2.getCloseFuture()).thenReturn(closeFuture2);
        when(channel2.getConnectFuture()).thenReturn(connectFuture2);
        when(connectFuture1.awaitConnectionComplete(5, TimeUnit.MILLISECONDS)).thenReturn(true);
        when(connectFuture2.awaitConnectionComplete(5, TimeUnit.MILLISECONDS)).thenReturn(false);

        try {
            client.connectToChannels(node, channels,  TimeValue.timeValueMillis(5), listener);
            fail("Should have thrown ConnectTransportException");
        } catch (ConnectTransportException e) {
            assertTrue(e.getMessage().contains("connect_timeout[5ms]"));
        }

        verify(channel1).closeAsync();
        verify(channel2).closeAsync();

        assertNull(channels[0]);
        assertNull(channels[1]);
    }

    public void testConnectionException() throws IOException, InterruptedException {
        NioSocketChannel channel1 = mock(NioSocketChannel.class);
        ConnectFuture connectFuture1 = mock(ConnectFuture.class);
        NioSocketChannel channel2 = mock(NioSocketChannel.class);
        ConnectFuture connectFuture2 = mock(ConnectFuture.class);
        IOException ioException = new IOException();

        when(channelFactory.openNioChannel(address.address(), selector, listener)).thenReturn(channel1, channel2);
        when(channel1.getConnectFuture()).thenReturn(connectFuture1);
        when(channel2.getConnectFuture()).thenReturn(connectFuture2);
        when(connectFuture1.awaitConnectionComplete(5, TimeUnit.MILLISECONDS)).thenReturn(true);
        when(connectFuture2.awaitConnectionComplete(5, TimeUnit.MILLISECONDS)).thenReturn(false);
        when(connectFuture2.getException()).thenReturn(ioException);

        try {
            client.connectToChannels(node, channels,  TimeValue.timeValueMillis(5), listener);
            fail("Should have thrown ConnectTransportException");
        } catch (ConnectTransportException e) {
            assertTrue(e.getMessage().contains("connect_exception"));
            assertSame(ioException, e.getCause());
        }

        verify(channel1).closeAsync();
        verify(channel2).closeAsync();

        assertNull(channels[0]);
        assertNull(channels[1]);
    }

    public void testCloseDoesNotAllowConnections() throws IOException {
        client.close();

        assertFalse(client.connectToChannels(node, channels,  TimeValue.timeValueMillis(5), listener));

        for (NioSocketChannel channel : channels) {
            assertNull(channel);
        }
    }
}
