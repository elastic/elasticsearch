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

package org.elasticsearch.transport.nio.channel;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.AcceptingSelector;
import org.elasticsearch.transport.nio.SocketSelector;
import org.elasticsearch.transport.nio.TcpReadHandler;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChannelFactoryTests extends ESTestCase {

    private ChannelFactory channelFactory;
    private ChannelFactory.RawChannelFactory rawChannelFactory;
    private Consumer<NioChannel> listener;
    private SocketChannel rawChannel;
    private ServerSocketChannel rawServerChannel;
    private SocketSelector socketSelector;
    private AcceptingSelector acceptingSelector;

    @Before
    @SuppressWarnings("unchecked")
    public void setupFactory() throws IOException {
        rawChannelFactory = mock(ChannelFactory.RawChannelFactory.class);
        channelFactory = new ChannelFactory(rawChannelFactory, mock(TcpReadHandler.class));
        listener = mock(Consumer.class);
        socketSelector = mock(SocketSelector.class);
        acceptingSelector = mock(AcceptingSelector.class);
        rawChannel = SocketChannel.open();
        rawServerChannel = ServerSocketChannel.open();
    }

    @After
    public void ensureClosed() throws IOException {
        IOUtils.closeWhileHandlingException(rawChannel);
        IOUtils.closeWhileHandlingException(rawServerChannel);
    }

    public void testAcceptChannel() throws IOException {
        NioServerSocketChannel serverChannel = mock(NioServerSocketChannel.class);
        when(rawChannelFactory.acceptNioChannel(serverChannel)).thenReturn(rawChannel);
        when(serverChannel.getProfile()).thenReturn("parent-profile");

        NioSocketChannel channel = channelFactory.acceptNioChannel(serverChannel, socketSelector, listener);

        verify(socketSelector).scheduleForRegistration(channel);

        assertEquals(socketSelector, channel.getSelector());
        assertEquals("parent-profile", channel.getProfile());
        assertEquals(rawChannel, channel.getRawChannel());

        channel.getCloseFuture().channelClosed(channel);

        verify(listener).accept(channel);
    }

    public void testAcceptedChannelRejected() throws IOException {
        NioServerSocketChannel serverChannel = mock(NioServerSocketChannel.class);
        when(rawChannelFactory.acceptNioChannel(serverChannel)).thenReturn(rawChannel);
        doThrow(new IllegalStateException()).when(socketSelector).scheduleForRegistration(any());

        expectThrows(IllegalStateException.class, () -> channelFactory.acceptNioChannel(serverChannel, socketSelector, listener));

        assertFalse(rawChannel.isOpen());
    }

    public void testOpenChannel() throws IOException {
        InetSocketAddress address = mock(InetSocketAddress.class);
        when(rawChannelFactory.openNioChannel(same(address))).thenReturn(rawChannel);

        NioSocketChannel channel = channelFactory.openNioChannel(address, socketSelector, listener);

        verify(socketSelector).scheduleForRegistration(channel);

        assertEquals(socketSelector, channel.getSelector());
        assertEquals("client-socket", channel.getProfile());
        assertEquals(rawChannel, channel.getRawChannel());

        channel.getCloseFuture().channelClosed(channel);

        verify(listener).accept(channel);
    }

    public void testOpenedChannelRejected() throws IOException {
        InetSocketAddress address = mock(InetSocketAddress.class);
        when(rawChannelFactory.openNioChannel(same(address))).thenReturn(rawChannel);
        doThrow(new IllegalStateException()).when(socketSelector).scheduleForRegistration(any());

        expectThrows(IllegalStateException.class, () -> channelFactory.openNioChannel(address, socketSelector, listener));

        assertFalse(rawChannel.isOpen());
    }

    public void testOpenServerChannel() throws IOException {
        InetSocketAddress address = mock(InetSocketAddress.class);
        when(rawChannelFactory.openNioServerSocketChannel(same(address))).thenReturn(rawServerChannel);

        String profile = "profile";
        NioServerSocketChannel channel = channelFactory.openNioServerSocketChannel(profile, address, acceptingSelector);

        verify(acceptingSelector).scheduleForRegistration(channel);

        assertEquals(acceptingSelector, channel.getSelector());
        assertEquals(profile, channel.getProfile());
        assertEquals(rawServerChannel, channel.getRawChannel());
    }

    public void testOpenedServerChannelRejected() throws IOException {
        InetSocketAddress address = mock(InetSocketAddress.class);
        when(rawChannelFactory.openNioServerSocketChannel(same(address))).thenReturn(rawServerChannel);
        doThrow(new IllegalStateException()).when(acceptingSelector).scheduleForRegistration(any());

        expectThrows(IllegalStateException.class, () -> channelFactory.openNioServerSocketChannel("", address, acceptingSelector));

        assertFalse(rawServerChannel.isOpen());
    }
}
