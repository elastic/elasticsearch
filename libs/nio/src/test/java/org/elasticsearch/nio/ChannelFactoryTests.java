/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChannelFactoryTests extends ESTestCase {

    private ChannelFactory<NioServerSocketChannel, NioSocketChannel> channelFactory;
    private ChannelFactory.RawChannelFactory rawChannelFactory;
    private SocketChannel rawChannel;
    private ServerSocketChannel rawServerChannel;
    private NioSelector socketSelector;
    private Supplier<NioSelector> socketSelectorSupplier;
    private Supplier<NioSelector> acceptingSelectorSupplier;
    private NioSelector acceptingSelector;

    @Before
    @SuppressWarnings("unchecked")
    public void setupFactory() throws IOException {
        rawChannelFactory = mock(ChannelFactory.RawChannelFactory.class);
        channelFactory = new TestChannelFactory(rawChannelFactory);
        socketSelector = mock(NioSelector.class);
        acceptingSelector = mock(NioSelector.class);
        socketSelectorSupplier = mock(Supplier.class);
        acceptingSelectorSupplier = mock(Supplier.class);
        rawChannel = SocketChannel.open();
        rawServerChannel = ServerSocketChannel.open();
        when(socketSelectorSupplier.get()).thenReturn(socketSelector);
        when(acceptingSelectorSupplier.get()).thenReturn(acceptingSelector);
    }

    @After
    public void ensureClosed() throws IOException {
        IOUtils.closeWhileHandlingException(rawChannel, rawServerChannel);
    }

    public void testAcceptChannel() throws IOException {
        NioSocketChannel channel = channelFactory.acceptNioChannel(rawChannel, socketSelectorSupplier);

        verify(socketSelector).scheduleForRegistration(channel);

        assertEquals(rawChannel, channel.getRawChannel());
    }

    public void testAcceptedChannelRejected() throws IOException {
        doThrow(new IllegalStateException()).when(socketSelector).scheduleForRegistration(any());

        expectThrows(IllegalStateException.class, () -> channelFactory.acceptNioChannel(rawChannel, socketSelectorSupplier));

        assertFalse(rawChannel.isOpen());
    }

    public void testOpenChannel() throws IOException {
        InetSocketAddress address = mock(InetSocketAddress.class);
        when(rawChannelFactory.openNioChannel()).thenReturn(rawChannel);

        NioSocketChannel channel = channelFactory.openNioChannel(address, socketSelectorSupplier);

        verify(socketSelector).scheduleForRegistration(channel);

        assertEquals(rawChannel, channel.getRawChannel());
    }

    public void testOpenedChannelRejected() throws IOException {
        InetSocketAddress address = mock(InetSocketAddress.class);
        when(rawChannelFactory.openNioChannel()).thenReturn(rawChannel);
        doThrow(new IllegalStateException()).when(socketSelector).scheduleForRegistration(any());

        expectThrows(IllegalStateException.class, () -> channelFactory.openNioChannel(address, socketSelectorSupplier));

        assertFalse(rawChannel.isOpen());
    }

    public void testOpenServerChannel() throws IOException {
        InetSocketAddress address = mock(InetSocketAddress.class);
        when(rawChannelFactory.openNioServerSocketChannel()).thenReturn(rawServerChannel);

        NioServerSocketChannel channel = channelFactory.openNioServerSocketChannel(address, acceptingSelectorSupplier);

        verify(acceptingSelector).scheduleForRegistration(channel);

        assertEquals(rawServerChannel, channel.getRawChannel());
    }

    public void testOpenedServerChannelRejected() throws IOException {
        InetSocketAddress address = mock(InetSocketAddress.class);
        when(rawChannelFactory.openNioServerSocketChannel()).thenReturn(rawServerChannel);
        doThrow(new IllegalStateException()).when(acceptingSelector).scheduleForRegistration(any());

        expectThrows(IllegalStateException.class, () -> channelFactory.openNioServerSocketChannel(address, acceptingSelectorSupplier));

        assertFalse(rawServerChannel.isOpen());
    }

    private static class TestChannelFactory extends ChannelFactory<NioServerSocketChannel, NioSocketChannel> {

        TestChannelFactory(RawChannelFactory rawChannelFactory) {
            super(randomBoolean(), randomBoolean(), -1, -1, -1, randomBoolean(), -1, -1, rawChannelFactory);
        }

        @Override
        public NioSocketChannel createChannel(NioSelector selector, SocketChannel channel, Config.Socket socketConfig) {
            NioSocketChannel nioSocketChannel = new NioSocketChannel(channel);
            nioSocketChannel.setContext(mock(SocketChannelContext.class));
            return nioSocketChannel;
        }

        @Override
        public NioServerSocketChannel createServerChannel(
            NioSelector selector,
            ServerSocketChannel channel,
            Config.ServerSocket socketConfig
        ) {
            return new NioServerSocketChannel(channel);
        }

        @Override
        protected InetSocketAddress getRemoteAddress(SocketChannel rawChannel) throws IOException {
            // Override this functionality to avoid having to connect the accepted channel
            return mock(InetSocketAddress.class);
        }
    }
}
