/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.nio;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.Config;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioServerSocketChannel;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

public class NioGroupFactoryTests extends ESTestCase {

    public void testSharedGroupStillWorksWhenOneInstanceClosed() throws IOException {
        NioGroupFactory groupFactory = new NioGroupFactory(Settings.EMPTY, logger);

        InetSocketAddress inetSocketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        NioGroup httpGroup = groupFactory.getHttpGroup();
        try {
            NioGroup transportGroup = groupFactory.getTransportGroup();
            transportGroup.close();
            expectThrows(IllegalStateException.class, () -> transportGroup.bindServerChannel(inetSocketAddress, new BindingFactory()));

            httpGroup.bindServerChannel(inetSocketAddress, new BindingFactory());
        } finally {
            httpGroup.close();
        }
        expectThrows(IllegalStateException.class, () -> httpGroup.bindServerChannel(inetSocketAddress, new BindingFactory()));
    }

    private static class BindingFactory extends ChannelFactory<NioServerSocketChannel, NioSocketChannel> {

        private BindingFactory() {
            super(false, false, -1, -1, -1, false, -1, -1);
        }

        @Override
        public NioSocketChannel createChannel(NioSelector selector, SocketChannel channel, Config.Socket socketConfig) throws IOException {
            throw new IOException("boom");
        }

        @Override
        public NioServerSocketChannel createServerChannel(
            NioSelector selector,
            ServerSocketChannel channel,
            Config.ServerSocket socketConfig
        ) {
            NioServerSocketChannel nioChannel = new NioServerSocketChannel(channel);
            Consumer<Exception> exceptionHandler = (e) -> {};
            Consumer<NioSocketChannel> acceptor = (c) -> {};
            ServerChannelContext context = new ServerChannelContext(nioChannel, this, selector, socketConfig, acceptor, exceptionHandler);
            nioChannel.setContext(context);
            return nioChannel;
        }
    }
}
