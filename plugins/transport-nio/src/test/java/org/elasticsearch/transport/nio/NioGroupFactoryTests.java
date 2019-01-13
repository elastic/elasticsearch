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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.nio.ChannelFactory;
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

    private NioGroupFactory groupFactory;

    public void testSharedGroupStillWorksWhenOneInstanceClosed() throws IOException {
        groupFactory = new NioGroupFactory(Settings.EMPTY, logger);

        InetSocketAddress inetSocketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        SharedNioGroup httpGroup = groupFactory.getHttpGroup();
        try {
            SharedNioGroup transportGroup = groupFactory.getTransportGroup();
            assertTrue(transportGroup.shareSameGroup(httpGroup));
            transportGroup.close();
            expectThrows(IllegalStateException.class, () -> transportGroup.bindServerChannel(inetSocketAddress, new BindingFactory()));

            httpGroup.bindServerChannel(inetSocketAddress, new BindingFactory());
        } finally {
            httpGroup.close();
        }
        expectThrows(IllegalStateException.class, () -> httpGroup.bindServerChannel(inetSocketAddress, new BindingFactory()));
    }

    public void testWhenHttpThreadsAreNotConfiguredGroupIsTheSame() throws IOException {
        groupFactory = new NioGroupFactory(Settings.EMPTY, logger);

        try (SharedNioGroup transportGroup = groupFactory.getTransportGroup();
             SharedNioGroup httpGroup = groupFactory.getHttpGroup()) {
            assertTrue(transportGroup.shareSameGroup(httpGroup));
        }
    }

    public void testWhenHttpThreadsAreConfiguredGroupIsNotTheSame() throws IOException {
        Settings settings = Settings.builder().put(NioTransportPlugin.NIO_HTTP_WORKER_COUNT.getKey(), 1).build();
        groupFactory = new NioGroupFactory(settings, logger);

        try (SharedNioGroup transportGroup = groupFactory.getTransportGroup();
             SharedNioGroup httpGroup = groupFactory.getHttpGroup()) {
            assertFalse(transportGroup.shareSameGroup(httpGroup));
        }
    }

    private static class BindingFactory extends ChannelFactory<NioServerSocketChannel, NioSocketChannel> {

        private BindingFactory() {
            super(new ChannelFactory.RawChannelFactory(false, false, false, -1, -1));
        }

        @Override
        public NioSocketChannel createChannel(NioSelector selector, SocketChannel channel) throws IOException {
            throw new IOException("boom");
        }

        @Override
        public NioServerSocketChannel createServerChannel(NioSelector selector, ServerSocketChannel channel) throws IOException {
            NioServerSocketChannel nioChannel = new NioServerSocketChannel(channel);
            Consumer<Exception> exceptionHandler = (e) -> {};
            Consumer<NioSocketChannel> acceptor = (c) -> {};
            ServerChannelContext context = new ServerChannelContext(nioChannel, this, selector, acceptor, exceptionHandler);
            nioChannel.setContext(context);
            return nioChannel;
        }
    }
}
