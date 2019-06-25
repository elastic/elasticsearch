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

package org.elasticsearch.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ServerChannelContext extends ChannelContext<ServerSocketChannel> {

    private final NioServerSocketChannel channel;
    private final NioSelector selector;
    private final Consumer<NioSocketChannel> acceptor;
    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final ChannelFactory<?, ?> channelFactory;

    public ServerChannelContext(NioServerSocketChannel channel, ChannelFactory<?, ?> channelFactory, NioSelector selector,
                                Consumer<NioSocketChannel> acceptor, Consumer<Exception> exceptionHandler) {
        this(channel, channelFactory, selector, null, acceptor, exceptionHandler);
    }

    public ServerChannelContext(NioServerSocketChannel channel, ChannelFactory<?, ?> channelFactory, NioSelector selector,
                                SocketConfig.ServerSocket config, Consumer<NioSocketChannel> acceptor,
                                Consumer<Exception> exceptionHandler) {
        super(channel.getRawChannel(), config, exceptionHandler);
        this.channel = channel;
        this.channelFactory = channelFactory;
        this.selector = selector;
        this.acceptor = acceptor;
    }

    public void acceptChannels(Supplier<NioSelector> selectorSupplier) throws IOException {
        NioSocketChannel acceptedChannel;
        while ((acceptedChannel = channelFactory.acceptNioChannel(this, selectorSupplier)) != null) {
            acceptor.accept(acceptedChannel);
        }
    }

    @Override
    protected void register() throws IOException {
        super.register();

        configureSocket(rawChannel.socket());

        InetSocketAddress localAddress = ((SocketConfig.ServerSocket) socketConfig).getLocalAddress();
        try {
            rawChannel.bind(localAddress);
        } catch (IOException e) {
            throw new IOException("Failed to bind server socket channel {localAddress=" + localAddress + "}.", e);
        }
    }

    @Override
    public void closeChannel() {
        if (isClosing.compareAndSet(false, true)) {
            getSelector().queueChannelClose(channel);
        }
    }

    @Override
    public NioSelector getSelector() {
        return selector;
    }

    @Override
    public NioServerSocketChannel getChannel() {
        return channel;
    }

    private void configureSocket(ServerSocket socket) throws IOException {
        socket.setReuseAddress(socketConfig.tcpReuseAddress());
    }

}
