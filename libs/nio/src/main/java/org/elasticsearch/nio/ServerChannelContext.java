/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import org.elasticsearch.core.CompletableContext;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ServerChannelContext extends ChannelContext<ServerSocketChannel> {

    private final NioServerSocketChannel channel;
    private final NioSelector selector;
    private final Config.ServerSocket config;
    private final Consumer<NioSocketChannel> acceptor;
    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final ChannelFactory<?, ?> channelFactory;
    private final CompletableContext<Void> bindContext = new CompletableContext<>();

    public ServerChannelContext(NioServerSocketChannel channel, ChannelFactory<?, ?> channelFactory, NioSelector selector,
                                Config.ServerSocket config, Consumer<NioSocketChannel> acceptor,
                                Consumer<Exception> exceptionHandler) {
        super(channel.getRawChannel(), exceptionHandler);
        this.channel = channel;
        this.channelFactory = channelFactory;
        this.selector = selector;
        this.config = config;
        this.acceptor = acceptor;
    }

    public void acceptChannels(Supplier<NioSelector> selectorSupplier) throws IOException {
        SocketChannel acceptedChannel;
        while ((acceptedChannel = accept(rawChannel)) != null) {
            NioSocketChannel nioChannel = channelFactory.acceptNioChannel(acceptedChannel, selectorSupplier);
            acceptor.accept(nioChannel);
        }
    }

    public void addBindListener(BiConsumer<Void, Exception> listener) {
        bindContext.addListener(listener);
    }

    @Override
    protected void register() throws IOException {
        super.register();

        configureSocket(rawChannel.socket());

        InetSocketAddress localAddress = config.getLocalAddress();
        try {
            rawChannel.bind(localAddress);
            bindContext.complete(null);
        } catch (IOException e) {
            BindException exception = new BindException("Failed to bind server socket channel {localAddress=" + localAddress + "}.");
            exception.initCause(e);
            bindContext.completeExceptionally(exception);
            throw exception;
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
        socket.setReuseAddress(config.tcpReuseAddress());
    }

    protected static SocketChannel accept(ServerSocketChannel serverSocketChannel) throws IOException {
        try {
            assert serverSocketChannel.isBlocking() == false;
            SocketChannel channel = AccessController.doPrivileged((PrivilegedExceptionAction<SocketChannel>) serverSocketChannel::accept);
            assert serverSocketChannel.isBlocking() == false;
            return channel;
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }
}
