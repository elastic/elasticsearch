/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class NioSocketChannel extends NioChannel {

    private final AtomicBoolean contextSet = new AtomicBoolean(false);
    private final SocketChannel socketChannel;
    private volatile InetSocketAddress remoteAddress;
    private volatile InetSocketAddress localAddress;
    private volatile SocketChannelContext context;

    public NioSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public void setContext(SocketChannelContext context) {
        if (contextSet.compareAndSet(false, true)) {
            this.context = context;
        } else {
            throw new IllegalStateException("Context on this channel were already set. It should only be once.");
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        if (localAddress == null) {
            localAddress = (InetSocketAddress) socketChannel.socket().getLocalSocketAddress();
        }
        return localAddress;
    }

    @Override
    public SocketChannel getRawChannel() {
        return socketChannel;
    }

    @Override
    public SocketChannelContext getContext() {
        return context;
    }

    public InetSocketAddress getRemoteAddress() {
        if (remoteAddress == null) {
            remoteAddress = (InetSocketAddress) socketChannel.socket().getRemoteSocketAddress();
        }
        return remoteAddress;
    }

    public void addConnectListener(BiConsumer<Void, Exception> listener) {
        context.addConnectListener(listener);
    }

    @Override
    public String toString() {
        return "NioSocketChannel{" + "localAddress=" + getLocalAddress() + ", remoteAddress=" + getRemoteAddress() + '}';
    }
}
