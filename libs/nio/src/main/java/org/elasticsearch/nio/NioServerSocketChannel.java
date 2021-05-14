/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class NioServerSocketChannel extends NioChannel {

    private final ServerSocketChannel serverSocketChannel;
    private final AtomicBoolean contextSet = new AtomicBoolean(false);
    private volatile InetSocketAddress localAddress;
    private ServerChannelContext context;

    public NioServerSocketChannel(ServerSocketChannel serverSocketChannel) {
        this.serverSocketChannel = serverSocketChannel;
    }

    /**
     * This method sets the context for a server socket channel. The context is called when a new channel is
     * accepted, an exception occurs, or it is time to close the channel.
     *
     * @param context to call
     */
    public void setContext(ServerChannelContext context) {
        if (contextSet.compareAndSet(false, true)) {
            this.context = context;
        } else {
            throw new IllegalStateException("Context on this channel were already set. It should only be once.");
        }
    }

    public void addBindListener(BiConsumer<Void, Exception> listener) {
        context.addBindListener(listener);
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        attemptToSetLocalAddress();
        return localAddress;
    }

    @Override
    public ServerSocketChannel getRawChannel() {
        return serverSocketChannel;
    }

    @Override
    public ServerChannelContext getContext() {
        return context;
    }

    @Override
    public String toString() {
        return "NioServerSocketChannel{" +
            "localAddress=" + getLocalAddress() +
            '}';
    }

    private void attemptToSetLocalAddress() {
        if (localAddress == null) {
            localAddress = (InetSocketAddress) serverSocketChannel.socket().getLocalSocketAddress();
        }
    }
}
