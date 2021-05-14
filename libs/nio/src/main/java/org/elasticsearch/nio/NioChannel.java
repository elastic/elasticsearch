/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.net.InetSocketAddress;
import java.nio.channels.NetworkChannel;
import java.util.function.BiConsumer;

/**
 * This is a basic channel abstraction used by the {@link NioSelector}.
 * <p>
 * A channel is open once it is constructed. The channel remains open and {@link #isOpen()} will return
 * true until the channel is explicitly closed.
 */
public abstract class NioChannel {

    public boolean isOpen() {
        return getContext().isOpen();
    }

    /**
     * Adds a close listener to the channel. Multiple close listeners can be added. There is no guarantee
     * about the order in which close listeners will be executed. If the channel is already closed, the
     * listener is executed immediately.
     *
     * @param listener to be called at close
     */
    public void addCloseListener(BiConsumer<Void, Exception> listener) {
        getContext().addCloseListener(listener);
    }

    /**
     * Schedules channel for close. This process is asynchronous.
     */
    public void close() {
        getContext().closeChannel();
    }

    public abstract InetSocketAddress getLocalAddress();

    public abstract NetworkChannel getRawChannel();

    public abstract ChannelContext<?> getContext();
}
