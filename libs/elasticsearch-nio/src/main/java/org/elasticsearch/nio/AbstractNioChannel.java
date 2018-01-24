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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * This is a basic channel abstraction used by the {@link ESSelector}.
 * <p>
 * A channel is open once it is constructed. The channel remains open and {@link #isOpen()} will return
 * true until the channel is explicitly closed.
 * <p>
 * A channel lifecycle has two stages:
 * <ol>
 * <li>OPEN - When a channel has been created. This is the state of a channel that can perform normal operations.
 * <li>CLOSED - The channel has been set to closed. All this means is that the channel has been scheduled to be
 * closed. The underlying raw channel may not yet be closed. The underlying channel has been closed if the close
 * future has been completed.
 * </ol>
 *
 * @param <S> the type of raw channel this AbstractNioChannel uses
 */
public abstract class AbstractNioChannel<S extends SelectableChannel & NetworkChannel> implements NioChannel {

    final S socketChannel;

    private final InetSocketAddress localAddress;

    AbstractNioChannel(S socketChannel) throws IOException {
        this.socketChannel = socketChannel;
        this.localAddress = (InetSocketAddress) socketChannel.getLocalAddress();
    }

    @Override
    public boolean isOpen() {
        return getContext().isOpen();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public S getRawChannel() {
        return socketChannel;
    }

    @Override
    public void addCloseListener(BiConsumer<Void, Throwable> listener) {
        getContext().addCloseListener(listener);
    }

    @Override
    public void close() {
        getContext().closeChannel();
    }
}
