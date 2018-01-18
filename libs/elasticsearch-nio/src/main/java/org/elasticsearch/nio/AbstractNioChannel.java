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
    private final CompletableFuture<Void> closeContext = new CompletableFuture<>();
    private final ESSelector selector;
    private SelectionKey selectionKey;

    AbstractNioChannel(S socketChannel, ESSelector selector) throws IOException {
        this.socketChannel = socketChannel;
        this.localAddress = (InetSocketAddress) socketChannel.getLocalAddress();
        this.selector = selector;
    }

    @Override
    public boolean isOpen() {
        return closeContext.isDone() == false;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    /**
     * Closes the channel synchronously. This method should only be called from the selector thread.
     * <p>
     * Once this method returns, the channel will be closed.
     */
    @Override
    public void closeFromSelector() throws IOException {
        selector.assertOnSelectorThread();
        if (closeContext.isDone() == false) {
            try {
                socketChannel.close();
                closeContext.complete(null);
            } catch (IOException e) {
                closeContext.completeExceptionally(e);
                throw e;
            }
        }
    }

    /**
     * This method attempts to registered a channel with the raw nio selector. It also sets the selection
     * key.
     *
     * @throws ClosedChannelException if the raw channel was closed
     */
    @Override
    public void register() throws ClosedChannelException {
        setSelectionKey(socketChannel.register(selector.rawSelector(), 0));
    }

    @Override
    public ESSelector getSelector() {
        return selector;
    }

    @Override
    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    @Override
    public S getRawChannel() {
        return socketChannel;
    }

    @Override
    public void addCloseListener(BiConsumer<Void, Throwable> listener) {
        closeContext.whenComplete(listener);
    }

    @Override
    public void close() {
        getContext().closeChannel();
    }

    // Package visibility for testing
    void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }
}
