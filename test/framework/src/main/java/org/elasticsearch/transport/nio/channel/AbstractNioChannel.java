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

package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.transport.nio.ESSelector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a basic channel abstraction used by the {@link org.elasticsearch.transport.nio.NioTransport}.
 *
 * A channel is open once it is constructed. The channel remains open and {@link #isOpen()} will return
 * true until the channel is explicitly closed.
 *
 * A channel lifecycle has four stages:
 *
 * 1. UNREGISTERED - When a channel is created and prior to it being registered with a selector.
 * 2. REGISTERED - When a channel has been registered with a selector. This is the state of a channel that
 * can perform normal operations.
 * 3. CLOSING - When a channel has been marked for closed, but is not yet closed. {@link #isOpen()} will
 * still return true. Normal operations should be rejected. The most common scenario for a channel to be
 * CLOSING is when channel that was REGISTERED has {@link #closeAsync()} called, but the selector thread
 * has not yet closed the channel.
 * 4. CLOSED - The channel has been closed.
 *
 * @param <S> the type of raw channel this AbstractNioChannel uses
 */
public abstract class AbstractNioChannel<S extends SelectableChannel & NetworkChannel> implements NioChannel {

    static final int UNREGISTERED = 0;
    static final int REGISTERED = 1;
    static final int CLOSING = 2;
    static final int CLOSED = 3;

    protected final S socketChannel;
    protected final AtomicInteger state = new AtomicInteger(UNREGISTERED);

    private final InetSocketAddress localAddress;
    private final String profile;
    private final CloseFuture closeFuture = new CloseFuture();
    private volatile ESSelector selector;
    private SelectionKey selectionKey;

    public AbstractNioChannel(String profile, S socketChannel) throws IOException {
        this.profile = profile;
        this.socketChannel = socketChannel;
        this.localAddress = (InetSocketAddress) socketChannel.getLocalAddress();
    }

    @Override
    public boolean isOpen() {
        return closeFuture.isClosed() == false;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public String getProfile() {
        return profile;
    }

    /**
     * Registers a channel to be closed by the selector event loop with which it is registered.
     *
     * If the current state is UNREGISTERED, the call will attempt to transition the state from UNREGISTERED
     * to CLOSING. If this transition is successful, the channel can no longer be registered with an event
     * loop and the channel will be synchronously closed in this method call. If the channel
     *
     * If the channel is REGISTERED and the state can be transitioned to CLOSING, the close operation will
     * be scheduled with the event loop.
     *
     * If the channel is CLOSING or CLOSED, nothing will be done.
     *
     * @return future that will be complete when the channel is closed
     */
    @Override
    public CloseFuture closeAsync() {
        for (;;) {
            int state = this.state.get();
            if (state == UNREGISTERED && this.state.compareAndSet(UNREGISTERED, CLOSING)) {
                close0();
                break;
            } else if (state == REGISTERED && this.state.compareAndSet(REGISTERED, CLOSING)) {
                selector.queueChannelClose(this);
                break;
            } else if (state == CLOSING || state == CLOSED) {
                break;
            }
        }
        return closeFuture;
    }

    /**
     * Closes the channel synchronously. This method should only be called from the selector thread. If it is
     * called on an UNREGISTERED channel or from a non-selector thread an exception will be thrown.
     *
     * Once this method returns, the channel will be closed.
     */
    @Override
    public void closeFromSelector() {
        for (;;) {
            int state = this.state.get();
            if (state == UNREGISTERED) {
                throw new IllegalStateException("Cannot close() an unregistered channel. Use closeAsync()");
            } else if (selector.isOnCurrentThread() == false) {
                throw new IllegalStateException("Cannot close() a channel from a non-selector thread");
            } else if (state == REGISTERED && this.state.compareAndSet(REGISTERED, CLOSING)) {
                close0();
            } else if (state == CLOSING) {
                close0();
            } else if (state == CLOSED) {
                break;
            }
        }
    }

    @Override
    public boolean register(ESSelector selector) throws ClosedChannelException {
        if (markRegistered(selector)) {
            setSelectionKey(socketChannel.register(selector.rawSelector(), 0));
            return true;
        } else {
            return false;
        }
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
    public CloseFuture getCloseFuture() {
        return closeFuture;
    }

    @Override
    public S getRawChannel() {
        return socketChannel;
    }

    void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    boolean markRegistered(ESSelector selector) {
        this.selector = selector;
        return state.compareAndSet(UNREGISTERED, REGISTERED);
    }

    private void close0() {
        if (this.state.compareAndSet(CLOSING, CLOSED)) {
            try {
                socketChannel.close();
                closeFuture.channelClosed(this);
            } catch (IOException e) {
                closeFuture.channelCloseThrewException(this, e);
            }
        }
    }
}
