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
 * <p>
 * A channel is open once it is constructed. The channel remains open and {@link #isOpen()} will return
 * true until the channel is explicitly closed.
 * <p>
 * A channel lifecycle has three stages:
 * <ol>
 * <li>OPEN - When a channel has been created. This is the state of a channel that can perform normal operations.
 * <li>CLOSING - When a channel has been marked for closed, but is not yet closed. {@link #isOpen()} will
 * still return true. Normal operations should be rejected. The most common scenario for a channel to be
 * CLOSING is when channel that was OPEN has {@link #closeAsync()} called, but the selector thread
 * has not yet closed the channel.
 * <li>CLOSED - The channel has been closed.
 * </ol>
 *
 * @param <S> the type of raw channel this AbstractNioChannel uses
 */
public abstract class AbstractNioChannel<S extends SelectableChannel & NetworkChannel> implements NioChannel {

    static final int OPEN = 0;
    static final int CLOSING = 1;
    static final int CLOSED = 2;

    final S socketChannel;
    final AtomicInteger state = new AtomicInteger(OPEN);

    private final InetSocketAddress localAddress;
    private final String profile;
    private final CloseFuture closeFuture = new CloseFuture();
    private final ESSelector selector;
    private SelectionKey selectionKey;

    public AbstractNioChannel(String profile, S socketChannel, ESSelector selector) throws IOException {
        this.profile = profile;
        this.socketChannel = socketChannel;
        this.localAddress = (InetSocketAddress) socketChannel.getLocalAddress();
        this.selector = selector;
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
     * Schedules a channel to be closed by the selector event loop with which it is registered.
     * <p>
     * If the channel is OPEN and the state can be transitioned to CLOSING, the close operation will
     * be scheduled with the event loop.
     * <p>
     * If the channel is CLOSING or CLOSED, nothing will be done.
     *
     * @return future that will be complete when the channel is closed
     */
    @Override
    public CloseFuture closeAsync() {
        for (; ; ) {
            int state = this.state.get();
            if (state == OPEN && this.state.compareAndSet(OPEN, CLOSING)) {
                selector.queueChannelClose(this);
                break;
            } else if (state == CLOSING || state == CLOSED) {
                break;
            }
        }
        return closeFuture;
    }

    /**
     * Closes the channel synchronously. This method should only be called from the selector thread.
     * <p>
     * Once this method returns, the channel will be closed.
     */
    @Override
    public void closeFromSelector() {
        // This will not exit the loop until this thread or someone else has set the state to CLOSED.
        // Whichever thread succeeds in setting the state to CLOSED will close the raw channel.
        for (; ; ) {
            int state = this.state.get();
            if (state < CLOSING && this.state.compareAndSet(state, CLOSING)) {
                close0();
            } else if (state == CLOSING) {
                close0();
            } else if (state == CLOSED) {
                break;
            }
        }
    }

    /**
     * This method attempts to registered a channel with its selector. If method returns true the channel was
     * successfully registered. If it returns false, the registration failed. The reason a registered might
     * fail is if something else closed this channel.
     *
     * @return if the channel was successfully registered
     * @throws ClosedChannelException if the raw channel was closed
     */
    @Override
    public boolean register() throws ClosedChannelException {
        setSelectionKey(socketChannel.register(selector.rawSelector(), 0));
        return true;
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

    // Package visibility for testing
    void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    private void close0() {
        if (this.state.compareAndSet(CLOSING, CLOSED)) {
            try {
                socketChannel.close();
                closeFuture.channelClosed(this);
            } catch (IOException e) {
                closeFuture.channelCloseThrewException(this, e);
            } finally {
                if (selector != null) {
                    selector.removeRegisteredChannel(this);
                }
            }
        }
    }
}
