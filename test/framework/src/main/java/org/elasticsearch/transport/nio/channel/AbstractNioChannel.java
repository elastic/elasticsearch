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

public abstract class AbstractNioChannel<S extends SelectableChannel & NetworkChannel> implements NioChannel {

    static final int UNREGISTERED = 0;
    static final int REGISTERED = 1;
    static final int CLOSING = 2;
    static final int CLOSED = 3;

    protected final S socketChannel;
    protected ESSelector selector;
    protected final AtomicInteger state = new AtomicInteger(UNREGISTERED);

    private final InetSocketAddress localAddress;
    private final String profile;
    private final CloseFuture closeFuture = new CloseFuture();
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

    @Override
    public CloseFuture close() {
        return close(true);
    }

    @Override
    public CloseFuture close(boolean attemptToCloseImmediately) {
        int state = this.state.get();
        if (attemptToCloseImmediately && (state == UNREGISTERED || selector.onThread())) {
            if (state != UNREGISTERED) {
                close0();
            } else if (this.state.compareAndSet(UNREGISTERED, CLOSING)) {
                close0();
            }
        } else if (state == REGISTERED && this.state.compareAndSet(REGISTERED, CLOSING)) {
            selector.queueChannelClose(this);
        }
        return closeFuture;
    }

    @Override
    public boolean markRegistered(ESSelector selector) {
        this.selector = selector;
        return state.compareAndSet(UNREGISTERED, REGISTERED);
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
    public S rawChannel() {
        return socketChannel;
    }

    void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    private void close0() {
        if (this.state.compareAndSet(CLOSING, CLOSED) && closeFuture.isDone() == false) {
            try {
                socketChannel.close();
                closeFuture.channelClosed(this);
            } catch (IOException e) {
                closeFuture.channelCloseThrewException(this, e);
            }
        }
    }
}
