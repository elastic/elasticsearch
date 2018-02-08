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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class NioSocketChannel extends AbstractNioChannel<SocketChannel> {

    private final InetSocketAddress remoteAddress;
    private final CompletableFuture<Void> connectContext = new CompletableFuture<>();
    private final SocketSelector socketSelector;
    private final AtomicBoolean contextSet = new AtomicBoolean(false);
    private SocketChannelContext context;
    private Exception connectException;

    public NioSocketChannel(SocketChannel socketChannel, SocketSelector selector) throws IOException {
        super(socketChannel, selector);
        this.remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
        this.socketSelector = selector;
    }

    @Override
    public SocketSelector getSelector() {
        return socketSelector;
    }

    public int write(ByteBuffer buffer) throws IOException {
        return socketChannel.write(buffer);
    }

    public int write(ByteBuffer[] buffers) throws IOException {
        if (buffers.length == 1) {
            return socketChannel.write(buffers[0]);
        } else {
            return (int) socketChannel.write(buffers);
        }
    }

    public int read(ByteBuffer buffer) throws IOException {
        return socketChannel.read(buffer);
    }

    public int read(ByteBuffer[] buffers) throws IOException {
        if (buffers.length == 1) {
            return socketChannel.read(buffers[0]);
        } else {
            return (int) socketChannel.read(buffers);
        }
    }

    public void setContext(SocketChannelContext context) {
        if (contextSet.compareAndSet(false, true)) {
            this.context = context;
        } else {
            throw new IllegalStateException("Context on this channel were already set. It should only be once.");
        }
    }

    @Override
    public SocketChannelContext getContext() {
        return context;
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public boolean isConnectComplete() {
        return isConnectComplete0();
    }

    /**
     * This method will attempt to complete the connection process for this channel. It should be called for
     * new channels or for a channel that has produced a OP_CONNECT event. If this method returns true then
     * the connection is complete and the channel is ready for reads and writes. If it returns false, the
     * channel is not yet connected and this method should be called again when a OP_CONNECT event is
     * received.
     *
     * @return true if the connection process is complete
     * @throws IOException if an I/O error occurs
     */
    public boolean finishConnect() throws IOException {
        if (isConnectComplete0()) {
            return true;
        } else if (connectContext.isCompletedExceptionally()) {
            Exception exception = connectException;
            if (exception == null) {
                throw new AssertionError("Should have received connection exception");
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
            } else {
                throw (RuntimeException) exception;
            }
        }

        boolean isConnected = socketChannel.isConnected();
        if (isConnected == false) {
            isConnected = internalFinish();
        }
        if (isConnected) {
            connectContext.complete(null);
        }
        return isConnected;
    }

    public void addConnectListener(BiConsumer<Void, Throwable> listener) {
        connectContext.whenComplete(listener);
    }

    @Override
    public String toString() {
        return "NioSocketChannel{" +
            "localAddress=" + getLocalAddress() +
            ", remoteAddress=" + remoteAddress +
            '}';
    }

    private boolean internalFinish() throws IOException {
        try {
            return socketChannel.finishConnect();
        } catch (IOException | RuntimeException e) {
            connectException = e;
            connectContext.completeExceptionally(e);
            throw e;
        }
    }

    private boolean isConnectComplete0() {
        return connectContext.isDone() && connectContext.isCompletedExceptionally() == false;
    }
}
