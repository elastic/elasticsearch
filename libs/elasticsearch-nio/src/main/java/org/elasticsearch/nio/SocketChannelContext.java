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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This context should implement the specific logic for a channel. When a channel receives a notification
 * that it is ready to perform certain operations (read, write, etc) the {@link SocketChannelContext} will
 * be called. This context will need to implement all protocol related logic. Additionally, if any special
 * close behavior is required, it should be implemented in this context.
 *
 * The only methods of the context that should ever be called from a non-selector thread are
 * {@link #closeChannel()} and {@link #sendMessage(ByteBuffer[], BiConsumer)}.
 */
public abstract class SocketChannelContext extends ChannelContext<SocketChannel> {

    protected final NioSocketChannel channel;
    private final SocketSelector selector;
    private final CompletableFuture<Void> connectContext = new CompletableFuture<>();
    private boolean ioException;
    private boolean peerClosed;
    private Exception connectException;

    protected SocketChannelContext(NioSocketChannel channel, SocketSelector selector, Consumer<Exception> exceptionHandler) {
        super(channel.getRawChannel(), exceptionHandler);
        this.selector = selector;
        this.channel = channel;
    }

    @Override
    public SocketSelector getSelector() {
        return selector;
    }

    @Override
    public NioSocketChannel getChannel() {
        return channel;
    }

    public void addConnectListener(BiConsumer<Void, Throwable> listener) {
        connectContext.whenComplete(listener);
    }

    public boolean isConnectComplete() {
        return connectContext.isDone() && connectContext.isCompletedExceptionally() == false;
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
    public boolean connect() throws IOException {
        if (isConnectComplete()) {
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

        boolean isConnected = rawChannel.isConnected();
        if (isConnected == false) {
            try {
                isConnected = rawChannel.finishConnect();
            } catch (IOException | RuntimeException e) {
                connectException = e;
                connectContext.completeExceptionally(e);
                throw e;
            }
        }
        if (isConnected) {
            connectContext.complete(null);
        }
        return isConnected;
    }

    public abstract int read() throws IOException;

    public abstract void sendMessage(ByteBuffer[] buffers, BiConsumer<Void, Throwable> listener);

    public abstract void queueWriteOperation(WriteOperation writeOperation);

    public abstract void flushChannel() throws IOException;

    public abstract boolean hasQueuedWriteOps();

    /**
     * This method indicates if a selector should close this channel.
     *
     * @return a boolean indicating if the selector should close
     */
    public abstract boolean selectorShouldClose();

    protected boolean hasIOException() {
        return ioException;
    }

    protected boolean isPeerClosed() {
        return peerClosed;
    }

    protected int readFromChannel(ByteBuffer buffer) throws IOException {
        try {
            int bytesRead = rawChannel.read(buffer);
            if (bytesRead < 0) {
                peerClosed = true;
                bytesRead = 0;
            }
            return bytesRead;
        } catch (IOException e) {
            ioException = true;
            throw e;
        }
    }

    protected int readFromChannel(ByteBuffer[] buffers) throws IOException {
        try {
            int bytesRead = (int) rawChannel.read(buffers);
            if (bytesRead < 0) {
                peerClosed = true;
                bytesRead = 0;
            }
            return bytesRead;
        } catch (IOException e) {
            ioException = true;
            throw e;
        }
    }

    protected int flushToChannel(ByteBuffer buffer) throws IOException {
        try {
            return rawChannel.write(buffer);
        } catch (IOException e) {
            ioException = true;
            throw e;
        }
    }

    protected int flushToChannel(ByteBuffer[] buffers) throws IOException {
        try {
            return (int) rawChannel.write(buffers);
        } catch (IOException e) {
            ioException = true;
            throw e;
        }
    }

    @FunctionalInterface
    public interface ReadConsumer {
        int consumeReads(InboundChannelBuffer channelBuffer) throws IOException;
    }
}
