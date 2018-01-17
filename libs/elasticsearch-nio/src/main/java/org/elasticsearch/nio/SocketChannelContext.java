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
import java.util.function.BiConsumer;

/**
 * This context should implement the specific logic for a channel. When a channel receives a notification
 * that it is ready to perform certain operations (read, write, etc) the {@link SocketChannelContext} will
 * be called. This context will need to implement all protocol related logic. Additionally, if any special
 * close behavior is required, it should be implemented in this context.
 *
 * The only methods of the context that should ever be called from a non-selector thread are
 * {@link #closeChannel()} and {@link #sendMessage(ByteBuffer[], BiConsumer)}.
 */
public abstract class SocketChannelContext implements ChannelContext {

    protected final NioSocketChannel channel;
    private final BiConsumer<NioSocketChannel, Exception> exceptionHandler;
    private boolean ioException;
    private boolean peerClosed;

    protected SocketChannelContext(NioSocketChannel channel, BiConsumer<NioSocketChannel, Exception> exceptionHandler) {
        this.channel = channel;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public void handleException(Exception e) {
        exceptionHandler.accept(channel, e);
    }

    public void channelRegistered() throws IOException {}

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
            int bytesRead = channel.read(buffer);
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
            int bytesRead = channel.read(buffers);
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
            return channel.write(buffer);
        } catch (IOException e) {
            ioException = true;
            throw e;
        }
    }

    protected int flushToChannel(ByteBuffer[] buffers) throws IOException {
        try {
            return channel.write(buffers);
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
