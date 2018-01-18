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
 * that it is ready to perform certain operations (read, write, etc) the {@link ChannelContext} will be
 * called. This context will need to implement all protocol related logic. Additionally, if any special
 * close behavior is required, it should be implemented in this context.
 *
 * The only methods of the context that should ever be called from a non-selector thread are
 * {@link #closeChannel()} and {@link #sendMessage(ByteBuffer[], BiConsumer)}.
 */
public interface ChannelContext {

    void channelRegistered() throws IOException;

    int read() throws IOException;

    void sendMessage(ByteBuffer[] buffers, BiConsumer<Void, Throwable> listener);

    void queueWriteOperation(WriteOperation writeOperation);

    void flushChannel() throws IOException;

    boolean hasQueuedWriteOps();

    /**
     * Schedules a channel to be closed by the selector event loop with which it is registered.
     * <p>
     * If the channel is open and the state can be transitioned to closed, the close operation will
     * be scheduled with the event loop.
     * <p>
     * If the channel is already set to closed, it is assumed that it is already scheduled to be closed.
     * <p>
     * Depending on the underlying protocol of the channel, a close operation might simply close the socket
     * channel or may involve reading and writing messages.
     */
    void closeChannel();

    /**
     * This method indicates if a selector should close this channel.
     *
     * @return a boolean indicating if the selector should close
     */
    boolean selectorShouldClose();

    /**
     * This method cleans up any context resources that need to be released when a channel is closed. It
     * should only be called by the selector thread.
     *
     * @throws IOException during channel / context close
     */
    void closeFromSelector() throws IOException;

    @FunctionalInterface
    interface ReadConsumer {
        int consumeReads(InboundChannelBuffer channelBuffer) throws IOException;
    }
}
