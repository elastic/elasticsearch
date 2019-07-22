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
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Implements the application specific logic for handling channel operations.
 */
public interface NioChannelHandler {

    /**
     * This method is called when the channel is active for use.
     */
    void channelActive();

    /**
     * This method is called when a message is queued with a channel. It can be called from any thread.
     * This method should validate that the message is a valid type and return a write operation object
     * to be queued with the channel
     *
     * @param context the channel context
     * @param message the message
     * @param listener the listener to be called when the message is sent
     * @return the write operation to be queued
     */
    WriteOperation createWriteOperation(SocketChannelContext context, Object message, BiConsumer<Void, Exception> listener);

    /**
     * This method is called on the event loop thread. It should serialize a write operation object to bytes
     * that can be flushed to the raw nio channel.
     *
     * @param writeOperation to be converted to bytes
     * @return the operations to flush the bytes to the channel
     */
    List<FlushOperation> writeToBytes(WriteOperation writeOperation);

    /**
     * Returns any flush operations that are ready to flush. This exists as a way to check if any flush
     * operations were produced during a read call.
     *
     * @return flush operations
     */
    List<FlushOperation> pollFlushOperations();

    /**
     * This method handles bytes that have been read from the network. It should return the number of bytes
     * consumed so that they can be released.
     *
     * @param channelBuffer of bytes read from the network
     * @return the number of bytes consumed
     * @throws IOException if an exception occurs
     */
    int consumeReads(InboundChannelBuffer channelBuffer) throws IOException;

    /**
     * This method indicates if the underlying channel should be closed.
     *
     * @return if the channel should be closed
     */
    boolean closeNow();

    void close() throws IOException;
}
