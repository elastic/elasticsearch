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

package org.elasticsearch.transport.nio;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.SelectionKeyUtils;
import org.elasticsearch.transport.nio.channel.WriteContext;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * Event handler designed to handle events from non-server sockets
 */
public class SocketEventHandler extends EventHandler {

    private final BiConsumer<NioSocketChannel, Exception> exceptionHandler;
    private final Logger logger;

    public SocketEventHandler(Logger logger, BiConsumer<NioSocketChannel, Exception> exceptionHandler, OpenChannels openChannels) {
        super(logger, openChannels);
        this.exceptionHandler = exceptionHandler;
        this.logger = logger;
    }

    /**
     * This method is called when a NioSocketChannel is successfully registered. It should only be called
     * once per channel.
     *
     * @param channel that was registered
     */
    void handleRegistration(NioSocketChannel channel) {
        SelectionKeyUtils.setConnectAndReadInterested(channel);
    }

    /**
     * This method is called when an attempt to register a channel throws an exception.
     *
     * @param channel that was registered
     * @param exception that occurred
     */
    void registrationException(NioSocketChannel channel, Exception exception) {
        logger.debug(() -> new ParameterizedMessage("failed to register socket channel: {}", channel), exception);
        exceptionCaught(channel, exception);
    }

    /**
     * This method is called when a NioSocketChannel is successfully connected. It should only be called
     * once per channel.
     *
     * @param channel that was registered
     */
    void handleConnect(NioSocketChannel channel) {
        SelectionKeyUtils.removeConnectInterested(channel);
    }

    /**
     * This method is called when an attempt to connect a channel throws an exception.
     *
     * @param channel that was connecting
     * @param exception that occurred
     */
    void connectException(NioSocketChannel channel, Exception exception) {
        logger.debug(() -> new ParameterizedMessage("failed to connect to socket channel: {}", channel), exception);
        exceptionCaught(channel, exception);
    }

    /**
     * This method is called when a channel signals it is ready for be read. All of the read logic should
     * occur in this call.
     *
     * @param channel that can be read
     */
    void handleRead(NioSocketChannel channel) throws IOException {
        int bytesRead = channel.getReadContext().read();
        if (bytesRead == -1) {
            handleClose(channel);
        }
    }

    /**
     * This method is called when an attempt to read from a channel throws an exception.
     *
     * @param channel that was being read
     * @param exception that occurred
     */
    void readException(NioSocketChannel channel, Exception exception) {
        logger.debug(() -> new ParameterizedMessage("exception while reading from socket channel: {}", channel), exception);
        exceptionCaught(channel, exception);
    }

    /**
     * This method is called when a channel signals it is ready to receive writes. All of the write logic
     * should occur in this call.
     *
     * @param channel that can be read
     */
    void handleWrite(NioSocketChannel channel) throws IOException {
        WriteContext channelContext = channel.getWriteContext();
        channelContext.flushChannel();
        if (channelContext.hasQueuedWriteOps()) {
            SelectionKeyUtils.setWriteInterested(channel);
        } else {
            SelectionKeyUtils.removeWriteInterested(channel);
        }
    }

    /**
     * This method is called when an attempt to write to a channel throws an exception.
     *
     * @param channel that was being written to
     * @param exception that occurred
     */
    void writeException(NioSocketChannel channel, Exception exception) {
        logger.debug(() -> new ParameterizedMessage("exception while writing to socket channel: {}", channel), exception);
        exceptionCaught(channel, exception);
    }

    /**
     * This method is called when handling an event from a channel fails due to an unexpected exception.
     * An example would be if checking ready ops on a {@link java.nio.channels.SelectionKey} threw
     * {@link java.nio.channels.CancelledKeyException}.
     *
     * @param channel that caused the exception
     * @param exception that was thrown
     */
    void genericChannelException(NioChannel channel, Exception exception) {
        super.genericChannelException(channel, exception);
        exceptionCaught((NioSocketChannel) channel, exception);
    }

    private void exceptionCaught(NioSocketChannel channel, Exception e) {
        exceptionHandler.accept(channel, e);
    }
}
