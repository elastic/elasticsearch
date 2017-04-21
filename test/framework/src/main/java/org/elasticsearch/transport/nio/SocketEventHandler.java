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
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.SKUtils;
import org.elasticsearch.transport.nio.channel.WriteContext;

import java.io.IOException;
import java.util.function.BiConsumer;

public class SocketEventHandler extends EventHandler {

    private final BiConsumer<NioSocketChannel, Throwable> exceptionHandler;
    private final Logger logger;

    public SocketEventHandler(Logger logger, BiConsumer<NioSocketChannel, Throwable> exceptionHandler) {
        super(logger);
        this.exceptionHandler = exceptionHandler;
        this.logger = logger;
    }

    public void handleRegistration(NioSocketChannel channel) {
        SKUtils.setConnectAndReadInterested(channel);
    }

    public void registrationException(NioSocketChannel channel, Exception e) {
        logger.trace("failed to register channel", e);
        exceptionCaught(channel, e);
    }

    public void handleConnect(NioSocketChannel channel) {
        SKUtils.removeConnectInterested(channel);
    }

    public void connectException(NioSocketChannel channel, Exception e) {
        logger.trace("failed to connect to channel", e);
        exceptionCaught(channel, e);

    }

    public void handleRead(NioSocketChannel channel) throws IOException {
        int bytesRead = channel.getReadContext().read();
        if (bytesRead == -1) {
            channel.close(false);
        }
    }

    public void readException(NioSocketChannel channel, Exception e) {
        logger.trace("failed to read from channel", e);
        exceptionCaught(channel, e);
    }

    public void handleWrite(NioSocketChannel channel) throws IOException {
        WriteContext channelContext = channel.getWriteContext();
        channelContext.flushChannel();
        if (channelContext.hasQueuedWriteOps()) {
            SKUtils.setWriteInterested(channel);
        } else {
            SKUtils.removeWriteInterested(channel);
        }
    }

    public void writeException(NioSocketChannel channel, Exception e) {
        logger.trace("failed to write to channel", e);
        exceptionCaught(channel, e);
    }

    // TODO: test
    public void genericChannelException(NioSocketChannel channel, Exception e) {
        logger.trace("event handling failed", e);
        exceptionCaught(channel, e);
    }

    private void exceptionCaught(NioSocketChannel channel, Exception e) {
        exceptionHandler.accept(channel, e);
    }
}
