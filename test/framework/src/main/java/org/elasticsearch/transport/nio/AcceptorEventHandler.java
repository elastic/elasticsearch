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
import org.elasticsearch.transport.nio.channel.ChannelFactory;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioServerSocketChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.SelectionKeyUtils;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Event handler designed to handle events from server sockets
 */
public class AcceptorEventHandler extends EventHandler {

    private final Supplier<SocketSelector> selectorSupplier;
    private final Consumer<NioChannel> acceptedChannelCallback;
    private final OpenChannels openChannels;

    public AcceptorEventHandler(Logger logger, OpenChannels openChannels, Supplier<SocketSelector> selectorSupplier,
                                Consumer<NioChannel> acceptedChannelCallback) {
        super(logger, openChannels);
        this.openChannels = openChannels;
        this.selectorSupplier = selectorSupplier;
        this.acceptedChannelCallback = acceptedChannelCallback;
    }

    /**
     * This method is called when a NioServerSocketChannel is successfully registered. It should only be
     * called once per channel.
     *
     * @param nioServerSocketChannel that was registered
     */
    void serverChannelRegistered(NioServerSocketChannel nioServerSocketChannel) {
        SelectionKeyUtils.setAcceptInterested(nioServerSocketChannel);
        openChannels.serverChannelOpened(nioServerSocketChannel);
    }

    /**
     * This method is called when an attempt to register a server channel throws an exception.
     *
     * @param channel that was registered
     * @param exception that occurred
     */
    void registrationException(NioServerSocketChannel channel, Exception exception) {
        logger.error(new ParameterizedMessage("failed to register server channel: {}", channel), exception);
    }

    /**
     * This method is called when a server channel signals it is ready to accept a connection. All of the
     * accept logic should occur in this call.
     *
     * @param nioServerChannel that can accept a connection
     */
    void acceptChannel(NioServerSocketChannel nioServerChannel) throws IOException {
        ChannelFactory channelFactory = nioServerChannel.getChannelFactory();
        SocketSelector selector = selectorSupplier.get();
        NioSocketChannel nioSocketChannel = channelFactory.acceptNioChannel(nioServerChannel, selector);
        openChannels.acceptedChannelOpened(nioSocketChannel);
        acceptedChannelCallback.accept(nioSocketChannel);
    }

    /**
     * This method is called when an attempt to accept a connection throws an exception.
     *
     * @param nioServerChannel that accepting a connection
     * @param exception that occurred
     */
    void acceptException(NioServerSocketChannel nioServerChannel, Exception exception) {
        logger.debug(() -> new ParameterizedMessage("exception while accepting new channel from server channel: {}",
            nioServerChannel), exception);
    }
}
