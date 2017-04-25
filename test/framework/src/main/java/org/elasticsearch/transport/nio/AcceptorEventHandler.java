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
import org.elasticsearch.transport.nio.channel.ChannelFactory;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioServerSocketChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.SKUtils;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AcceptorEventHandler extends EventHandler {

    private final Supplier<SocketSelector> selectorSupplier;
    private final OpenChannels openChannels;

    public AcceptorEventHandler(Logger logger, OpenChannels openChannels, Supplier<SocketSelector> selectorSupplier) {
        super(logger);
        this.openChannels = openChannels;
        this.selectorSupplier = selectorSupplier;
    }

    public void serverChannelRegistered(NioServerSocketChannel nioServerSocketChannel) {
        SKUtils.setAcceptInterested(nioServerSocketChannel);
        openChannels.serverChannelOpened(nioServerSocketChannel);
    }

    public void acceptChannel(NioServerSocketChannel nioChannel) throws IOException {
        ChannelFactory channelFactory = nioChannel.getChannelFactory();
        NioSocketChannel nioSocketChannel = channelFactory.acceptNioChannel(nioChannel);
        openChannels.acceptedChannelOpened(nioSocketChannel);
        nioSocketChannel.getCloseFuture().setListener(openChannels::channelClosed);
        selectorSupplier.get().registerSocketChannel(nioSocketChannel);
    }

    public void acceptException(NioServerSocketChannel nioChannel, IOException exception) {
        logger.debug("exception while accepting new channel", exception);
    }

    public void genericServerChannelException(NioServerSocketChannel channel, Exception e) {
        logger.trace("event handling exception", e);
    }
}
