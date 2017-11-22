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

import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.nio.AcceptingSelector;
import org.elasticsearch.transport.nio.SocketSelector;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

/**
 * This is an implementation of {@link ChannelFactory} which returns channels that adhere to the
 * {@link org.elasticsearch.transport.TcpChannel} interface. The channels will use the provided
 * {@link TcpTransport.ProfileSettings}. The provided context setters will be called with the channel after
 * construction.
 */
public class TcpChannelFactory extends ChannelFactory<TcpNioServerSocketChannel, TcpNioSocketChannel> {

    private final Consumer<NioSocketChannel> contextSetter;
    private final Consumer<NioServerSocketChannel> serverContextSetter;

    public TcpChannelFactory(TcpTransport.ProfileSettings profileSettings, Consumer<NioSocketChannel> contextSetter,
                             Consumer<NioServerSocketChannel> serverContextSetter) {
        super(new RawChannelFactory(profileSettings.tcpNoDelay,
            profileSettings.tcpKeepAlive,
            profileSettings.reuseAddress,
            Math.toIntExact(profileSettings.sendBufferSize.getBytes()),
            Math.toIntExact(profileSettings.receiveBufferSize.getBytes())));
        this.contextSetter = contextSetter;
        this.serverContextSetter = serverContextSetter;
    }

    @Override
    public TcpNioSocketChannel createChannel(SocketSelector selector, SocketChannel channel) throws IOException {
        TcpNioSocketChannel nioChannel = new TcpNioSocketChannel(channel, selector);
        contextSetter.accept(nioChannel);
        return nioChannel;
    }

    @Override
    public TcpNioServerSocketChannel createServerChannel(AcceptingSelector selector, ServerSocketChannel channel) throws IOException {
        TcpNioServerSocketChannel nioServerChannel = new TcpNioServerSocketChannel(channel, this, selector);
        serverContextSetter.accept(nioServerChannel);
        return nioServerChannel;
    }
}
