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


import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.mocksocket.PrivilegedSocketAccess;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.nio.AcceptingSelector;
import org.elasticsearch.transport.nio.SocketSelector;
import org.elasticsearch.transport.nio.TcpReadHandler;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

public class ChannelFactory {

    private final TcpReadHandler handler;
    private final RawChannelFactory rawChannelFactory;

    public ChannelFactory(TcpTransport.ProfileSettings profileSettings, TcpReadHandler handler) {
        this(new RawChannelFactory(profileSettings), handler);
    }

    ChannelFactory(RawChannelFactory rawChannelFactory, TcpReadHandler handler) {
        this.handler = handler;
        this.rawChannelFactory = rawChannelFactory;
    }

    public NioSocketChannel openNioChannel(InetSocketAddress remoteAddress, SocketSelector selector,
                                           Consumer<NioChannel> closeListener) throws IOException {
        SocketChannel rawChannel = rawChannelFactory.openNioChannel(remoteAddress);
        NioSocketChannel channel = new NioSocketChannel(NioChannel.CLIENT, rawChannel, selector);
        channel.setContexts(new TcpReadContext(channel, handler), new TcpWriteContext(channel));
        channel.getCloseFuture().addListener(ActionListener.wrap(closeListener::accept, (e) -> closeListener.accept(channel)));
        scheduleChannel(channel, selector);
        return channel;
    }

    public NioSocketChannel acceptNioChannel(NioServerSocketChannel serverChannel, SocketSelector selector,
                                             Consumer<NioChannel> closeListener) throws IOException {
        SocketChannel rawChannel = rawChannelFactory.acceptNioChannel(serverChannel);
        NioSocketChannel channel = new NioSocketChannel(serverChannel.getProfile(), rawChannel, selector);
        channel.setContexts(new TcpReadContext(channel, handler), new TcpWriteContext(channel));
        channel.getCloseFuture().addListener(ActionListener.wrap(closeListener::accept, (e) -> closeListener.accept(channel)));
        scheduleChannel(channel, selector);
        return channel;
    }

    public NioServerSocketChannel openNioServerSocketChannel(String profileName, InetSocketAddress address, AcceptingSelector selector)
        throws IOException {
        ServerSocketChannel rawChannel = rawChannelFactory.openNioServerSocketChannel(address);
        NioServerSocketChannel serverChannel = new NioServerSocketChannel(profileName, rawChannel, this, selector);
        scheduleServerChannel(serverChannel, selector);
        return serverChannel;
    }

    private void scheduleChannel(NioSocketChannel channel, SocketSelector selector) {
        try {
            selector.scheduleForRegistration(channel);
        } catch (IllegalStateException e) {
            IOUtils.closeWhileHandlingException(channel.getRawChannel());
            throw e;
        }
    }

    private void scheduleServerChannel(NioServerSocketChannel channel, AcceptingSelector selector) {
        try {
            selector.scheduleForRegistration(channel);
        } catch (IllegalStateException e) {
            IOUtils.closeWhileHandlingException(channel.getRawChannel());
            throw e;
        }
    }

    static class RawChannelFactory {

        private final boolean tcpNoDelay;
        private final boolean tcpKeepAlive;
        private final boolean tcpReusedAddress;
        private final int tcpSendBufferSize;
        private final int tcpReceiveBufferSize;

        RawChannelFactory(TcpTransport.ProfileSettings profileSettings) {
            tcpNoDelay = profileSettings.tcpNoDelay;
            tcpKeepAlive = profileSettings.tcpKeepAlive;
            tcpReusedAddress = profileSettings.reuseAddress;
            tcpSendBufferSize = Math.toIntExact(profileSettings.sendBufferSize.getBytes());
            tcpReceiveBufferSize = Math.toIntExact(profileSettings.receiveBufferSize.getBytes());
        }

        SocketChannel openNioChannel(InetSocketAddress remoteAddress) throws IOException {
            SocketChannel socketChannel = SocketChannel.open();
            try {
                configureSocketChannel(socketChannel);
                PrivilegedSocketAccess.connect(socketChannel, remoteAddress);
            } catch (IOException e) {
                closeRawChannel(socketChannel, e);
                throw e;
            }
            return socketChannel;
        }

        SocketChannel acceptNioChannel(NioServerSocketChannel serverChannel) throws IOException {
            ServerSocketChannel serverSocketChannel = serverChannel.getRawChannel();
            SocketChannel socketChannel = PrivilegedSocketAccess.accept(serverSocketChannel);
            configureSocketChannel(socketChannel);
            return socketChannel;
        }

        ServerSocketChannel openNioServerSocketChannel(InetSocketAddress address) throws IOException {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            ServerSocket socket = serverSocketChannel.socket();
            try {
                socket.setReuseAddress(tcpReusedAddress);
                serverSocketChannel.bind(address);
            } catch (IOException e) {
                closeRawChannel(serverSocketChannel, e);
                throw e;
            }
            return serverSocketChannel;
        }

        private void closeRawChannel(Closeable c, IOException e) {
            try {
                c.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
        }

        private void configureSocketChannel(SocketChannel channel) throws IOException {
            channel.configureBlocking(false);
            Socket socket = channel.socket();
            socket.setTcpNoDelay(tcpNoDelay);
            socket.setKeepAlive(tcpKeepAlive);
            socket.setReuseAddress(tcpReusedAddress);
            if (tcpSendBufferSize > 0) {
                socket.setSendBufferSize(tcpSendBufferSize);
            }
            if (tcpReceiveBufferSize > 0) {
                socket.setSendBufferSize(tcpReceiveBufferSize);
            }
        }
    }
}
