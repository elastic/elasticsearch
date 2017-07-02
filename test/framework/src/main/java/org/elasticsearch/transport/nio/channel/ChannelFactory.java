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
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.PrivilegedSocketAccess;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.nio.AcceptingSelector;
import org.elasticsearch.transport.nio.SocketSelector;
import org.elasticsearch.transport.nio.TcpReadHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.function.Consumer;

public class ChannelFactory {

    private final TcpReadHandler handler;
    private final RawChannelFactory rawChannelFactory;

    public ChannelFactory(Settings settings, TcpReadHandler handler) {
        this(new RawChannelFactory(settings), handler);
    }

    ChannelFactory(RawChannelFactory rawChannelFactory, TcpReadHandler handler) {
        this.handler = handler;
        this.rawChannelFactory = rawChannelFactory;
    }

    public NioSocketChannel openNioChannel(InetSocketAddress remoteAddress, SocketSelector selector) throws IOException {
        return openNioChannel(remoteAddress, selector, null);
    }

    public NioSocketChannel openNioChannel(InetSocketAddress remoteAddress, SocketSelector selector,
                                           Consumer<NioChannel> closeListener) throws IOException {
        SocketChannel rawChannel = rawChannelFactory.openNioChannel(remoteAddress);
        NioSocketChannel channel = new NioSocketChannel(NioChannel.CLIENT, rawChannel, selector);
        channel.setContexts(new TcpReadContext(channel, handler), new TcpWriteContext(channel));
        if (closeListener != null) {
            channel.getCloseFuture().setListener(closeListener);
        }
        scheduleChannel(channel, selector);
        return channel;
    }

    public NioSocketChannel acceptNioChannel(NioServerSocketChannel serverChannel, SocketSelector selector) throws IOException {
        return acceptNioChannel(serverChannel, selector, null);
    }

    public NioSocketChannel acceptNioChannel(NioServerSocketChannel serverChannel, SocketSelector selector,
                                             Consumer<NioChannel> closeListener) throws IOException {
        SocketChannel rawChannel = rawChannelFactory.acceptNioChannel(serverChannel);
        NioSocketChannel channel = new NioSocketChannel(serverChannel.getProfile(), rawChannel, selector);
        channel.setContexts(new TcpReadContext(channel, handler), new TcpWriteContext(channel));
        if (closeListener != null) {
            channel.getCloseFuture().setListener(closeListener);
        }
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

        RawChannelFactory(Settings settings) {
            tcpNoDelay = TcpTransport.TCP_NO_DELAY.get(settings);
            tcpKeepAlive = TcpTransport.TCP_KEEP_ALIVE.get(settings);
            tcpReusedAddress = TcpTransport.TCP_REUSE_ADDRESS.get(settings);
            tcpSendBufferSize = Math.toIntExact(TcpTransport.TCP_SEND_BUFFER_SIZE.get(settings).getBytes());
            tcpReceiveBufferSize = Math.toIntExact(TcpTransport.TCP_RECEIVE_BUFFER_SIZE.get(settings).getBytes());
        }

        SocketChannel openNioChannel(InetSocketAddress remoteAddress) throws IOException {
            SocketChannel socketChannel = SocketChannel.open();
            configureSocketChannel(socketChannel);
            PrivilegedSocketAccess.connect(socketChannel, remoteAddress);
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
            socket.setReuseAddress(tcpReusedAddress);
            serverSocketChannel.bind(address);
            return serverSocketChannel;
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

        private static <T> T getSocketChannel(CheckedSupplier<T, IOException> supplier) throws IOException {
            try {
                return AccessController.doPrivileged((PrivilegedExceptionAction<T>) supplier::get);
            } catch (PrivilegedActionException e) {
                throw (IOException) e.getCause();
            }
        }
    }
}
