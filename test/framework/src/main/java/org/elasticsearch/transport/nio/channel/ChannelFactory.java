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

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.PrivilegedSocketAccess;
import org.elasticsearch.transport.TcpTransport;
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

public class ChannelFactory {

    private final boolean tcpNoDelay;
    private final boolean tcpKeepAlive;
    private final boolean tcpReusedAddress;
    private final int tcpSendBufferSize;
    private final int tcpReceiveBufferSize;
    private final TcpReadHandler handler;

    public ChannelFactory(Settings settings, TcpReadHandler handler) {
        tcpNoDelay = TcpTransport.TCP_NO_DELAY.get(settings);
        tcpKeepAlive = TcpTransport.TCP_KEEP_ALIVE.get(settings);
        tcpReusedAddress = TcpTransport.TCP_REUSE_ADDRESS.get(settings);
        tcpSendBufferSize = Math.toIntExact(TcpTransport.TCP_SEND_BUFFER_SIZE.get(settings).getBytes());
        tcpReceiveBufferSize = Math.toIntExact(TcpTransport.TCP_RECEIVE_BUFFER_SIZE.get(settings).getBytes());
        this.handler = handler;
    }

    public NioSocketChannel openNioChannel(InetSocketAddress remoteAddress) throws IOException {
        SocketChannel rawChannel = SocketChannel.open();
        configureSocketChannel(rawChannel);
        PrivilegedSocketAccess.connect(rawChannel, remoteAddress);
        NioSocketChannel channel = new NioSocketChannel(NioChannel.CLIENT, rawChannel);
        channel.setContexts(new TcpReadContext(channel, handler), new TcpWriteContext(channel));
        return channel;
    }

    public NioSocketChannel acceptNioChannel(NioServerSocketChannel serverChannel) throws IOException {
        ServerSocketChannel serverSocketChannel = serverChannel.getRawChannel();
        SocketChannel rawChannel = PrivilegedSocketAccess.accept(serverSocketChannel);
        configureSocketChannel(rawChannel);
        NioSocketChannel channel = new NioSocketChannel(serverChannel.getProfile(), rawChannel);
        channel.setContexts(new TcpReadContext(channel, handler), new TcpWriteContext(channel));
        return channel;
    }

    public NioServerSocketChannel openNioServerSocketChannel(String profileName, InetSocketAddress address)
        throws IOException {
        ServerSocketChannel socketChannel = ServerSocketChannel.open();
        socketChannel.configureBlocking(false);
        ServerSocket socket = socketChannel.socket();
        socket.setReuseAddress(tcpReusedAddress);
        socketChannel.bind(address);
        return new NioServerSocketChannel(profileName, socketChannel, this);
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
