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

import org.elasticsearch.mocksocket.PrivilegedSocketAccess;
import org.elasticsearch.transport.nio.AcceptingSelector;
import org.elasticsearch.transport.nio.SocketSelector;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public abstract class ChannelFactory<ServerSocket extends NioServerSocketChannel, Socket extends NioSocketChannel> {

    private final ChannelFactory.RawChannelFactory rawChannelFactory;

    /**
     * This will create a {@link ChannelFactory} using the raw channel factory passed to the constructor.
     *
     * @param rawChannelFactory a factory that will construct the raw socket channels
     */
    ChannelFactory(RawChannelFactory rawChannelFactory) {
        this.rawChannelFactory = rawChannelFactory;
    }

    public Socket openNioChannel(InetSocketAddress remoteAddress, SocketSelector selector) throws IOException {
        SocketChannel rawChannel = rawChannelFactory.openNioChannel(remoteAddress);
        Socket channel = internalCreateChannel(selector, rawChannel);
        scheduleChannel(channel, selector);
        return channel;
    }

    public Socket acceptNioChannel(NioServerSocketChannel serverChannel, SocketSelector selector) throws IOException {
        SocketChannel rawChannel = rawChannelFactory.acceptNioChannel(serverChannel);
        Socket channel = internalCreateChannel(selector, rawChannel);
        scheduleChannel(channel, selector);
        return channel;
    }

    public ServerSocket openNioServerSocketChannel(InetSocketAddress address, AcceptingSelector selector) throws IOException {
        ServerSocketChannel rawChannel = rawChannelFactory.openNioServerSocketChannel(address);
        ServerSocket serverChannel = internalCreateServerChannel(selector, rawChannel);
        scheduleServerChannel(serverChannel, selector);
        return serverChannel;
    }

    /**
     * This method should return a new {@link NioSocketChannel} implementation. When this method has
     * returned, the channel should be fully created and setup. Read and write contexts and the channel
     * exception handler should have been set.
     *
     * @param selector the channel will be registered with
     * @param channel the raw channel
     * @return the channel
     * @throws IOException related to the creation of the channel
     */
    public abstract Socket createChannel(SocketSelector selector, SocketChannel channel) throws IOException;

    /**
     * This method should return a new {@link NioServerSocketChannel} implementation. When this method has
     * returned, the channel should be fully created and setup.
     *
     * @param selector the channel will be registered with
     * @param channel the raw channel
     * @return the server channel
     * @throws IOException related to the creation of the channel
     */
    public abstract ServerSocket createServerChannel(AcceptingSelector selector, ServerSocketChannel channel) throws IOException;

    private Socket internalCreateChannel(SocketSelector selector, SocketChannel rawChannel) throws IOException {
        try {
            Socket channel = createChannel(selector, rawChannel);
            assert channel.getReadContext() != null : "read context should have been set on channel";
            assert channel.getWriteContext() != null : "write context should have been set on channel";
            assert channel.getExceptionContext() != null : "exception handler should have been set on channel";
            return channel;
        } catch (Exception e) {
            closeRawChannel(rawChannel, e);
            throw e;
        }
    }

    private ServerSocket internalCreateServerChannel(AcceptingSelector selector, ServerSocketChannel rawChannel) throws IOException {
        try {
            return createServerChannel(selector, rawChannel);
        } catch (Exception e) {
            closeRawChannel(rawChannel, e);
            throw e;
        }
    }

    private void scheduleChannel(Socket channel, SocketSelector selector) {
        try {
            selector.scheduleForRegistration(channel);
        } catch (IllegalStateException e) {
            closeRawChannel(channel.getRawChannel(), e);
            throw e;
        }
    }

    private void scheduleServerChannel(ServerSocket channel, AcceptingSelector selector) {
        try {
            selector.scheduleForRegistration(channel);
        } catch (IllegalStateException e) {
            closeRawChannel(channel.getRawChannel(), e);
            throw e;
        }
    }

    private static void closeRawChannel(Closeable c, Exception e) {
        try {
            c.close();
        } catch (IOException closeException) {
            e.addSuppressed(closeException);
        }
    }

    static class RawChannelFactory {

        private final boolean tcpNoDelay;
        private final boolean tcpKeepAlive;
        private final boolean tcpReusedAddress;
        private final int tcpSendBufferSize;
        private final int tcpReceiveBufferSize;

        RawChannelFactory(boolean tcpNoDelay, boolean tcpKeepAlive, boolean tcpReusedAddress, int tcpSendBufferSize,
                          int tcpReceiveBufferSize) {
            this.tcpNoDelay = tcpNoDelay;
            this.tcpKeepAlive = tcpKeepAlive;
            this.tcpReusedAddress = tcpReusedAddress;
            this.tcpSendBufferSize = tcpSendBufferSize;
            this.tcpReceiveBufferSize = tcpReceiveBufferSize;
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
            try {
                configureSocketChannel(socketChannel);
            } catch (IOException e) {
                closeRawChannel(socketChannel, e);
                throw e;
            }
            return socketChannel;
        }

        ServerSocketChannel openNioServerSocketChannel(InetSocketAddress address) throws IOException {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            java.net.ServerSocket socket = serverSocketChannel.socket();
            try {
                socket.setReuseAddress(tcpReusedAddress);
                serverSocketChannel.bind(address);
            } catch (IOException e) {
                closeRawChannel(serverSocketChannel, e);
                throw e;
            }
            return serverSocketChannel;
        }

        private void configureSocketChannel(SocketChannel channel) throws IOException {
            channel.configureBlocking(false);
            java.net.Socket socket = channel.socket();
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
