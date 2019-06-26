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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.function.Supplier;

public abstract class ChannelFactory<ServerSocket extends NioServerSocketChannel, Socket extends NioSocketChannel> {

    private final ChannelFactory.RawChannelFactory rawChannelFactory;

    /**
     * This will create a {@link ChannelFactory} using the raw channel factory passed to the constructor.
     */
    protected ChannelFactory() {
        this(new RawChannelFactory());
    }

    /**
     * This will create a {@link ChannelFactory} using the raw channel factory passed to the constructor.
     *
     * @param rawChannelFactory a factory that will construct the raw socket channels
     */
    protected ChannelFactory(RawChannelFactory rawChannelFactory) {
        this.rawChannelFactory = rawChannelFactory;
    }

    public Socket openNioChannel(InetSocketAddress remoteAddress, Supplier<NioSelector> supplier) throws IOException {
        SocketChannel rawChannel = rawChannelFactory.openNioChannel();
        NioSelector selector = supplier.get();
        Socket channel = internalCreateChannel(selector, rawChannel);
        scheduleChannel(channel, selector);
        return channel;
    }

    public Socket acceptNioChannel(SocketChannel acceptedChannel, Supplier<NioSelector> supplier) throws IOException {
        SocketChannel rawChannel = rawChannelFactory.acceptNioChannel(acceptedChannel);
        NioSelector selector = supplier.get();
        Socket channel = internalCreateChannel(selector, rawChannel);
        scheduleChannel(channel, selector);
        return channel;
    }

    public ServerSocket openNioServerSocketChannel(InetSocketAddress localAddress, Supplier<NioSelector> supplier) throws IOException {
        ServerSocketChannel rawChannel = rawChannelFactory.openNioServerSocketChannel();
        NioSelector selector = supplier.get();
        ServerSocket serverChannel = internalCreateServerChannel(selector, rawChannel);
        scheduleServerChannel(serverChannel, selector);
        return serverChannel;
    }

    /**
     * This method should return a new {@link NioSocketChannel} implementation. When this method has
     * returned, the channel should be fully created and setup. Read and write contexts and the channel
     * exception handler should have been set.
     *
     * @param selector     the channel will be registered with
     * @param channel      the raw channel
     * @param socketConfig the socket config
     * @return the channel
     * @throws IOException related to the creation of the channel
     */
    public abstract Socket createChannel(NioSelector selector, SocketChannel channel, Config.Socket socketConfig) throws IOException;

    /**
     * This method should return a new {@link NioServerSocketChannel} implementation. When this method has
     * returned, the channel should be fully created and setup.
     *
     * @param selector the channel will be registered with
     * @param channel  the raw channel
     * @param socketConfig the socket config
     * @return the server channel
     * @throws IOException related to the creation of the channel
     */
    public abstract ServerSocket createServerChannel(NioSelector selector, ServerSocketChannel channel, Config.ServerSocket socketConfig)
        throws IOException;

    private Socket internalCreateChannel(NioSelector selector, SocketChannel rawChannel) throws IOException {
        try {
            Socket channel = createChannel(selector, rawChannel, null);
            assert channel.getContext() != null : "channel context should have been set on channel";
            return channel;
        } catch (UncheckedIOException e) {
            // This can happen if getRemoteAddress throws IOException.
            IOException cause = e.getCause();
            closeRawChannel(rawChannel, cause);
            throw cause;
        } catch (Exception e) {
            closeRawChannel(rawChannel, e);
            throw e;
        }
    }

    private ServerSocket internalCreateServerChannel(NioSelector selector, ServerSocketChannel rawChannel) throws IOException {
        try {
            return createServerChannel(selector, rawChannel, null);
        } catch (Exception e) {
            closeRawChannel(rawChannel, e);
            throw e;
        }
    }

    private void scheduleChannel(Socket channel, NioSelector selector) {
        try {
            selector.scheduleForRegistration(channel);
        } catch (IllegalStateException e) {
            closeRawChannel(channel.getRawChannel(), e);
            throw e;
        }
    }

    private void scheduleServerChannel(ServerSocket channel, NioSelector selector) {
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

    public static class RawChannelFactory {

        public RawChannelFactory() {
        }

        SocketChannel openNioChannel() throws IOException {
            SocketChannel socketChannel = SocketChannel.open();
            try {
                socketChannel.configureBlocking(false);
            } catch (IOException e) {
                closeRawChannel(socketChannel, e);
                throw e;
            }
            return socketChannel;
        }

        SocketChannel acceptNioChannel(SocketChannel socketChannel) throws IOException {
            try {
                socketChannel.configureBlocking(false);
                return socketChannel;
            } catch (IOException e) {
                closeRawChannel(socketChannel, e);
                throw e;
            }
        }

        ServerSocketChannel openNioServerSocketChannel() throws IOException {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            try {
                serverSocketChannel.configureBlocking(false);
            } catch (IOException e) {
                closeRawChannel(serverSocketChannel, e);
                throw e;
            }
            return serverSocketChannel;
        }

        public static SocketChannel accept(ServerSocketChannel serverSocketChannel) throws IOException {
            try {
                return AccessController.doPrivileged((PrivilegedExceptionAction<SocketChannel>) serverSocketChannel::accept);
            } catch (PrivilegedActionException e) {
                throw (IOException) e.getCause();
            }
        }
    }
}
