/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.function.Supplier;

public abstract class ChannelFactory<ServerSocket extends NioServerSocketChannel, Socket extends NioSocketChannel> {

    private final boolean tcpNoDelay;
    private final boolean tcpKeepAlive;
    private final int tcpKeepIdle;
    private final int tcpKeepInterval;
    private final int tcpKeepCount;
    private final boolean tcpReuseAddress;
    private final int tcpSendBufferSize;
    private final int tcpReceiveBufferSize;
    private final ChannelFactory.RawChannelFactory rawChannelFactory;

    /**
     * This will create a {@link ChannelFactory}.
     */
    protected ChannelFactory(
        boolean tcpNoDelay,
        boolean tcpKeepAlive,
        int tcpKeepIdle,
        int tcpKeepInterval,
        int tcpKeepCount,
        boolean tcpReuseAddress,
        int tcpSendBufferSize,
        int tcpReceiveBufferSize
    ) {
        this(
            tcpNoDelay,
            tcpKeepAlive,
            tcpKeepIdle,
            tcpKeepInterval,
            tcpKeepCount,
            tcpReuseAddress,
            tcpSendBufferSize,
            tcpReceiveBufferSize,
            new RawChannelFactory()
        );
    }

    /**
     * This will create a {@link ChannelFactory} using the raw channel factory passed to the constructor.
     */
    protected ChannelFactory(
        boolean tcpNoDelay,
        boolean tcpKeepAlive,
        int tcpKeepIdle,
        int tcpKeepInterval,
        int tcpKeepCount,
        boolean tcpReuseAddress,
        int tcpSendBufferSize,
        int tcpReceiveBufferSize,
        RawChannelFactory rawChannelFactory
    ) {
        this.tcpNoDelay = tcpNoDelay;
        this.tcpKeepAlive = tcpKeepAlive;
        this.tcpKeepIdle = tcpKeepIdle;
        this.tcpKeepInterval = tcpKeepInterval;
        this.tcpKeepCount = tcpKeepCount;
        this.tcpReuseAddress = tcpReuseAddress;
        this.tcpSendBufferSize = tcpSendBufferSize;
        this.tcpReceiveBufferSize = tcpReceiveBufferSize;
        this.rawChannelFactory = rawChannelFactory;
    }

    public Socket openNioChannel(InetSocketAddress remoteAddress, Supplier<NioSelector> supplier) throws IOException {
        SocketChannel rawChannel = rawChannelFactory.openNioChannel();
        setNonBlocking(rawChannel);
        NioSelector selector = supplier.get();
        Socket channel = internalCreateChannel(selector, rawChannel, createSocketConfig(remoteAddress, false));
        scheduleChannel(channel, selector);
        return channel;
    }

    public Socket acceptNioChannel(SocketChannel rawChannel, Supplier<NioSelector> supplier) throws IOException {
        setNonBlocking(rawChannel);
        NioSelector selector = supplier.get();
        InetSocketAddress remoteAddress = getRemoteAddress(rawChannel);
        Socket channel = internalCreateChannel(selector, rawChannel, createSocketConfig(remoteAddress, true));
        scheduleChannel(channel, selector);
        return channel;
    }

    public ServerSocket openNioServerSocketChannel(InetSocketAddress localAddress, Supplier<NioSelector> supplier) throws IOException {
        ServerSocketChannel rawChannel = rawChannelFactory.openNioServerSocketChannel();
        setNonBlocking(rawChannel);
        NioSelector selector = supplier.get();
        Config.ServerSocket config = new Config.ServerSocket(tcpReuseAddress, localAddress);
        ServerSocket serverChannel = internalCreateServerChannel(selector, rawChannel, config);
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

    protected InetSocketAddress getRemoteAddress(SocketChannel rawChannel) throws IOException {
        InetSocketAddress remoteAddress = (InetSocketAddress) rawChannel.socket().getRemoteSocketAddress();
        if (remoteAddress == null) {
            throw new IOException("Accepted socket does not have remote address");
        }
        return remoteAddress;
    }

    private Socket internalCreateChannel(NioSelector selector, SocketChannel rawChannel, Config.Socket config) throws IOException {
        try {
            Socket channel = createChannel(selector, rawChannel, config);
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

    private ServerSocket internalCreateServerChannel(NioSelector selector, ServerSocketChannel rawChannel, Config.ServerSocket config)
        throws IOException {
        try {
            return createServerChannel(selector, rawChannel, config);
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

    private void setNonBlocking(AbstractSelectableChannel rawChannel) throws IOException {
        try {
            rawChannel.configureBlocking(false);
        } catch (IOException e) {
            closeRawChannel(rawChannel, e);
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

    private Config.Socket createSocketConfig(InetSocketAddress remoteAddress, boolean isAccepted) {
        return new Config.Socket(
            tcpNoDelay,
            tcpKeepAlive,
            tcpKeepIdle,
            tcpKeepInterval,
            tcpKeepCount,
            tcpReuseAddress,
            tcpSendBufferSize,
            tcpReceiveBufferSize,
            remoteAddress,
            isAccepted
        );
    }

    public static class RawChannelFactory {

        SocketChannel openNioChannel() throws IOException {
            return SocketChannel.open();
        }

        ServerSocketChannel openNioServerSocketChannel() throws IOException {
            return ServerSocketChannel.open();
        }
    }
}
