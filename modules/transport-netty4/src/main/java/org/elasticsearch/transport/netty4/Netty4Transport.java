/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.transport.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.util.AttributeKey;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.ThreadWatchdog;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.InboundAggregator;
import org.elasticsearch.transport.InboundDecoder;
import org.elasticsearch.transport.InboundPipeline;
import org.elasticsearch.transport.NetworkTraceFlag;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;

/**
 * There are 4 types of connections per node, low/med/high/ping. Low if for batch oriented APIs (like recovery or
 * batch) with high payload that will cause regular request. (like search or single index) to take
 * longer. Med is for the typical search / single doc index. And High for things like cluster state. Ping is reserved for
 * sending out ping requests to other nodes.
 */
public class Netty4Transport extends TcpTransport {
    private static final Logger logger = LogManager.getLogger(Netty4Transport.class);

    public static final ChannelOption<Integer> OPTION_TCP_KEEP_IDLE = NioChannelOption.of(NetUtils.getTcpKeepIdleSocketOption());
    public static final ChannelOption<Integer> OPTION_TCP_KEEP_INTERVAL = NioChannelOption.of(NetUtils.getTcpKeepIntervalSocketOption());
    public static final ChannelOption<Integer> OPTION_TCP_KEEP_COUNT = NioChannelOption.of(NetUtils.getTcpKeepCountSocketOption());

    private final SharedGroupFactory sharedGroupFactory;
    private final RecvByteBufAllocator recvByteBufAllocator;
    private final ByteSizeValue receivePredictorMin;
    private final ByteSizeValue receivePredictorMax;
    private final Map<String, ServerBootstrap> serverBootstraps = newConcurrentMap();
    private volatile Bootstrap clientBootstrap;
    private volatile SharedGroupFactory.SharedGroup sharedGroup;
    protected final boolean remoteClusterPortEnabled;

    private final ThreadWatchdog threadWatchdog;

    public Netty4Transport(
        Settings settings,
        TransportVersion version,
        ThreadPool threadPool,
        NetworkService networkService,
        PageCacheRecycler pageCacheRecycler,
        NamedWriteableRegistry namedWriteableRegistry,
        CircuitBreakerService circuitBreakerService,
        SharedGroupFactory sharedGroupFactory
    ) {
        super(settings, version, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService);
        Netty4Utils.setAvailableProcessors(EsExecutors.allocatedProcessors(settings));
        NettyAllocator.logAllocatorDescriptionIfNeeded();
        this.sharedGroupFactory = sharedGroupFactory;
        this.threadWatchdog = networkService.getThreadWatchdog();

        // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
        this.receivePredictorMin = Netty4Plugin.NETTY_RECEIVE_PREDICTOR_MIN.get(settings);
        this.receivePredictorMax = Netty4Plugin.NETTY_RECEIVE_PREDICTOR_MAX.get(settings);
        if (receivePredictorMax.getBytes() == receivePredictorMin.getBytes()) {
            recvByteBufAllocator = new FixedRecvByteBufAllocator((int) receivePredictorMax.getBytes());
        } else {
            recvByteBufAllocator = new AdaptiveRecvByteBufAllocator(
                (int) receivePredictorMin.getBytes(),
                (int) receivePredictorMin.getBytes(),
                (int) receivePredictorMax.getBytes()
            );
        }
        this.remoteClusterPortEnabled = REMOTE_CLUSTER_SERVER_ENABLED.get(settings);
    }

    @Override
    protected Recycler<BytesRef> createRecycler(Settings settings, PageCacheRecycler pageCacheRecycler) {
        return Netty4Utils.createRecycler(settings);
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            sharedGroup = sharedGroupFactory.getTransportGroup();
            clientBootstrap = createClientBootstrap(sharedGroup);
            if (NetworkService.NETWORK_SERVER.get(settings)) {
                for (ProfileSettings profileSettings : profileSettingsSet) {
                    createServerBootstrap(profileSettings, sharedGroup);
                    bindServer(profileSettings);
                }
            }
            threadWatchdog.run(settings, threadPool, lifecycle);
            success = true;
        } finally {
            if (success == false) {
                doStop();
            }
        }
    }

    private Bootstrap createClientBootstrap(SharedGroupFactory.SharedGroup sharedGroupForBootstrap) {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(sharedGroupForBootstrap.getLowLevelGroup());

        // NettyAllocator will return the channel type designed to work with the configured allocator
        assert Netty4NioSocketChannel.class.isAssignableFrom(NettyAllocator.getChannelType());
        bootstrap.channel(NettyAllocator.getChannelType());
        bootstrap.option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator());

        // The TCP options are re-configured for client connections to RCS remote clusters
        // If how options are configured is changed here, please also update RemoteClusterClientBootstrapOptions#configure
        // which is used inside SecurityNetty4Transport#getClientBootstrap
        bootstrap.option(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));
        if (TransportSettings.TCP_KEEP_ALIVE.get(settings)) {
            // Note that Netty logs a warning if it can't set the option
            if (TransportSettings.TCP_KEEP_IDLE.get(settings) >= 0) {
                bootstrap.option(OPTION_TCP_KEEP_IDLE, TransportSettings.TCP_KEEP_IDLE.get(settings));
            }
            if (TransportSettings.TCP_KEEP_INTERVAL.get(settings) >= 0) {
                bootstrap.option(OPTION_TCP_KEEP_INTERVAL, TransportSettings.TCP_KEEP_INTERVAL.get(settings));
            }
            if (TransportSettings.TCP_KEEP_COUNT.get(settings) >= 0) {
                bootstrap.option(OPTION_TCP_KEEP_COUNT, TransportSettings.TCP_KEEP_COUNT.get(settings));
            }
        }

        final ByteSizeValue tcpSendBufferSize = TransportSettings.TCP_SEND_BUFFER_SIZE.get(settings);
        if (tcpSendBufferSize.getBytes() > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, Math.toIntExact(tcpSendBufferSize.getBytes()));
        }

        final ByteSizeValue tcpReceiveBufferSize = TransportSettings.TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.getBytes() > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, Math.toIntExact(tcpReceiveBufferSize.getBytes()));
        }

        bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);

        final boolean reuseAddress = TransportSettings.TCP_REUSE_ADDRESS.get(settings);
        bootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);

        return bootstrap;
    }

    private void createServerBootstrap(ProfileSettings profileSettings, SharedGroupFactory.SharedGroup sharedGroupForServerBootstrap) {
        String name = profileSettings.profileName;
        if (logger.isDebugEnabled()) {
            logger.debug(
                "using profile[{}], worker_count[{}], port[{}], bind_host[{}], publish_host[{}], receive_predictor[{}->{}]",
                name,
                sharedGroupFactory.getTransportWorkerCount(),
                profileSettings.portOrRange,
                profileSettings.bindHosts,
                profileSettings.publishHosts,
                receivePredictorMin,
                receivePredictorMax
            );
        }

        final ServerBootstrap serverBootstrap = new ServerBootstrap();

        serverBootstrap.group(sharedGroupForServerBootstrap.getLowLevelGroup());

        // NettyAllocator will return the channel type designed to work with the configuredAllocator
        serverBootstrap.channel(NettyAllocator.getServerChannelType());

        // Set the allocators for both the server channel and the child channels created
        serverBootstrap.option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator());
        serverBootstrap.childOption(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator());

        serverBootstrap.childHandler(getServerChannelInitializer(name));
        serverBootstrap.handler(new ServerChannelExceptionHandler());

        serverBootstrap.childOption(ChannelOption.TCP_NODELAY, profileSettings.tcpNoDelay);
        serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, profileSettings.tcpKeepAlive);
        if (profileSettings.tcpKeepAlive) {
            // Note that Netty logs a warning if it can't set the option
            if (profileSettings.tcpKeepIdle >= 0) {
                serverBootstrap.childOption(NioChannelOption.of(NetUtils.getTcpKeepIdleSocketOption()), profileSettings.tcpKeepIdle);
            }
            if (profileSettings.tcpKeepInterval >= 0) {
                serverBootstrap.childOption(
                    NioChannelOption.of(NetUtils.getTcpKeepIntervalSocketOption()),
                    profileSettings.tcpKeepInterval
                );
            }
            if (profileSettings.tcpKeepCount >= 0) {
                serverBootstrap.childOption(NioChannelOption.of(NetUtils.getTcpKeepCountSocketOption()), profileSettings.tcpKeepCount);
            }
        }

        if (profileSettings.sendBufferSize.getBytes() != -1) {
            serverBootstrap.childOption(ChannelOption.SO_SNDBUF, Math.toIntExact(profileSettings.sendBufferSize.getBytes()));
        }

        if (profileSettings.receiveBufferSize.getBytes() != -1) {
            serverBootstrap.childOption(ChannelOption.SO_RCVBUF, Math.toIntExact(profileSettings.receiveBufferSize.bytesAsInt()));
        }

        serverBootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);
        serverBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);

        serverBootstrap.option(ChannelOption.SO_REUSEADDR, profileSettings.reuseAddress);
        serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, profileSettings.reuseAddress);
        serverBootstrap.validate();

        serverBootstraps.put(name, serverBootstrap);
    }

    protected ChannelHandler getServerChannelInitializer(String name) {
        return new ServerChannelInitializer(name);
    }

    protected ChannelHandler getClientChannelInitializer(DiscoveryNode node, ConnectionProfile connectionProfile) {
        return new ClientChannelInitializer();
    }

    static final AttributeKey<Netty4TcpChannel> CHANNEL_KEY = AttributeKey.newInstance("es-channel");
    static final AttributeKey<Netty4TcpServerChannel> SERVER_CHANNEL_KEY = AttributeKey.newInstance("es-server-channel");

    @Override
    protected Netty4TcpChannel initiateChannel(DiscoveryNode node, ConnectionProfile connectionProfile) throws IOException {
        InetSocketAddress address = node.getAddress().address();
        Bootstrap bootstrapWithHandler = getClientBootstrap(connectionProfile);
        bootstrapWithHandler.handler(getClientChannelInitializer(node, connectionProfile));
        bootstrapWithHandler.remoteAddress(address);
        ChannelFuture connectFuture = bootstrapWithHandler.connect();

        Channel channel = connectFuture.channel();
        if (channel == null) {
            ExceptionsHelper.maybeDieOnAnotherThread(connectFuture.cause());
            throw new IOException(connectFuture.cause());
        }

        Netty4TcpChannel nettyChannel = new Netty4TcpChannel(
            channel,
            false,
            connectionProfile.getTransportProfile(),
            rstOnClose,
            connectFuture
        );
        channel.attr(CHANNEL_KEY).set(nettyChannel);

        return nettyChannel;
    }

    protected Bootstrap getClientBootstrap(ConnectionProfile connectionProfile) {
        return clientBootstrap.clone();
    }

    @Override
    protected Netty4TcpServerChannel bind(String name, InetSocketAddress address) {
        Channel channel = serverBootstraps.get(name).bind(address).syncUninterruptibly().channel();
        Netty4TcpServerChannel esChannel = new Netty4TcpServerChannel(channel);
        channel.attr(SERVER_CHANNEL_KEY).set(esChannel);
        return esChannel;
    }

    @Override
    @SuppressForbidden(reason = "debug")
    protected void stopInternal() {
        Releasables.close(() -> {
            if (sharedGroup != null) {
                sharedGroup.shutdown();
            }
        }, serverBootstraps::clear, () -> clientBootstrap = null);
    }

    static Exception exceptionFromThrowable(Throwable cause) {
        if (cause instanceof Error) {
            return new Exception(cause);
        } else {
            return (Exception) cause;
        }
    }

    protected class ClientChannelInitializer extends ChannelInitializer<Channel> {

        @Override
        protected void initChannel(Channel ch) throws Exception {
            assert ch instanceof Netty4NioSocketChannel;
            NetUtils.tryEnsureReasonableKeepAliveConfig(((Netty4NioSocketChannel) ch).javaChannel());
            setupPipeline(ch, false);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            Netty4TcpChannel channel = ctx.channel().attr(CHANNEL_KEY).get();
            channel.setCloseException(exceptionFromThrowable(cause));
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            super.exceptionCaught(ctx, cause);
        }
    }

    protected class ServerChannelInitializer extends ChannelInitializer<Channel> {

        protected final String name;
        private final boolean isRemoteClusterServerChannel;

        protected ServerChannelInitializer(String name) {
            this.name = name;
            this.isRemoteClusterServerChannel = remoteClusterPortEnabled && REMOTE_CLUSTER_PROFILE.equals(name);
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            assert ch instanceof Netty4NioSocketChannel;
            NetUtils.tryEnsureReasonableKeepAliveConfig(((Netty4NioSocketChannel) ch).javaChannel());
            Netty4TcpChannel nettyTcpChannel = new Netty4TcpChannel(ch, true, name, rstOnClose, ch.newSucceededFuture());
            ch.attr(CHANNEL_KEY).set(nettyTcpChannel);
            setupPipeline(ch, isRemoteClusterServerChannel);
            serverAcceptedChannel(nettyTcpChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            Netty4TcpChannel channel = ctx.channel().attr(CHANNEL_KEY).get();
            channel.setCloseException(exceptionFromThrowable(cause));
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            super.exceptionCaught(ctx, cause);
        }
    }

    private void setupPipeline(Channel ch, boolean isRemoteClusterServerChannel) {
        final var pipeline = ch.pipeline();
        pipeline.addLast("byte_buf_sizer", NettyByteBufSizer.INSTANCE);
        if (NetworkTraceFlag.TRACE_ENABLED) {
            pipeline.addLast("logging", ESLoggingHandler.INSTANCE);
        }
        pipeline.addLast(
            "chunked_writer",
            new Netty4WriteThrottlingHandler(getThreadPool().getThreadContext(), threadWatchdog.getActivityTrackerForCurrentThread())
        );
        pipeline.addLast(
            "dispatcher",
            new Netty4MessageInboundHandler(
                this,
                getInboundPipeline(ch, isRemoteClusterServerChannel),
                threadWatchdog.getActivityTrackerForCurrentThread()
            )
        );
    }

    protected InboundPipeline getInboundPipeline(Channel ch, boolean isRemoteClusterServerChannel) {
        return new InboundPipeline(
            getStatsTracker(),
            threadPool.relativeTimeInMillisSupplier(),
            new InboundDecoder(recycler),
            new InboundAggregator(getInflightBreaker(), getRequestHandlers()::getHandler, ignoreDeserializationErrors()),
            this::inboundMessage
        );
    }

    @ChannelHandler.Sharable
    private static class ServerChannelExceptionHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            Netty4TcpServerChannel serverChannel = ctx.channel().attr(SERVER_CHANNEL_KEY).get();
            onServerException(serverChannel, exceptionFromThrowable(cause));
        }
    }
}
