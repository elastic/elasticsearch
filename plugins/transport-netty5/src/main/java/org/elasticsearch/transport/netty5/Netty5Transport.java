/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport.netty5;

import io.netty5.bootstrap.Bootstrap;
import io.netty5.bootstrap.ServerBootstrap;
import io.netty5.channel.AdaptiveRecvBufferAllocator;
import io.netty5.channel.Channel;
import io.netty5.channel.ChannelHandler;
import io.netty5.channel.ChannelHandlerAdapter;
import io.netty5.channel.ChannelHandlerContext;
import io.netty5.channel.ChannelInitializer;
import io.netty5.channel.ChannelOption;
import io.netty5.channel.FixedRecvBufferAllocator;
import io.netty5.channel.RecvBufferAllocator;
import io.netty5.channel.socket.nio.NioChannelOption;
import io.netty5.util.AttributeKey;
import io.netty5.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.net.NetUtils;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportSettings;

import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.util.Map;

import static org.elasticsearch.common.settings.Setting.byteSizeSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * There are 4 types of connections per node, low/med/high/ping. Low if for batch oriented APIs (like recovery or
 * batch) with high payload that will cause regular request. (like search or single index) to take
 * longer. Med is for the typical search / single doc index. And High for things like cluster state. Ping is reserved for
 * sending out ping requests to other nodes.
 */
public class Netty5Transport extends TcpTransport {
    private static final Logger logger = LogManager.getLogger(Netty5Transport.class);

    public static final Setting<Integer> WORKER_COUNT = new Setting<>(
        "transport.netty5.worker_count",
        (s) -> Integer.toString(EsExecutors.allocatedProcessors(s)),
        (s) -> Setting.parseInt(s, 1, "transport.netty5.worker_count"),
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_SIZE = Setting.byteSizeSetting(
        "transport.netty5.receive_predictor_size",
        new ByteSizeValue(64, ByteSizeUnit.KB),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MIN = byteSizeSetting(
        "transport.netty5.receive_predictor_min",
        NETTY_RECEIVE_PREDICTOR_SIZE,
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MAX = byteSizeSetting(
        "transport.netty5.receive_predictor_max",
        NETTY_RECEIVE_PREDICTOR_SIZE,
        Property.NodeScope
    );

    public static final Setting<Integer> NETTY_BOSS_COUNT = intSetting("transport.netty5.boss_count", 1, 1, Property.NodeScope);

    private final SharedGroupFactory sharedGroupFactory;
    private final RecvBufferAllocator recvByteBufAllocator;
    private final ByteSizeValue receivePredictorMin;
    private final ByteSizeValue receivePredictorMax;
    private final Map<String, ServerBootstrap> serverBootstraps = newConcurrentMap();
    private volatile Bootstrap clientBootstrap;
    private volatile SharedGroupFactory.SharedGroup sharedGroup;

    public Netty5Transport(
        Settings settings,
        Version version,
        ThreadPool threadPool,
        NetworkService networkService,
        PageCacheRecycler pageCacheRecycler,
        NamedWriteableRegistry namedWriteableRegistry,
        CircuitBreakerService circuitBreakerService,
        SharedGroupFactory sharedGroupFactory
    ) {
        super(settings, version, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService);
        Netty5Utils.setAvailableProcessors(EsExecutors.NODE_PROCESSORS_SETTING.get(settings));
        NettyAllocator.logAllocatorDescriptionIfNeeded();
        this.sharedGroupFactory = sharedGroupFactory;

        // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
        this.receivePredictorMin = NETTY_RECEIVE_PREDICTOR_MIN.get(settings);
        this.receivePredictorMax = NETTY_RECEIVE_PREDICTOR_MAX.get(settings);
        if (receivePredictorMax.getBytes() == receivePredictorMin.getBytes()) {
            recvByteBufAllocator = new FixedRecvBufferAllocator((int) receivePredictorMax.getBytes());
        } else {
            recvByteBufAllocator = new AdaptiveRecvBufferAllocator(
                (int) receivePredictorMin.getBytes(),
                (int) receivePredictorMin.getBytes(),
                (int) receivePredictorMax.getBytes()
            );
        }
    }

    @Override
    protected Recycler<BytesRef> createRecycler(Settings settings, PageCacheRecycler pageCacheRecycler) {
        // If this method is called by super ctor the processors will not be set. Accessing NettyAllocator initializes netty's internals
        // setting the processors. We must do it ourselves first just in case.
        Netty5Utils.setAvailableProcessors(EsExecutors.NODE_PROCESSORS_SETTING.get(settings));
        return NettyAllocator.getRecycler();
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
        assert Netty5NioSocketChannel.class.isAssignableFrom(NettyAllocator.getChannelType());
        bootstrap.channel(NettyAllocator.getChannelType());
        bootstrap.option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator());

        bootstrap.option(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));
        if (TransportSettings.TCP_KEEP_ALIVE.get(settings)) {
            // Note that Netty logs a warning if it can't set the option
            if (TransportSettings.TCP_KEEP_IDLE.get(settings) >= 0) {
                final SocketOption<Integer> keepIdleOption = NetUtils.getTcpKeepIdleSocketOptionOrNull();
                if (keepIdleOption != null) {
                    bootstrap.option(NioChannelOption.of(keepIdleOption), TransportSettings.TCP_KEEP_IDLE.get(settings));
                }
            }
            if (TransportSettings.TCP_KEEP_INTERVAL.get(settings) >= 0) {
                final SocketOption<Integer> keepIntervalOption = NetUtils.getTcpKeepIntervalSocketOptionOrNull();
                if (keepIntervalOption != null) {
                    bootstrap.option(NioChannelOption.of(keepIntervalOption), TransportSettings.TCP_KEEP_INTERVAL.get(settings));
                }
            }
            if (TransportSettings.TCP_KEEP_COUNT.get(settings) >= 0) {
                final SocketOption<Integer> keepCountOption = NetUtils.getTcpKeepCountSocketOptionOrNull();
                if (keepCountOption != null) {
                    bootstrap.option(NioChannelOption.of(keepCountOption), TransportSettings.TCP_KEEP_COUNT.get(settings));
                }
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
                final SocketOption<Integer> keepIdleOption = NetUtils.getTcpKeepIdleSocketOptionOrNull();
                if (keepIdleOption != null) {
                    serverBootstrap.childOption(NioChannelOption.of(keepIdleOption), profileSettings.tcpKeepIdle);
                }
            }
            if (profileSettings.tcpKeepInterval >= 0) {
                final SocketOption<Integer> keepIntervalOption = NetUtils.getTcpKeepIntervalSocketOptionOrNull();
                if (keepIntervalOption != null) {
                    serverBootstrap.childOption(NioChannelOption.of(keepIntervalOption), profileSettings.tcpKeepInterval);
                }

            }
            if (profileSettings.tcpKeepCount >= 0) {
                final SocketOption<Integer> keepCountOption = NetUtils.getTcpKeepCountSocketOptionOrNull();
                if (keepCountOption != null) {
                    serverBootstrap.childOption(NioChannelOption.of(keepCountOption), profileSettings.tcpKeepCount);
                }
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

    protected ChannelHandler getClientChannelInitializer(DiscoveryNode node) {
        return new ClientChannelInitializer();
    }

    static final AttributeKey<Netty5TcpChannel> CHANNEL_KEY = AttributeKey.newInstance("es-channel");
    static final AttributeKey<Netty5TcpServerChannel> SERVER_CHANNEL_KEY = AttributeKey.newInstance("es-server-channel");

    @Override
    protected Netty5TcpChannel initiateChannel(DiscoveryNode node) {
        InetSocketAddress address = node.getAddress().address();
        Bootstrap bootstrapWithHandler = clientBootstrap.clone();
        bootstrapWithHandler.handler(getClientChannelInitializer(node));
        bootstrapWithHandler.remoteAddress(address);
        Future<Channel> connectFuture = bootstrapWithHandler.connect();

        return new Netty5TcpChannel(false, "default", rstOnClose, connectFuture);
    }

    @Override
    protected Netty5TcpServerChannel bind(String name, InetSocketAddress address) {
        Channel channel = serverBootstraps.get(name).bind(address).syncUninterruptibly().getNow();
        Netty5TcpServerChannel esChannel = new Netty5TcpServerChannel(channel);
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

    protected class ClientChannelInitializer extends ChannelInitializer<Channel> {

        @Override
        protected void initChannel(Channel ch) {
            addClosedExceptionLogger(ch);
            assert ch instanceof Netty5NioSocketChannel;
            NetUtils.tryEnsureReasonableKeepAliveConfig(((Netty5NioSocketChannel) ch).javaChannel());
            ch.pipeline().addLast("byte_buf_sizer", NettyByteBufSizer.INSTANCE);
            ch.pipeline().addLast("logging", ESLoggingHandler.INSTANCE);
            // using a dot as a prefix means this cannot come from any settings parsed
            ch.pipeline().addLast("dispatcher", new Netty5MessageChannelHandler(Netty5Transport.this, recycler));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            super.exceptionCaught(ctx, cause);
        }
    }

    protected class ServerChannelInitializer extends ChannelInitializer<Channel> {

        protected final String name;

        protected ServerChannelInitializer(String name) {
            this.name = name;
        }

        @Override
        protected void initChannel(Channel ch) {
            addClosedExceptionLogger(ch);
            assert ch instanceof Netty5NioSocketChannel;
            NetUtils.tryEnsureReasonableKeepAliveConfig(((Netty5NioSocketChannel) ch).javaChannel());
            Netty5TcpChannel nettyTcpChannel = new Netty5TcpChannel(true, name, rstOnClose, ch.executor().newSucceededFuture(ch));
            ch.attr(CHANNEL_KEY).set(nettyTcpChannel);
            ch.pipeline().addLast("byte_buf_sizer", NettyByteBufSizer.INSTANCE);
            ch.pipeline().addLast("logging", ESLoggingHandler.INSTANCE);
            ch.pipeline().addLast("dispatcher", new Netty5MessageChannelHandler(Netty5Transport.this, recycler));
            serverAcceptedChannel(nettyTcpChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            super.exceptionCaught(ctx, cause);
        }
    }

    private void addClosedExceptionLogger(Channel channel) {
        channel.closeFuture().addListener(f -> {
            if (f.isSuccess() == false) {
                logger.debug(() -> new ParameterizedMessage("exception while closing channel: {}", channel), f.cause());
            }
        });
    }

    @ChannelHandler.Sharable
    private class ServerChannelExceptionHandler extends ChannelHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            Netty5TcpServerChannel serverChannel = ctx.channel().attr(SERVER_CHANNEL_KEY).get();
            if (cause instanceof Error) {
                onServerException(serverChannel, new Exception(cause));
            } else {
                onServerException(serverChannel, (Exception) cause);
            }
        }
    }
}
