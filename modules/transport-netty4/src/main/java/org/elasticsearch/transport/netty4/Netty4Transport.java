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

package org.elasticsearch.transport.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.oio.OioServerSocketChannel;
import io.netty.channel.socket.oio.OioSocketChannel;
import io.netty.util.concurrent.Future;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkService.TcpSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportServiceAdapter;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.Setting.byteSizeSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * There are 4 types of connections per node, low/med/high/ping. Low if for batch oriented APIs (like recovery or
 * batch) with high payload that will cause regular request. (like search or single index) to take
 * longer. Med is for the typical search / single doc index. And High for things like cluster state. Ping is reserved for
 * sending out ping requests to other nodes.
 */
public class Netty4Transport extends TcpTransport<Channel> {

    static {
        Netty4Utils.setup();
    }

    public static final Setting<Integer> WORKER_COUNT =
        new Setting<>("transport.netty.worker_count",
            (s) -> Integer.toString(EsExecutors.boundedNumberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "transport.netty.worker_count"), Property.NodeScope, Property.Shared);

    public static final Setting<ByteSizeValue> NETTY_MAX_CUMULATION_BUFFER_CAPACITY =
        Setting.byteSizeSetting(
                "transport.netty.max_cumulation_buffer_capacity",
                new ByteSizeValue(-1),
                Property.NodeScope,
                Property.Shared);
    public static final Setting<Integer> NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS =
        Setting.intSetting("transport.netty.max_composite_buffer_components", -1, -1, Property.NodeScope, Property.Shared);

    // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_SIZE = Setting.byteSizeSetting(
            "transport.netty.receive_predictor_size",
            settings -> {
                long defaultReceiverPredictor = 512 * 1024;
                if (JvmInfo.jvmInfo().getMem().getDirectMemoryMax().bytes() > 0) {
                    // we can guess a better default...
                    long l = (long) ((0.3 * JvmInfo.jvmInfo().getMem().getDirectMemoryMax().bytes()) / WORKER_COUNT.get(settings));
                    defaultReceiverPredictor = Math.min(defaultReceiverPredictor, Math.max(l, 64 * 1024));
                }
                return new ByteSizeValue(defaultReceiverPredictor).toString();
            },
            Property.NodeScope,
            Property.Shared);
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MIN =
        byteSizeSetting("transport.netty.receive_predictor_min", NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope, Property.Shared);
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MAX =
        byteSizeSetting("transport.netty.receive_predictor_max", NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope, Property.Shared);
    public static final Setting<Integer> NETTY_BOSS_COUNT =
        intSetting("transport.netty.boss_count", 1, 1, Property.NodeScope, Property.Shared);


    protected final ByteSizeValue maxCumulationBufferCapacity;
    protected final int maxCompositeBufferComponents;
    protected final RecvByteBufAllocator recvByteBufAllocator;
    protected final int workerCount;
    protected final ByteSizeValue receivePredictorMin;
    protected final ByteSizeValue receivePredictorMax;
    // package private for testing
    volatile Netty4OpenChannelsHandler serverOpenChannels;
    protected volatile Bootstrap bootstrap;
    protected final Map<String, ServerBootstrap> serverBootstraps = newConcurrentMap();

    @Inject
    public Netty4Transport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                          NamedWriteableRegistry namedWriteableRegistry, CircuitBreakerService circuitBreakerService) {
        super("netty", settings, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, networkService);
        this.workerCount = WORKER_COUNT.get(settings);
        this.maxCumulationBufferCapacity = NETTY_MAX_CUMULATION_BUFFER_CAPACITY.get(settings);
        this.maxCompositeBufferComponents = NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS.get(settings);

        // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
        this.receivePredictorMin = NETTY_RECEIVE_PREDICTOR_MIN.get(settings);
        this.receivePredictorMax = NETTY_RECEIVE_PREDICTOR_MAX.get(settings);
        if (receivePredictorMax.bytes() == receivePredictorMin.bytes()) {
            recvByteBufAllocator = new FixedRecvByteBufAllocator((int) receivePredictorMax.bytes());
        } else {
            recvByteBufAllocator = new AdaptiveRecvByteBufAllocator((int) receivePredictorMin.bytes(),
                (int) receivePredictorMin.bytes(), (int) receivePredictorMax.bytes());
        }
    }

    TransportServiceAdapter transportServiceAdapter() {
        return transportServiceAdapter;
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            bootstrap = createBootstrap();
            if (NetworkService.NETWORK_SERVER.get(settings)) {
                final Netty4OpenChannelsHandler openChannels = new Netty4OpenChannelsHandler(logger);
                this.serverOpenChannels = openChannels;
                // loop through all profiles and start them up, special handling for default one
                for (Map.Entry<String, Settings> entry : buildProfileSettings().entrySet()) {
                    // merge fallback settings with default settings with profile settings so we have complete settings with default values
                    final Settings settings = Settings.builder()
                        .put(createFallbackSettings())
                        .put(entry.getValue()).build();
                    createServerBootstrap(entry.getKey(), settings);
                    bindServer(entry.getKey(), settings);
                }
            }
            super.doStart();
            success = true;
        } finally {
            if (success == false) {
                doStop();
            }
        }
    }

    private Bootstrap createBootstrap() {
        final Bootstrap bootstrap = new Bootstrap();
        if (TCP_BLOCKING_CLIENT.get(settings)) {
            bootstrap.group(new OioEventLoopGroup(1, daemonThreadFactory(settings, TRANSPORT_CLIENT_WORKER_THREAD_NAME_PREFIX)));
            bootstrap.channel(OioSocketChannel.class);
        } else {
            bootstrap.group(new NioEventLoopGroup(workerCount, daemonThreadFactory(settings, TRANSPORT_CLIENT_BOSS_THREAD_NAME_PREFIX)));
            bootstrap.channel(NioSocketChannel.class);
        }

        bootstrap.handler(getClientChannelInitializer());

        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(connectTimeout.millis()));
        bootstrap.option(ChannelOption.TCP_NODELAY, TCP_NO_DELAY.get(settings));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, TCP_KEEP_ALIVE.get(settings));

        final ByteSizeValue tcpSendBufferSize = TCP_SEND_BUFFER_SIZE.get(settings);
        if (tcpSendBufferSize.bytes() > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, Math.toIntExact(tcpSendBufferSize.bytes()));
        }

        final ByteSizeValue tcpReceiveBufferSize = TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.bytes() > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, Math.toIntExact(tcpReceiveBufferSize.bytes()));
        }

        bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);

        final boolean reuseAddress = TCP_REUSE_ADDRESS.get(settings);
        bootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);

        bootstrap.validate();

        return bootstrap;
    }

    private Settings createFallbackSettings() {
        Settings.Builder fallbackSettingsBuilder = Settings.builder();

        List<String> fallbackBindHost = TransportSettings.BIND_HOST.get(settings);
        if (fallbackBindHost.isEmpty() == false) {
            fallbackSettingsBuilder.putArray("bind_host", fallbackBindHost);
        }

        List<String> fallbackPublishHost = TransportSettings.PUBLISH_HOST.get(settings);
        if (fallbackPublishHost.isEmpty() == false) {
            fallbackSettingsBuilder.putArray("publish_host", fallbackPublishHost);
        }

        boolean fallbackTcpNoDelay = settings.getAsBoolean("transport.netty.tcp_no_delay", TcpSettings.TCP_NO_DELAY.get(settings));
        fallbackSettingsBuilder.put("tcp_no_delay", fallbackTcpNoDelay);

        boolean fallbackTcpKeepAlive = settings.getAsBoolean("transport.netty.tcp_keep_alive", TcpSettings.TCP_KEEP_ALIVE.get(settings));
        fallbackSettingsBuilder.put("tcp_keep_alive", fallbackTcpKeepAlive);

        boolean fallbackReuseAddress = settings.getAsBoolean("transport.netty.reuse_address", TcpSettings.TCP_REUSE_ADDRESS.get(settings));
        fallbackSettingsBuilder.put("reuse_address", fallbackReuseAddress);

        ByteSizeValue fallbackTcpSendBufferSize = settings.getAsBytesSize("transport.netty.tcp_send_buffer_size",
            TCP_SEND_BUFFER_SIZE.get(settings));
        if (fallbackTcpSendBufferSize.bytes() >= 0) {
            fallbackSettingsBuilder.put("tcp_send_buffer_size", fallbackTcpSendBufferSize);
        }

        ByteSizeValue fallbackTcpBufferSize = settings.getAsBytesSize("transport.netty.tcp_receive_buffer_size",
            TCP_RECEIVE_BUFFER_SIZE.get(settings));
        if (fallbackTcpBufferSize.bytes() >= 0) {
            fallbackSettingsBuilder.put("tcp_receive_buffer_size", fallbackTcpBufferSize);
        }

        return fallbackSettingsBuilder.build();
    }

    private void createServerBootstrap(String name, Settings settings) {
        if (logger.isDebugEnabled()) {
            logger.debug("using profile[{}], worker_count[{}], port[{}], bind_host[{}], publish_host[{}], compress[{}], "
                    + "connect_timeout[{}], connections_per_node[{}/{}/{}/{}/{}], receive_predictor[{}->{}]",
                name, workerCount, settings.get("port"), settings.get("bind_host"), settings.get("publish_host"), compress,
                connectTimeout, connectionsPerNodeRecovery, connectionsPerNodeBulk, connectionsPerNodeReg, connectionsPerNodeState,
                connectionsPerNodePing, receivePredictorMin, receivePredictorMax);
        }

        final ThreadFactory workerFactory = daemonThreadFactory(this.settings, HTTP_SERVER_WORKER_THREAD_NAME_PREFIX, name);

        final ServerBootstrap serverBootstrap = new ServerBootstrap();

        if (TCP_BLOCKING_SERVER.get(settings)) {
            serverBootstrap.group(new OioEventLoopGroup(workerCount, workerFactory));
            serverBootstrap.channel(OioServerSocketChannel.class);
        } else {
            serverBootstrap.group(new NioEventLoopGroup(workerCount, workerFactory));
            serverBootstrap.channel(NioServerSocketChannel.class);
        }

        serverBootstrap.childHandler(getServerChannelInitializer(name, settings));

        serverBootstrap.childOption(ChannelOption.TCP_NODELAY, TCP_NO_DELAY.get(settings));
        serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, TCP_KEEP_ALIVE.get(settings));

        final ByteSizeValue tcpSendBufferSize = TCP_SEND_BUFFER_SIZE.getDefault(settings);
        if (tcpSendBufferSize != null && tcpSendBufferSize.bytes() > 0) {
            serverBootstrap.childOption(ChannelOption.SO_SNDBUF, Math.toIntExact(tcpSendBufferSize.bytes()));
        }

        final ByteSizeValue tcpReceiveBufferSize = TCP_RECEIVE_BUFFER_SIZE.getDefault(settings);
        if (tcpReceiveBufferSize != null && tcpReceiveBufferSize.bytes() > 0) {
            serverBootstrap.childOption(ChannelOption.SO_RCVBUF, Math.toIntExact(tcpReceiveBufferSize.bytesAsInt()));
        }

        serverBootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);
        serverBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);

        final boolean reuseAddress = TCP_REUSE_ADDRESS.get(settings);
        serverBootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);
        serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, reuseAddress);

        serverBootstrap.validate();

        serverBootstraps.put(name, serverBootstrap);
    }

    protected ChannelHandler getServerChannelInitializer(String name, Settings settings) {
        return new ServerChannelInitializer(name, settings);
    }

    protected ChannelHandler getClientChannelInitializer() {
        return new ClientChannelInitializer();
    }

    protected final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        final Throwable unwrapped = ExceptionsHelper.unwrap(cause, ElasticsearchException.class);
        final Throwable t = unwrapped != null ? unwrapped : cause;
        onException(ctx.channel(), t instanceof Exception ? (Exception) t : new ElasticsearchException(t));
    }

    @Override
    public long serverOpen() {
        Netty4OpenChannelsHandler channels = serverOpenChannels;
        return channels == null ? 0 : channels.numberOfOpenChannels();
    }

    protected NodeChannels connectToChannelsLight(DiscoveryNode node) {
        InetSocketAddress address = ((InetSocketTransportAddress) node.getAddress()).address();
        ChannelFuture connect = bootstrap.connect(address);
        connect.awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
        if (!connect.isSuccess()) {
            throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", connect.cause());
        }
        Channel[] channels = new Channel[1];
        channels[0] = connect.channel();
        channels[0].closeFuture().addListener(new ChannelCloseListener(node));
        NodeChannels nodeChannels = new NodeChannels(channels, channels, channels, channels, channels);
        onAfterChannelsConnected(nodeChannels);
        return nodeChannels;
    }

    protected NodeChannels connectToChannels(DiscoveryNode node) {
        final NodeChannels nodeChannels =
            new NodeChannels(
                new Channel[connectionsPerNodeRecovery],
                new Channel[connectionsPerNodeBulk],
                new Channel[connectionsPerNodeReg],
                new Channel[connectionsPerNodeState],
                new Channel[connectionsPerNodePing]);
        boolean success = false;
        try {
            int numConnections =
                connectionsPerNodeRecovery +
                    connectionsPerNodeBulk +
                    connectionsPerNodeReg +
                    connectionsPerNodeState +
                    connectionsPerNodeRecovery;
            final ArrayList<ChannelFuture> connections = new ArrayList<>(numConnections);
            final InetSocketAddress address = ((InetSocketTransportAddress) node.getAddress()).address();
            for (int i = 0; i < numConnections; i++) {
                connections.add(bootstrap.connect(address));
            }
            final Iterator<ChannelFuture> iterator = connections.iterator();
            try {
                for (Channel[] channels : nodeChannels.getChannelArrays()) {
                    for (int i = 0; i < channels.length; i++) {
                        assert iterator.hasNext();
                        ChannelFuture future = iterator.next();
                        future.awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
                        if (!future.isSuccess()) {
                            throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", future.cause());
                        }
                        channels[i] = future.channel();
                        channels[i].closeFuture().addListener(new ChannelCloseListener(node));
                    }
                }
                if (nodeChannels.recovery.length == 0) {
                    if (nodeChannels.bulk.length > 0) {
                        nodeChannels.recovery = nodeChannels.bulk;
                    } else {
                        nodeChannels.recovery = nodeChannels.reg;
                    }
                }
                if (nodeChannels.bulk.length == 0) {
                    nodeChannels.bulk = nodeChannels.reg;
                }
            } catch (final RuntimeException e) {
                for (final ChannelFuture future : Collections.unmodifiableList(connections)) {
                    FutureUtils.cancel(future);
                    if (future.channel() != null && future.channel().isOpen()) {
                        try {
                            future.channel().close();
                        } catch (Exception inner) {
                            e.addSuppressed(inner);
                        }
                    }
                }
                throw e;
            }
            onAfterChannelsConnected(nodeChannels);
            success = true;
        } finally {
            if (success == false) {
                try {
                    nodeChannels.close();
                } catch (IOException e) {
                    logger.trace("exception while closing channels", e);
                }
            }
        }
        return nodeChannels;
    }

    /**
     * Allows for logic to be executed after a connection has been made on all channels. While this method is being executed, the node is
     * not listed as being connected to.
     * @param nodeChannels the {@link NodeChannels} that have been connected
     */
    protected void onAfterChannelsConnected(NodeChannels nodeChannels) {
    }

    private class ChannelCloseListener implements ChannelFutureListener {

        private final DiscoveryNode node;

        private ChannelCloseListener(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public void operationComplete(final ChannelFuture future) throws Exception {
            NodeChannels nodeChannels = connectedNodes.get(node);
            if (nodeChannels != null && nodeChannels.hasChannel(future.channel())) {
                threadPool.generic().execute(() -> disconnectFromNode(node, future.channel(), "channel closed event"));
            }
        }
    }

    @Override
    protected void sendMessage(Channel channel, BytesReference reference, Runnable sendListener, boolean close) {
        final ChannelFuture future = channel.writeAndFlush(Netty4Utils.toByteBuf(reference));
        if (close) {
            future.addListener(f -> {
                try {
                    sendListener.run();
                } finally {
                    future.channel().close();
                }
            });
        } else {
            future.addListener(f -> sendListener.run());
        }
    }

    @Override
    protected void closeChannels(final List<Channel> channels) throws IOException {
        Netty4Utils.closeChannels(channels);
    }

    @Override
    protected InetSocketAddress getLocalAddress(Channel channel) {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    protected Channel bind(String name, InetSocketAddress address) {
        return serverBootstraps.get(name).bind(address).syncUninterruptibly().channel();
    }

    ScheduledPing getPing() {
        return scheduledPing;
    }

    @Override
    protected boolean isOpen(Channel channel) {
        return channel.isOpen();
    }

    @Override
    @SuppressForbidden(reason = "debug")
    protected void stopInternal() {
        Releasables.close(serverOpenChannels, () -> {
            final List<Tuple<String, Future<?>>> serverBootstrapCloseFutures = new ArrayList<>(serverBootstraps.size());
            for (final Map.Entry<String, ServerBootstrap> entry : serverBootstraps.entrySet()) {
                serverBootstrapCloseFutures.add(
                    Tuple.tuple(entry.getKey(), entry.getValue().config().group().shutdownGracefully(0, 5, TimeUnit.SECONDS)));
            }
            for (final Tuple<String, Future<?>> future : serverBootstrapCloseFutures) {
                future.v2().awaitUninterruptibly();
                if (!future.v2().isSuccess()) {
                    logger.debug("Error closing server bootstrap for profile [{}]", future.v2().cause(), future.v1());
                }
            }
            serverBootstraps.clear();

            if (bootstrap != null) {
                bootstrap.config().group().shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
                bootstrap = null;
            }
        });
    }

    protected class ClientChannelInitializer extends ChannelInitializer<Channel> {

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast("size", new Netty4SizeHeaderFrameDecoder());
            // using a dot as a prefix means this cannot come from any settings parsed
            ch.pipeline().addLast("dispatcher", new Netty4MessageChannelHandler(Netty4Transport.this, ".client"));
        }

    }

    protected class ServerChannelInitializer extends ChannelInitializer<Channel> {

        protected final String name;
        protected final Settings settings;

        protected ServerChannelInitializer(String name, Settings settings) {
            this.name = name;
            this.settings = settings;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast("open_channels", Netty4Transport.this.serverOpenChannels);
            ch.pipeline().addLast("size", new Netty4SizeHeaderFrameDecoder());
            ch.pipeline().addLast("dispatcher", new Netty4MessageChannelHandler(Netty4Transport.this, name));
        }
    }

}
