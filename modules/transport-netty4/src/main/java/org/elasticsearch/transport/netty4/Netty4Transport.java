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
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportRequestOptions;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
            (s) -> Integer.toString(EsExecutors.numberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "transport.netty.worker_count"), Property.NodeScope);

    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_SIZE = Setting.byteSizeSetting(
            "transport.netty.receive_predictor_size", new ByteSizeValue(64, ByteSizeUnit.KB), Property.NodeScope);
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MIN =
        byteSizeSetting("transport.netty.receive_predictor_min", NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope);
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MAX =
        byteSizeSetting("transport.netty.receive_predictor_max", NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope);
    public static final Setting<Integer> NETTY_BOSS_COUNT =
        intSetting("transport.netty.boss_count", 1, 1, Property.NodeScope);


    protected final RecvByteBufAllocator recvByteBufAllocator;
    protected final int workerCount;
    protected final ByteSizeValue receivePredictorMin;
    protected final ByteSizeValue receivePredictorMax;
    // package private for testing
    volatile Netty4OpenChannelsHandler serverOpenChannels;
    protected volatile Bootstrap bootstrap;
    protected final Map<String, ServerBootstrap> serverBootstraps = newConcurrentMap();

    public Netty4Transport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                          NamedWriteableRegistry namedWriteableRegistry, CircuitBreakerService circuitBreakerService) {
        super("netty", settings, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, networkService);
        Netty4Utils.setAvailableProcessors(EsExecutors.PROCESSORS_SETTING.get(settings));
        this.workerCount = WORKER_COUNT.get(settings);

        // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
        this.receivePredictorMin = NETTY_RECEIVE_PREDICTOR_MIN.get(settings);
        this.receivePredictorMax = NETTY_RECEIVE_PREDICTOR_MAX.get(settings);
        if (receivePredictorMax.getBytes() == receivePredictorMin.getBytes()) {
            recvByteBufAllocator = new FixedRecvByteBufAllocator((int) receivePredictorMax.getBytes());
        } else {
            recvByteBufAllocator = new AdaptiveRecvByteBufAllocator((int) receivePredictorMin.getBytes(),
                (int) receivePredictorMin.getBytes(), (int) receivePredictorMax.getBytes());
        }
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            bootstrap = createBootstrap();
            if (NetworkService.NETWORK_SERVER.get(settings)) {
                final Netty4OpenChannelsHandler openChannels = new Netty4OpenChannelsHandler(logger);
                this.serverOpenChannels = openChannels;
                for (ProfileSettings profileSettings : profileSettings) {
                    createServerBootstrap(profileSettings);
                    bindServer(profileSettings);
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
        bootstrap.group(new NioEventLoopGroup(workerCount, daemonThreadFactory(settings, TRANSPORT_CLIENT_BOSS_THREAD_NAME_PREFIX)));
        bootstrap.channel(NioSocketChannel.class);

        bootstrap.handler(getClientChannelInitializer());

        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(defaultConnectionProfile.getConnectTimeout().millis()));
        bootstrap.option(ChannelOption.TCP_NODELAY, TCP_NO_DELAY.get(settings));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, TCP_KEEP_ALIVE.get(settings));

        final ByteSizeValue tcpSendBufferSize = TCP_SEND_BUFFER_SIZE.get(settings);
        if (tcpSendBufferSize.getBytes() > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, Math.toIntExact(tcpSendBufferSize.getBytes()));
        }

        final ByteSizeValue tcpReceiveBufferSize = TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.getBytes() > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, Math.toIntExact(tcpReceiveBufferSize.getBytes()));
        }

        bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);

        final boolean reuseAddress = TCP_REUSE_ADDRESS.get(settings);
        bootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);

        bootstrap.validate();

        return bootstrap;
    }

    private void createServerBootstrap(ProfileSettings profileSettings) {
        String name = profileSettings.profileName;
        if (logger.isDebugEnabled()) {
            logger.debug("using profile[{}], worker_count[{}], port[{}], bind_host[{}], publish_host[{}], compress[{}], "
                    + "connect_timeout[{}], connections_per_node[{}/{}/{}/{}/{}], receive_predictor[{}->{}]",
                name, workerCount, profileSettings.portOrRange, profileSettings.bindHosts, profileSettings.publishHosts, compress,
                defaultConnectionProfile.getConnectTimeout(),
                defaultConnectionProfile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY),
                defaultConnectionProfile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK),
                defaultConnectionProfile.getNumConnectionsPerType(TransportRequestOptions.Type.REG),
                defaultConnectionProfile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE),
                defaultConnectionProfile.getNumConnectionsPerType(TransportRequestOptions.Type.PING),
                receivePredictorMin, receivePredictorMax);
        }


        final ThreadFactory workerFactory = daemonThreadFactory(this.settings, TRANSPORT_SERVER_WORKER_THREAD_NAME_PREFIX, name);

        final ServerBootstrap serverBootstrap = new ServerBootstrap();

        serverBootstrap.group(new NioEventLoopGroup(workerCount, workerFactory));
        serverBootstrap.channel(NioServerSocketChannel.class);

        serverBootstrap.childHandler(getServerChannelInitializer(name));

        serverBootstrap.childOption(ChannelOption.TCP_NODELAY, profileSettings.tcpNoDelay);
        serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, profileSettings.tcpKeepAlive);

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

    protected ChannelHandler getClientChannelInitializer() {
        return new ClientChannelInitializer();
    }

    protected final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        final Throwable unwrapped = ExceptionsHelper.unwrap(cause, ElasticsearchException.class);
        final Throwable t = unwrapped != null ? unwrapped : cause;
        onException(ctx.channel(), t instanceof Exception ? (Exception) t : new ElasticsearchException(t));
    }

    @Override
    public long getNumOpenServerConnections() {
        Netty4OpenChannelsHandler channels = serverOpenChannels;
        return channels == null ? 0 : channels.numberOfOpenChannels();
    }

    @Override
    protected NodeChannels connectToChannels(DiscoveryNode node, ConnectionProfile profile, Consumer<Channel> onChannelClose) {
        final Channel[] channels = new Channel[profile.getNumConnections()];
        final NodeChannels nodeChannels = new NodeChannels(node, channels, profile);
        boolean success = false;
        try {
            final TimeValue connectTimeout;
            final Bootstrap bootstrap;
            final TimeValue defaultConnectTimeout = defaultConnectionProfile.getConnectTimeout();
            if (profile.getConnectTimeout() != null && profile.getConnectTimeout().equals(defaultConnectTimeout) == false) {
                bootstrap = this.bootstrap.clone(this.bootstrap.config().group());
                bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(profile.getConnectTimeout().millis()));
                connectTimeout = profile.getConnectTimeout();
            } else {
                connectTimeout = defaultConnectTimeout;
                bootstrap = this.bootstrap;
            }
            final ArrayList<ChannelFuture> connections = new ArrayList<>(channels.length);
            final InetSocketAddress address = node.getAddress().address();
            for (int i = 0; i < channels.length; i++) {
                connections.add(bootstrap.connect(address));
            }
            final Iterator<ChannelFuture> iterator = connections.iterator();
            final ChannelFutureListener closeListener = future -> onChannelClose.accept(future.channel());
            try {
                for (int i = 0; i < channels.length; i++) {
                    assert iterator.hasNext();
                    ChannelFuture future = iterator.next();
                    future.awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
                    if (!future.isSuccess()) {
                        throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", future.cause());
                    }
                    channels[i] = future.channel();
                    channels[i].closeFuture().addListener(closeListener);
                }
                assert iterator.hasNext() == false : "not all created connection have been consumed";
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

    @Override
    protected void sendMessage(Channel channel, BytesReference reference, ActionListener<Channel> listener) {
        final ChannelFuture future = channel.writeAndFlush(Netty4Utils.toByteBuf(reference));
        future.addListener(f -> {
            if (f.isSuccess()) {
                listener.onResponse(channel);
            } else {
                final Throwable cause = f.cause();
                Netty4Utils.maybeDie(cause);
                logger.warn((Supplier<?>) () ->
                    new ParameterizedMessage("write and flush on the network layer failed (channel: {})", channel), cause);
                assert cause instanceof Exception;
                listener.onFailure((Exception) cause);
            }
        });
    }

    @Override
    protected void closeChannels(final List<Channel> channels, boolean blocking, boolean closingTransport) throws IOException {
        if (closingTransport) {
            for (Channel channel : channels) {
                /* We set SO_LINGER timeout to 0 to ensure that when we shutdown the node we don't have a gazillion connections sitting
                 * in TIME_WAIT to free up resources quickly. This is really the only part where we close the connection from the server
                 * side otherwise the client (node) initiates the TCP closing sequence which doesn't cause these issues. Setting this
                 * by default from the beginning can have unexpected side-effects an should be avoided, our protocol is designed
                 * in a way that clients close connection which is how it should be*/
                if (channel.isOpen()) {
                    channel.config().setOption(ChannelOption.SO_LINGER, 0);
                }
            }
        }
        if (blocking) {
            Netty4Utils.closeChannels(channels);
        } else {
            for (Channel channel : channels) {
                if (channel != null && channel.isOpen()) {
                    ChannelFuture closeFuture = channel.close();
                    closeFuture.addListener((f) -> {
                        if (f.isSuccess() == false) {
                            logger.warn("failed to close channel", f.cause());
                        }
                    });
                }
            }
        }
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
                    logger.debug(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "Error closing server bootstrap for profile [{}]", future.v1()), future.v2().cause());
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
            ch.pipeline().addLast("logging", new ESLoggingHandler());
            ch.pipeline().addLast("size", new Netty4SizeHeaderFrameDecoder());
            // using a dot as a prefix means this cannot come from any settings parsed
            ch.pipeline().addLast("dispatcher", new Netty4MessageChannelHandler(Netty4Transport.this, ".client"));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            Netty4Utils.maybeDie(cause);
            super.exceptionCaught(ctx, cause);
        }

    }

    protected class ServerChannelInitializer extends ChannelInitializer<Channel> {

        protected final String name;

        protected ServerChannelInitializer(String name) {
            this.name = name;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast("logging", new ESLoggingHandler());
            ch.pipeline().addLast("open_channels", Netty4Transport.this.serverOpenChannels);
            ch.pipeline().addLast("size", new Netty4SizeHeaderFrameDecoder());
            ch.pipeline().addLast("dispatcher", new Netty4MessageChannelHandler(Netty4Transport.this, name));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            Netty4Utils.maybeDie(cause);
            super.exceptionCaught(ctx, cause);
        }

    }

}
