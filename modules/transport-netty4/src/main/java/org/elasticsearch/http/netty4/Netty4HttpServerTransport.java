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

package org.elasticsearch.http.netty4;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import io.netty.channel.socket.oio.OioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.timeout.ReadTimeoutException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServerAdapter;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfigBuilder;
import org.elasticsearch.http.netty4.cors.Netty4CorsHandler;
import org.elasticsearch.http.netty4.pipelining.HttpPipeliningHandler;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.netty4.Netty4OpenChannelsHandler;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.byteSizeSetting;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_MAX_AGE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_BIND_HOST;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PORT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PUBLISH_PORT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_RESET_COOKIES;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;
import static org.elasticsearch.http.netty4.cors.Netty4CorsHandler.ANY_ORIGIN;

public class Netty4HttpServerTransport extends AbstractLifecycleComponent implements HttpServerTransport {

    static {
        Netty4Utils.setup();
    }

    public static Setting<ByteSizeValue> SETTING_HTTP_NETTY_MAX_CUMULATION_BUFFER_CAPACITY =
        Setting.byteSizeSetting("http.netty.max_cumulation_buffer_capacity", new ByteSizeValue(-1),
            Property.NodeScope, Property.Shared);
    public static Setting<Integer> SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS =
        Setting.intSetting("http.netty.max_composite_buffer_components", -1, Property.NodeScope, Property.Shared);

    public static final Setting<Integer> SETTING_HTTP_WORKER_COUNT = new Setting<>("http.netty.worker_count",
        (s) -> Integer.toString(EsExecutors.boundedNumberOfProcessors(s) * 2),
        (s) -> Setting.parseInt(s, 1, "http.netty.worker_count"), Property.NodeScope, Property.Shared);

    public static final Setting<Boolean> SETTING_HTTP_TCP_NO_DELAY =
        boolSetting("http.tcp_no_delay", NetworkService.TcpSettings.TCP_NO_DELAY, Property.NodeScope, Property.Shared);
    public static final Setting<Boolean> SETTING_HTTP_TCP_KEEP_ALIVE =
        boolSetting("http.tcp.keep_alive", NetworkService.TcpSettings.TCP_KEEP_ALIVE, Property.NodeScope, Property.Shared);
    public static final Setting<Boolean> SETTING_HTTP_TCP_BLOCKING_SERVER =
        boolSetting("http.tcp.blocking_server", NetworkService.TcpSettings.TCP_BLOCKING_SERVER, Property.NodeScope, Property.Shared);
    public static final Setting<Boolean> SETTING_HTTP_TCP_REUSE_ADDRESS =
        boolSetting("http.tcp.reuse_address", NetworkService.TcpSettings.TCP_REUSE_ADDRESS, Property.NodeScope, Property.Shared);

    public static final Setting<ByteSizeValue> SETTING_HTTP_TCP_SEND_BUFFER_SIZE =
        Setting.byteSizeSetting("http.tcp.send_buffer_size", NetworkService.TcpSettings.TCP_SEND_BUFFER_SIZE,
            Property.NodeScope, Property.Shared);
    public static final Setting<ByteSizeValue> SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE =
        Setting.byteSizeSetting("http.tcp.receive_buffer_size", NetworkService.TcpSettings.TCP_RECEIVE_BUFFER_SIZE,
            Property.NodeScope, Property.Shared);
    public static final Setting<ByteSizeValue> SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE =
        Setting.byteSizeSetting("transport.netty.receive_predictor_size",
            settings -> {
                long defaultReceiverPredictor = 512 * 1024;
                if (JvmInfo.jvmInfo().getMem().getDirectMemoryMax().bytes() > 0) {
                    // we can guess a better default...
                    long l = (long) ((0.3 * JvmInfo.jvmInfo().getMem().getDirectMemoryMax().bytes()) / SETTING_HTTP_WORKER_COUNT.get
                        (settings));
                    defaultReceiverPredictor = Math.min(defaultReceiverPredictor, Math.max(l, 64 * 1024));
                }
                return new ByteSizeValue(defaultReceiverPredictor).toString();
            }, Property.NodeScope);
    public static final Setting<ByteSizeValue> SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_MIN =
        byteSizeSetting("http.netty.receive_predictor_min", SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope);
    public static final Setting<ByteSizeValue> SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_MAX =
        byteSizeSetting("http.netty.receive_predictor_max", SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope);


    protected final NetworkService networkService;
    protected final BigArrays bigArrays;

    protected final ByteSizeValue maxContentLength;
    protected final ByteSizeValue maxInitialLineLength;
    protected final ByteSizeValue maxHeaderSize;
    protected final ByteSizeValue maxChunkSize;

    protected final int workerCount;

    protected final boolean blockingServer;

    protected final boolean pipelining;

    protected final int pipeliningMaxEvents;

    protected final boolean compression;

    protected final int compressionLevel;

    protected final boolean resetCookies;

    protected final PortsRange port;

    protected final String bindHosts[];

    protected final String publishHosts[];

    protected final boolean detailedErrorsEnabled;
    protected final ThreadPool threadPool;

    protected final boolean tcpNoDelay;
    protected final boolean tcpKeepAlive;
    protected final boolean reuseAddress;

    protected final ByteSizeValue tcpSendBufferSize;
    protected final ByteSizeValue tcpReceiveBufferSize;
    protected final RecvByteBufAllocator recvByteBufAllocator;

    protected final ByteSizeValue maxCumulationBufferCapacity;
    protected final int maxCompositeBufferComponents;

    protected volatile ServerBootstrap serverBootstrap;

    protected volatile BoundTransportAddress boundAddress;

    protected final List<Channel> serverChannels = new ArrayList<>();

    // package private for testing
    Netty4OpenChannelsHandler serverOpenChannels;

    protected volatile HttpServerAdapter httpServerAdapter;

    private final Netty4CorsConfig corsConfig;

    @Inject
    public Netty4HttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays, ThreadPool threadPool) {
        super(settings);
        this.networkService = networkService;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;

        ByteSizeValue maxContentLength = SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings);
        this.maxChunkSize = SETTING_HTTP_MAX_CHUNK_SIZE.get(settings);
        this.maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.get(settings);
        this.maxInitialLineLength = SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings);
        this.resetCookies = SETTING_HTTP_RESET_COOKIES.get(settings);
        this.maxCumulationBufferCapacity = SETTING_HTTP_NETTY_MAX_CUMULATION_BUFFER_CAPACITY.get(settings);
        this.maxCompositeBufferComponents = SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS.get(settings);
        this.workerCount = SETTING_HTTP_WORKER_COUNT.get(settings);
        this.blockingServer = SETTING_HTTP_TCP_BLOCKING_SERVER.get(settings);
        this.port = SETTING_HTTP_PORT.get(settings);
        this.bindHosts = SETTING_HTTP_BIND_HOST.get(settings).toArray(Strings.EMPTY_ARRAY);
        this.publishHosts = SETTING_HTTP_PUBLISH_HOST.get(settings).toArray(Strings.EMPTY_ARRAY);
        this.tcpNoDelay = SETTING_HTTP_TCP_NO_DELAY.get(settings);
        this.tcpKeepAlive = SETTING_HTTP_TCP_KEEP_ALIVE.get(settings);
        this.reuseAddress = SETTING_HTTP_TCP_REUSE_ADDRESS.get(settings);
        this.tcpSendBufferSize = SETTING_HTTP_TCP_SEND_BUFFER_SIZE.get(settings);
        this.tcpReceiveBufferSize = SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE.get(settings);
        this.detailedErrorsEnabled = SETTING_HTTP_DETAILED_ERRORS_ENABLED.get(settings);

        // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
        ByteSizeValue receivePredictorMin = SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_MIN.get(settings);
        ByteSizeValue receivePredictorMax = SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_MAX.get(settings);
        if (receivePredictorMax.bytes() == receivePredictorMin.bytes()) {
            recvByteBufAllocator = new FixedRecvByteBufAllocator(Math.toIntExact(receivePredictorMax.bytes()));
        } else {
            recvByteBufAllocator = new AdaptiveRecvByteBufAllocator(
                Math.toIntExact(receivePredictorMin.bytes()),
                Math.toIntExact(receivePredictorMin.bytes()),
                Math.toIntExact(receivePredictorMax.bytes()));
        }

        this.compression = SETTING_HTTP_COMPRESSION.get(settings);
        this.compressionLevel = SETTING_HTTP_COMPRESSION_LEVEL.get(settings);
        this.pipelining = SETTING_PIPELINING.get(settings);
        this.pipeliningMaxEvents = SETTING_PIPELINING_MAX_EVENTS.get(settings);
        this.corsConfig = buildCorsConfig(settings);

        // validate max content length
        if (maxContentLength.bytes() > Integer.MAX_VALUE) {
            logger.warn("maxContentLength[{}] set to high value, resetting it to [100mb]", maxContentLength);
            maxContentLength = new ByteSizeValue(100, ByteSizeUnit.MB);
        }
        this.maxContentLength = maxContentLength;

        logger.debug("using max_chunk_size[{}], max_header_size[{}], max_initial_line_length[{}], max_content_length[{}], " +
                "receive_predictor[{}->{}], pipelining[{}], pipelining_max_events[{}]",
            maxChunkSize, maxHeaderSize, maxInitialLineLength, this.maxContentLength,
            receivePredictorMin, receivePredictorMax, pipelining, pipeliningMaxEvents);
    }

    public Settings settings() {
        return this.settings;
    }

    @Override
    public void httpServerAdapter(HttpServerAdapter httpServerAdapter) {
        this.httpServerAdapter = httpServerAdapter;
    }

    @Override
    protected void doStart() {
        this.serverOpenChannels = new Netty4OpenChannelsHandler(logger);

        serverBootstrap = new ServerBootstrap();
        if (blockingServer) {
            serverBootstrap.group(new OioEventLoopGroup(workerCount, daemonThreadFactory(settings, "http_server_worker")));
            serverBootstrap.channel(OioServerSocketChannel.class);
        } else {
            serverBootstrap.group(new NioEventLoopGroup(workerCount, daemonThreadFactory(settings, "http_server_worker")));
            serverBootstrap.channel(NioServerSocketChannel.class);
        }

        serverBootstrap.childHandler(configureServerChannelHandler());

        serverBootstrap.childOption(ChannelOption.TCP_NODELAY, SETTING_HTTP_TCP_NO_DELAY.get(settings));
        serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, SETTING_HTTP_TCP_KEEP_ALIVE.get(settings));

        final ByteSizeValue tcpSendBufferSize = SETTING_HTTP_TCP_SEND_BUFFER_SIZE.get(settings);
        if (tcpSendBufferSize.bytes() > 0) {
            serverBootstrap.childOption(ChannelOption.SO_SNDBUF, Math.toIntExact(tcpSendBufferSize.bytes()));
        }

        final ByteSizeValue tcpReceiveBufferSize = SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.bytes() > 0) {
            serverBootstrap.childOption(ChannelOption.SO_RCVBUF, Math.toIntExact(tcpReceiveBufferSize.bytes()));
        }

        serverBootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);
        serverBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);

        final boolean reuseAddress = SETTING_HTTP_TCP_REUSE_ADDRESS.get(settings);
        serverBootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);
        serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, reuseAddress);

        this.boundAddress = createBoundHttpAddress();
    }

    private BoundTransportAddress createBoundHttpAddress() {
        // Bind and start to accept incoming connections.
        InetAddress hostAddresses[];
        try {
            hostAddresses = networkService.resolveBindHostAddresses(bindHosts);
        } catch (IOException e) {
            throw new BindHttpException("Failed to resolve host [" + Arrays.toString(bindHosts) + "]", e);
        }

        List<InetSocketTransportAddress> boundAddresses = new ArrayList<>(hostAddresses.length);
        for (InetAddress address : hostAddresses) {
            boundAddresses.add(bindAddress(address));
        }

        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }

        final int publishPort = resolvePublishPort(settings, boundAddresses, publishInetAddress);
        final InetSocketAddress publishAddress = new InetSocketAddress(publishInetAddress, publishPort);
        return new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[0]), new InetSocketTransportAddress(publishAddress));
    }

    // package private for tests
    static int resolvePublishPort(Settings settings, List<InetSocketTransportAddress> boundAddresses, InetAddress publishInetAddress) {
        int publishPort = SETTING_HTTP_PUBLISH_PORT.get(settings);

        if (publishPort < 0) {
            for (InetSocketTransportAddress boundAddress : boundAddresses) {
                InetAddress boundInetAddress = boundAddress.address().getAddress();
                if (boundInetAddress.isAnyLocalAddress() || boundInetAddress.equals(publishInetAddress)) {
                    publishPort = boundAddress.getPort();
                    break;
                }
            }
        }

        // if no matching boundAddress found, check if there is a unique port for all bound addresses
        if (publishPort < 0) {
            final IntSet ports = new IntHashSet();
            for (InetSocketTransportAddress boundAddress : boundAddresses) {
                ports.add(boundAddress.getPort());
            }
            if (ports.size() == 1) {
                publishPort = ports.iterator().next().value;
            }
        }

        if (publishPort < 0) {
            throw new BindHttpException("Failed to auto-resolve http publish port, multiple bound addresses " + boundAddresses +
                " with distinct ports and none of them matched the publish address (" + publishInetAddress + "). " +
                "Please specify a unique port by setting " + SETTING_HTTP_PORT.getKey() + " or " + SETTING_HTTP_PUBLISH_PORT.getKey());
        }
        return publishPort;
    }

    private Netty4CorsConfig buildCorsConfig(Settings settings) {
        if (SETTING_CORS_ENABLED.get(settings) == false) {
            return Netty4CorsConfigBuilder.forOrigins().disable().build();
        }
        String origin = SETTING_CORS_ALLOW_ORIGIN.get(settings);
        final Netty4CorsConfigBuilder builder;
        if (Strings.isNullOrEmpty(origin)) {
            builder = Netty4CorsConfigBuilder.forOrigins();
        } else if (origin.equals(ANY_ORIGIN)) {
            builder = Netty4CorsConfigBuilder.forAnyOrigin();
        } else {
            Pattern p = RestUtils.checkCorsSettingForRegex(origin);
            if (p == null) {
                builder = Netty4CorsConfigBuilder.forOrigins(RestUtils.corsSettingAsArray(origin));
            } else {
                builder = Netty4CorsConfigBuilder.forPattern(p);
            }
        }
        if (SETTING_CORS_ALLOW_CREDENTIALS.get(settings)) {
            builder.allowCredentials();
        }
        String[] strMethods = settings.getAsArray(SETTING_CORS_ALLOW_METHODS.getKey());
        HttpMethod[] methods = Arrays.asList(strMethods)
            .stream()
            .map(HttpMethod::valueOf)
            .toArray(size -> new HttpMethod[size]);
        return builder.allowedRequestMethods(methods)
            .maxAge(SETTING_CORS_MAX_AGE.get(settings))
            .allowedRequestHeaders(settings.getAsArray(SETTING_CORS_ALLOW_HEADERS.getKey()))
            .shortCircuit()
            .build();
    }

    private InetSocketTransportAddress bindAddress(final InetAddress hostAddress) {
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = port.iterate(new PortsRange.PortCallback() {
            @Override
            public boolean onPortNumber(int portNumber) {
                try {
                    synchronized (serverChannels) {
                        ChannelFuture future = serverBootstrap.bind(new InetSocketAddress(hostAddress, portNumber)).sync();
                        serverChannels.add(future.channel());
                        boundSocket.set((InetSocketAddress) future.channel().localAddress());
                    }
                } catch (Exception e) {
                    lastException.set(e);
                    return false;
                }
                return true;
            }
        });
        if (!success) {
            throw new BindHttpException("Failed to bind to [" + port + "]", lastException.get());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Bound http to address {{}}", NetworkAddress.format(boundSocket.get()));
        }
        return new InetSocketTransportAddress(boundSocket.get());
    }

    @Override
    protected void doStop() {
        synchronized (serverChannels) {
            if (!serverChannels.isEmpty()) {
                try {
                    Netty4Utils.closeChannels(serverChannels);
                } catch (IOException e) {
                    logger.trace("exception while closing channels", e);
                }
                serverChannels.clear();
            }
        }

        if (serverOpenChannels != null) {
            serverOpenChannels.close();
            serverOpenChannels = null;
        }

        if (serverBootstrap != null) {
            serverBootstrap.config().group().shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
            serverBootstrap = null;
        }
    }

    @Override
    protected void doClose() {
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    @Override
    public HttpInfo info() {
        BoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        return new HttpInfo(boundTransportAddress, maxContentLength.bytes());
    }

    @Override
    public HttpStats stats() {
        Netty4OpenChannelsHandler channels = serverOpenChannels;
        return new HttpStats(channels == null ? 0 : channels.numberOfOpenChannels(), channels == null ? 0 : channels.totalChannels());
    }

    public Netty4CorsConfig getCorsConfig() {
        return corsConfig;
    }

    protected void dispatchRequest(RestRequest request, RestChannel channel) {
        httpServerAdapter.dispatchRequest(request, channel, threadPool.getThreadContext());
    }

    protected void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof ReadTimeoutException) {
            if (logger.isTraceEnabled()) {
                logger.trace("Connection timeout [{}]", ctx.channel().remoteAddress());
            }
            ctx.channel().close();
        } else {
            if (!lifecycle.started()) {
                // ignore
                return;
            }
            if (!NetworkExceptionHelper.isCloseConnectionException(cause)) {
                logger.warn("caught exception while handling client http traffic, closing connection {}", cause, ctx.channel());
                ctx.channel().close();
            } else {
                logger.debug("caught exception while handling client http traffic, closing connection {}", cause, ctx.channel());
                ctx.channel().close();
            }
        }
    }

    public ChannelHandler configureServerChannelHandler() {
        return new HttpChannelHandler(this, detailedErrorsEnabled, threadPool.getThreadContext());
    }

    protected static class HttpChannelHandler extends ChannelInitializer<Channel> {

        private final Netty4HttpServerTransport transport;
        private final Netty4HttpRequestHandler requestHandler;

        protected HttpChannelHandler(
            final Netty4HttpServerTransport transport,
            final boolean detailedErrorsEnabled,
            final ThreadContext threadContext) {
            this.transport = transport;
            this.requestHandler = new Netty4HttpRequestHandler(transport, detailedErrorsEnabled, threadContext);
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast("openChannels", transport.serverOpenChannels);
            final HttpRequestDecoder decoder = new HttpRequestDecoder(
                Math.toIntExact(transport.maxInitialLineLength.bytes()),
                Math.toIntExact(transport.maxHeaderSize.bytes()),
                Math.toIntExact(transport.maxChunkSize.bytes()));
            decoder.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
            ch.pipeline().addLast("decoder", decoder);
            ch.pipeline().addLast("decoder_compress", new HttpContentDecompressor());
            final HttpObjectAggregator aggregator = new HttpObjectAggregator(Math.toIntExact(transport.maxContentLength.bytes()));
            if (transport.maxCompositeBufferComponents != -1) {
                aggregator.setMaxCumulationBufferComponents(transport.maxCompositeBufferComponents);
            }
            ch.pipeline().addLast("aggregator", aggregator);
            ch.pipeline().addLast("encoder", new HttpResponseEncoder());
            if (transport.compression) {
                ch.pipeline().addLast("encoder_compress", new HttpContentCompressor(transport.compressionLevel));
            }
            if (SETTING_CORS_ENABLED.get(transport.settings())) {
                ch.pipeline().addLast("cors", new Netty4CorsHandler(transport.getCorsConfig()));
            }
            if (transport.pipelining) {
                ch.pipeline().addLast("pipelining", new HttpPipeliningHandler(transport.pipeliningMaxEvents));
            }
            ch.pipeline().addLast("handler", requestHandler);
        }

    }

}
