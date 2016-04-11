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

package org.elasticsearch.http.netty;

import org.elasticsearch.common.Booleans;
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.netty.NettyUtils;
import org.elasticsearch.common.netty.OpenChannelsHandler;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.*;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.http.netty.pipelining.HttpPipeliningHandler;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpServerAdapter;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.http.netty.cors.CorsConfig;
import org.elasticsearch.http.netty.cors.CorsConfigBuilder;
import org.elasticsearch.http.netty.cors.CorsHandler;
import org.elasticsearch.rest.support.RestUtils;
import org.elasticsearch.transport.BindTransportException;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.timeout.ReadTimeoutException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.elasticsearch.common.network.NetworkService.TcpSettings.*;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.common.network.NetworkService.TcpSettings.TCP_BLOCKING;
import static org.elasticsearch.common.network.NetworkService.TcpSettings.TCP_KEEP_ALIVE;
import static org.elasticsearch.common.network.NetworkService.TcpSettings.TCP_NO_DELAY;
import static org.elasticsearch.common.network.NetworkService.TcpSettings.TCP_RECEIVE_BUFFER_SIZE;
import static org.elasticsearch.common.network.NetworkService.TcpSettings.TCP_REUSE_ADDRESS;
import static org.elasticsearch.common.network.NetworkService.TcpSettings.TCP_SEND_BUFFER_SIZE;
import static org.elasticsearch.http.netty.cors.CorsHandler.ANY_ORIGIN;

/**
 *
 */
public class NettyHttpServerTransport extends AbstractLifecycleComponent<HttpServerTransport> implements HttpServerTransport {

    static {
        NettyUtils.setup();
    }

    public static final String SETTING_CORS_ENABLED = "http.cors.enabled";
    public static final String SETTING_CORS_ALLOW_ORIGIN = "http.cors.allow-origin";
    public static final String SETTING_CORS_MAX_AGE = "http.cors.max-age";
    public static final String SETTING_CORS_ALLOW_METHODS = "http.cors.allow-methods";
    public static final String SETTING_CORS_ALLOW_HEADERS = "http.cors.allow-headers";
    public static final String SETTING_CORS_ALLOW_CREDENTIALS = "http.cors.allow-credentials";
    public static final String SETTING_PIPELINING = "http.pipelining";
    public static final String SETTING_PIPELINING_MAX_EVENTS = "http.pipelining.max_events";
    public static final String SETTING_HTTP_COMPRESSION = "http.compression";
    public static final String SETTING_HTTP_COMPRESSION_LEVEL = "http.compression_level";
    public static final String SETTING_HTTP_DETAILED_ERRORS_ENABLED = "http.detailed_errors.enabled";

    public static final boolean DEFAULT_SETTING_PIPELINING = true;
    public static final int DEFAULT_SETTING_PIPELINING_MAX_EVENTS = 10000;
    public static final String DEFAULT_PORT_RANGE = "9200-9300";

    private static final String[] DEFAULT_CORS_METHODS = { "OPTIONS", "HEAD", "GET", "POST", "PUT", "DELETE" };
    private static final String[] DEFAULT_CORS_HEADERS = { "X-Requested-With", "Content-Type", "Content-Length" };
    private static final int DEFAULT_CORS_MAX_AGE = 1728000;

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

    protected final String port;

    protected final String bindHosts[];

    protected final String publishHosts[];

    protected final boolean detailedErrorsEnabled;

    protected final String tcpNoDelay;
    protected final String tcpKeepAlive;
    protected final boolean reuseAddress;

    protected final ByteSizeValue tcpSendBufferSize;
    protected final ByteSizeValue tcpReceiveBufferSize;
    protected final ReceiveBufferSizePredictorFactory receiveBufferSizePredictorFactory;

    protected final ByteSizeValue maxCumulationBufferCapacity;
    protected final int maxCompositeBufferComponents;

    protected volatile ServerBootstrap serverBootstrap;

    protected volatile BoundTransportAddress boundAddress;

    protected volatile List<Channel> serverChannels = new ArrayList<>();

    // package private for testing
    OpenChannelsHandler serverOpenChannels;

    protected volatile HttpServerAdapter httpServerAdapter;

    private final CorsConfig corsConfig;

    @Inject
    public NettyHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays) {
        super(settings);
        this.networkService = networkService;
        this.bigArrays = bigArrays;

        if (settings.getAsBoolean("netty.epollBugWorkaround", false)) {
            System.setProperty("org.jboss.netty.epollBugWorkaround", "true");
        }

        ByteSizeValue maxContentLength = settings.getAsBytesSize("http.netty.max_content_length", settings.getAsBytesSize("http.max_content_length", new ByteSizeValue(100, ByteSizeUnit.MB)));
        this.maxChunkSize = settings.getAsBytesSize("http.netty.max_chunk_size", settings.getAsBytesSize("http.max_chunk_size", new ByteSizeValue(8, ByteSizeUnit.KB)));
        this.maxHeaderSize = settings.getAsBytesSize("http.netty.max_header_size", settings.getAsBytesSize("http.max_header_size", new ByteSizeValue(8, ByteSizeUnit.KB)));
        this.maxInitialLineLength = settings.getAsBytesSize("http.netty.max_initial_line_length", settings.getAsBytesSize("http.max_initial_line_length", new ByteSizeValue(4, ByteSizeUnit.KB)));
        // don't reset cookies by default, since I don't think we really need to
        // note, parsing cookies was fixed in netty 3.5.1 regarding stack allocation, but still, currently, we don't need cookies
        this.resetCookies = settings.getAsBoolean("http.netty.reset_cookies", settings.getAsBoolean("http.reset_cookies", false));
        this.maxCumulationBufferCapacity = settings.getAsBytesSize("http.netty.max_cumulation_buffer_capacity", null);
        this.maxCompositeBufferComponents = settings.getAsInt("http.netty.max_composite_buffer_components", -1);
        this.workerCount = settings.getAsInt("http.netty.worker_count", EsExecutors.boundedNumberOfProcessors(settings) * 2);
        this.blockingServer = settings.getAsBoolean("http.netty.http.blocking_server", settings.getAsBoolean(TCP_BLOCKING_SERVER, settings.getAsBoolean(TCP_BLOCKING, false)));
        this.port = settings.get("http.netty.port", settings.get("http.port", DEFAULT_PORT_RANGE));
        this.bindHosts = settings.getAsArray("http.netty.bind_host", settings.getAsArray("http.bind_host", settings.getAsArray("http.host", null)));
        this.publishHosts = settings.getAsArray("http.netty.publish_host", settings.getAsArray("http.publish_host", settings.getAsArray("http.host", null)));
        this.tcpNoDelay = settings.get("http.netty.tcp_no_delay", settings.get(TCP_NO_DELAY, "true"));
        this.tcpKeepAlive = settings.get("http.netty.tcp_keep_alive", settings.get(TCP_KEEP_ALIVE, "true"));
        this.reuseAddress = settings.getAsBoolean("http.netty.reuse_address", settings.getAsBoolean(TCP_REUSE_ADDRESS, NetworkUtils.defaultReuseAddress()));
        this.tcpSendBufferSize = settings.getAsBytesSize("http.netty.tcp_send_buffer_size", settings.getAsBytesSize(TCP_SEND_BUFFER_SIZE, TCP_DEFAULT_SEND_BUFFER_SIZE));
        this.tcpReceiveBufferSize = settings.getAsBytesSize("http.netty.tcp_receive_buffer_size", settings.getAsBytesSize(TCP_RECEIVE_BUFFER_SIZE, TCP_DEFAULT_RECEIVE_BUFFER_SIZE));
        this.detailedErrorsEnabled = settings.getAsBoolean(SETTING_HTTP_DETAILED_ERRORS_ENABLED, true);

        long defaultReceiverPredictor = 512 * 1024;
        if (JvmInfo.jvmInfo().getMem().getDirectMemoryMax().bytes() > 0) {
            // we can guess a better default...
            long l = (long) ((0.3 * JvmInfo.jvmInfo().getMem().getDirectMemoryMax().bytes()) / workerCount);
            defaultReceiverPredictor = Math.min(defaultReceiverPredictor, Math.max(l, 64 * 1024));
        }

        // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
        ByteSizeValue receivePredictorMin = settings.getAsBytesSize("http.netty.receive_predictor_min", settings.getAsBytesSize("http.netty.receive_predictor_size", new ByteSizeValue(defaultReceiverPredictor)));
        ByteSizeValue receivePredictorMax = settings.getAsBytesSize("http.netty.receive_predictor_max", settings.getAsBytesSize("http.netty.receive_predictor_size", new ByteSizeValue(defaultReceiverPredictor)));
        if (receivePredictorMax.bytes() == receivePredictorMin.bytes()) {
            receiveBufferSizePredictorFactory = new FixedReceiveBufferSizePredictorFactory((int) receivePredictorMax.bytes());
        } else {
            receiveBufferSizePredictorFactory = new AdaptiveReceiveBufferSizePredictorFactory((int) receivePredictorMin.bytes(), (int) receivePredictorMin.bytes(), (int) receivePredictorMax.bytes());
        }

        this.compression = settings.getAsBoolean(SETTING_HTTP_COMPRESSION, false);
        this.compressionLevel = settings.getAsInt(SETTING_HTTP_COMPRESSION_LEVEL, 6);
        this.pipelining = settings.getAsBoolean(SETTING_PIPELINING, DEFAULT_SETTING_PIPELINING);
        this.pipeliningMaxEvents = settings.getAsInt(SETTING_PIPELINING_MAX_EVENTS, DEFAULT_SETTING_PIPELINING_MAX_EVENTS);
        this.corsConfig = buildCorsConfig(settings);

        // validate max content length
        if (maxContentLength.bytes() > Integer.MAX_VALUE) {
            logger.warn("maxContentLength[" + maxContentLength + "] set to high value, resetting it to [100mb]");
            maxContentLength = new ByteSizeValue(100, ByteSizeUnit.MB);
        }
        this.maxContentLength = maxContentLength;

        logger.debug("using max_chunk_size[{}], max_header_size[{}], max_initial_line_length[{}], max_content_length[{}], receive_predictor[{}->{}], pipelining[{}], pipelining_max_events[{}]",
                maxChunkSize, maxHeaderSize, maxInitialLineLength, this.maxContentLength, receivePredictorMin, receivePredictorMax, pipelining, pipeliningMaxEvents);
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
        this.serverOpenChannels = new OpenChannelsHandler(logger);

        if (blockingServer) {
            serverBootstrap = new ServerBootstrap(new OioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, "http_server_boss")),
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, "http_server_worker"))
            ));
        } else {
            serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, "http_server_boss")),
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, "http_server_worker")),
                    workerCount));
        }

        serverBootstrap.setPipelineFactory(configureServerChannelPipelineFactory());

        if (!"default".equals(tcpNoDelay)) {
            serverBootstrap.setOption("child.tcpNoDelay", Booleans.parseBoolean(tcpNoDelay, null));
        }
        if (!"default".equals(tcpKeepAlive)) {
            serverBootstrap.setOption("child.keepAlive", Booleans.parseBoolean(tcpKeepAlive, null));
        }
        if (tcpSendBufferSize != null && tcpSendBufferSize.bytes() > 0) {
            serverBootstrap.setOption("child.sendBufferSize", tcpSendBufferSize.bytes());
        }
        if (tcpReceiveBufferSize != null && tcpReceiveBufferSize.bytes() > 0) {
            serverBootstrap.setOption("child.receiveBufferSize", tcpReceiveBufferSize.bytes());
        }
        serverBootstrap.setOption("receiveBufferSizePredictorFactory", receiveBufferSizePredictorFactory);
        serverBootstrap.setOption("child.receiveBufferSizePredictorFactory", receiveBufferSizePredictorFactory);
        serverBootstrap.setOption("reuseAddress", reuseAddress);
        serverBootstrap.setOption("child.reuseAddress", reuseAddress);
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
        return new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[boundAddresses.size()]), new InetSocketTransportAddress(publishAddress));
    }

    // package private for tests
    static int resolvePublishPort(Settings settings, List<InetSocketTransportAddress> boundAddresses, InetAddress publishInetAddress) {
        int publishPort = settings.getAsInt("http.netty.publish_port", settings.getAsInt("http.publish_port", -1));

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
                "Please specify a unique port by setting http.port or http.publish_port");
        }
        return publishPort;
    }

    private CorsConfig buildCorsConfig(Settings settings) {
        if (settings.getAsBoolean(SETTING_CORS_ENABLED, false) == false) {
            return CorsConfigBuilder.forOrigins().disable().build();
        }
        String origin = settings.get(SETTING_CORS_ALLOW_ORIGIN);
        final CorsConfigBuilder builder;
        if (Strings.isNullOrEmpty(origin)) {
            builder = CorsConfigBuilder.forOrigins();
        } else if (origin.equals(ANY_ORIGIN)) {
            builder = CorsConfigBuilder.forAnyOrigin();
        } else {
            Pattern p = RestUtils.checkCorsSettingForRegex(origin);
            if (p == null) {
                builder = CorsConfigBuilder.forOrigins(RestUtils.corsSettingAsArray(origin));
            } else {
                builder = CorsConfigBuilder.forPattern(p);
            }
        }
        if (settings.getAsBoolean(SETTING_CORS_ALLOW_CREDENTIALS, false)) {
            builder.allowCredentials();
        }
        String[] strMethods = settings.getAsArray(SETTING_CORS_ALLOW_METHODS, DEFAULT_CORS_METHODS);
        HttpMethod[] methods = new HttpMethod[strMethods.length];
        for (int i = 0; i < methods.length; i++) {
            methods[i] = HttpMethod.valueOf(strMethods[i]);
        }
        return builder.allowedRequestMethods(methods)
                      .maxAge(settings.getAsInt(SETTING_CORS_MAX_AGE, DEFAULT_CORS_MAX_AGE))
                      .allowedRequestHeaders(settings.getAsArray(SETTING_CORS_ALLOW_HEADERS, DEFAULT_CORS_HEADERS))
                      .shortCircuit()
                      .build();
    }

    private InetSocketTransportAddress bindAddress(final InetAddress hostAddress) {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = portsRange.iterate(new PortsRange.PortCallback() {
            @Override
            public boolean onPortNumber(int portNumber) {
                try {
                    synchronized (serverChannels) {
                        Channel channel = serverBootstrap.bind(new InetSocketAddress(hostAddress, portNumber));
                        serverChannels.add(channel);
                        boundSocket.set((InetSocketAddress) channel.getLocalAddress());
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
            if (serverChannels != null) {
                for (Channel channel : serverChannels) {
                    channel.close().awaitUninterruptibly();
                }
                serverChannels = null;
            }
        }

        if (serverOpenChannels != null) {
            serverOpenChannels.close();
            serverOpenChannels = null;
        }

        if (serverBootstrap != null) {
            serverBootstrap.releaseExternalResources();
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
        OpenChannelsHandler channels = serverOpenChannels;
        return new HttpStats(channels == null ? 0 : channels.numberOfOpenChannels(), channels == null ? 0 : channels.totalChannels());
    }

    public CorsConfig getCorsConfig() {
        return corsConfig;
    }

    protected void dispatchRequest(HttpRequest request, HttpChannel channel) {
        httpServerAdapter.dispatchRequest(request, channel);
    }

    protected void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if (e.getCause() instanceof ReadTimeoutException) {
            if (logger.isTraceEnabled()) {
                logger.trace("Connection timeout [{}]", ctx.getChannel().getRemoteAddress());
            }
            ctx.getChannel().close();
        } else {
            if (!lifecycle.started()) {
                // ignore
                return;
            }
            if (!NetworkExceptionHelper.isCloseConnectionException(e.getCause())) {
                logger.warn("Caught exception while handling client http traffic, closing connection {}", e.getCause(), ctx.getChannel());
                ctx.getChannel().close();
            } else {
                logger.debug("Caught exception while handling client http traffic, closing connection {}", e.getCause(), ctx.getChannel());
                ctx.getChannel().close();
            }
        }
    }

    public ChannelPipelineFactory configureServerChannelPipelineFactory() {
        return new HttpChannelPipelineFactory(this, detailedErrorsEnabled);
    }

    protected static class HttpChannelPipelineFactory implements ChannelPipelineFactory {

        protected final NettyHttpServerTransport transport;
        protected final HttpRequestHandler requestHandler;

        public HttpChannelPipelineFactory(NettyHttpServerTransport transport, boolean detailedErrorsEnabled) {
            this.transport = transport;
            this.requestHandler = new HttpRequestHandler(transport, detailedErrorsEnabled);
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("openChannels", transport.serverOpenChannels);
            HttpRequestDecoder requestDecoder = new HttpRequestDecoder(
                    (int) transport.maxInitialLineLength.bytes(),
                    (int) transport.maxHeaderSize.bytes(),
                    (int) transport.maxChunkSize.bytes()
            );
            if (transport.maxCumulationBufferCapacity != null) {
                if (transport.maxCumulationBufferCapacity.bytes() > Integer.MAX_VALUE) {
                    requestDecoder.setMaxCumulationBufferCapacity(Integer.MAX_VALUE);
                } else {
                    requestDecoder.setMaxCumulationBufferCapacity((int) transport.maxCumulationBufferCapacity.bytes());
                }
            }
            if (transport.maxCompositeBufferComponents != -1) {
                requestDecoder.setMaxCumulationBufferComponents(transport.maxCompositeBufferComponents);
            }
            pipeline.addLast("decoder", requestDecoder);
            pipeline.addLast("decoder_compress", new ESHttpContentDecompressor(transport.compression));
            HttpChunkAggregator httpChunkAggregator = new HttpChunkAggregator((int) transport.maxContentLength.bytes());
            if (transport.maxCompositeBufferComponents != -1) {
                httpChunkAggregator.setMaxCumulationBufferComponents(transport.maxCompositeBufferComponents);
            }
            pipeline.addLast("aggregator", httpChunkAggregator);
            if (transport.settings().getAsBoolean(SETTING_CORS_ENABLED, false)) {
                pipeline.addLast("cors", new CorsHandler(transport.getCorsConfig()));
            }
            pipeline.addLast("encoder", new ESHttpResponseEncoder());
            if (transport.compression) {
                pipeline.addLast("encoder_compress", new HttpContentCompressor(transport.compressionLevel));
            }
            if (transport.pipelining) {
                pipeline.addLast("pipelining", new HttpPipeliningHandler(transport.pipeliningMaxEvents));
            }
            pipeline.addLast("handler", requestHandler);
            return pipeline;
        }
    }
}
