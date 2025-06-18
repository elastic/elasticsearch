/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.bootstrap.ServerBootstrap;
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
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.flow.FlowControlHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;
import io.netty.util.ResourceLeakDetector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.ThreadWatchdog;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.http.AbstractHttpServerTransport;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpReadTimeoutException;
import org.elasticsearch.http.HttpServerChannel;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty4.internal.HttpHeadersAuthenticatorUtils;
import org.elasticsearch.http.netty4.internal.HttpValidator;
import org.elasticsearch.rest.ChunkedZipResponse;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.AcceptChannelHandler;
import org.elasticsearch.transport.netty4.NetUtils;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.transport.netty4.Netty4WriteThrottlingHandler;
import org.elasticsearch.transport.netty4.NettyAllocator;
import org.elasticsearch.transport.netty4.NettyByteBufSizer;
import org.elasticsearch.transport.netty4.SSLExceptionHelper;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_ALIVE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_COUNT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_IDLE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_INTERVAL;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_NO_DELAY;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_REUSE_ADDRESS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_SEND_BUFFER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;

public class Netty4HttpServerTransport extends AbstractHttpServerTransport {
    private static final Logger logger = LogManager.getLogger(Netty4HttpServerTransport.class);

    private final int pipeliningMaxEvents;

    private final SharedGroupFactory sharedGroupFactory;
    private final RecvByteBufAllocator recvByteBufAllocator;
    private final TLSConfig tlsConfig;
    private final AcceptChannelHandler.AcceptPredicate acceptChannelPredicate;
    private final HttpValidator httpValidator;
    private final IncrementalBulkService.Enabled enabled;
    private final ThreadWatchdog threadWatchdog;
    private final int readTimeoutMillis;

    private final int maxCompositeBufferComponents;

    private volatile ServerBootstrap serverBootstrap;
    private volatile SharedGroupFactory.SharedGroup sharedGroup;

    public Netty4HttpServerTransport(
        Settings settings,
        NetworkService networkService,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        SharedGroupFactory sharedGroupFactory,
        Tracer tracer,
        TLSConfig tlsConfig,
        @Nullable AcceptChannelHandler.AcceptPredicate acceptChannelPredicate,
        @Nullable HttpValidator httpValidator
    ) {
        super(
            settings,
            networkService,
            Netty4Utils.createRecycler(settings),
            threadPool,
            xContentRegistry,
            dispatcher,
            clusterSettings,
            tracer
        );
        Netty4Utils.setAvailableProcessors(EsExecutors.allocatedProcessors(settings));
        NettyAllocator.logAllocatorDescriptionIfNeeded();
        this.sharedGroupFactory = sharedGroupFactory;
        this.tlsConfig = tlsConfig;
        this.acceptChannelPredicate = acceptChannelPredicate;
        this.httpValidator = httpValidator;
        this.threadWatchdog = networkService.getThreadWatchdog();
        this.enabled = new IncrementalBulkService.Enabled(clusterSettings);

        this.pipeliningMaxEvents = SETTING_PIPELINING_MAX_EVENTS.get(settings);

        this.maxCompositeBufferComponents = Netty4Plugin.SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS.get(settings);

        this.readTimeoutMillis = Math.toIntExact(SETTING_HTTP_READ_TIMEOUT.get(settings).getMillis());

        ByteSizeValue receivePredictor = Netty4Plugin.SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE.get(settings);
        recvByteBufAllocator = new FixedRecvByteBufAllocator(receivePredictor.bytesAsInt());

        logger.debug(
            "using max_chunk_size[{}], max_header_size[{}], max_initial_line_length[{}], max_content_length[{}], "
                + "receive_predictor[{}], max_composite_buffer_components[{}], pipelining_max_events[{}]",
            SETTING_HTTP_MAX_CHUNK_SIZE.get(settings),
            SETTING_HTTP_MAX_HEADER_SIZE.get(settings),
            SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings),
            maxContentLength,
            receivePredictor,
            maxCompositeBufferComponents,
            pipeliningMaxEvents
        );
    }

    public Settings settings() {
        return this.settings;
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            sharedGroup = sharedGroupFactory.getHttpGroup();
            serverBootstrap = new ServerBootstrap();

            serverBootstrap.group(sharedGroup.getLowLevelGroup());

            // NettyAllocator will return the channel type designed to work with the configuredAllocator
            serverBootstrap.channel(NettyAllocator.getServerChannelType());

            // Set the allocators for both the server channel and the child channels created
            serverBootstrap.option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator());
            serverBootstrap.childOption(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator());

            serverBootstrap.childHandler(configureServerChannelHandler());
            serverBootstrap.handler(ServerChannelExceptionHandler.INSTANCE);

            serverBootstrap.childOption(ChannelOption.TCP_NODELAY, SETTING_HTTP_TCP_NO_DELAY.get(settings));
            serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, SETTING_HTTP_TCP_KEEP_ALIVE.get(settings));

            if (SETTING_HTTP_TCP_KEEP_ALIVE.get(settings)) {
                // Netty logs a warning if it can't set the option, so try this only on supported platforms
                if (IOUtils.LINUX || IOUtils.MAC_OS_X) {
                    if (SETTING_HTTP_TCP_KEEP_IDLE.get(settings) >= 0) {
                        serverBootstrap.childOption(
                            NioChannelOption.of(NetUtils.getTcpKeepIdleSocketOption()),
                            SETTING_HTTP_TCP_KEEP_IDLE.get(settings)
                        );
                    }
                    if (SETTING_HTTP_TCP_KEEP_INTERVAL.get(settings) >= 0) {
                        serverBootstrap.childOption(
                            NioChannelOption.of(NetUtils.getTcpKeepIntervalSocketOption()),
                            SETTING_HTTP_TCP_KEEP_INTERVAL.get(settings)
                        );
                    }
                    if (SETTING_HTTP_TCP_KEEP_COUNT.get(settings) >= 0) {
                        serverBootstrap.childOption(
                            NioChannelOption.of(NetUtils.getTcpKeepCountSocketOption()),
                            SETTING_HTTP_TCP_KEEP_COUNT.get(settings)
                        );
                    }
                }
            }

            final ByteSizeValue tcpSendBufferSize = SETTING_HTTP_TCP_SEND_BUFFER_SIZE.get(settings);
            if (tcpSendBufferSize.getBytes() > 0) {
                serverBootstrap.childOption(ChannelOption.SO_SNDBUF, Math.toIntExact(tcpSendBufferSize.getBytes()));
            }

            final ByteSizeValue tcpReceiveBufferSize = SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE.get(settings);
            if (tcpReceiveBufferSize.getBytes() > 0) {
                serverBootstrap.childOption(ChannelOption.SO_RCVBUF, Math.toIntExact(tcpReceiveBufferSize.getBytes()));
            }

            serverBootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);
            serverBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);

            final boolean reuseAddress = SETTING_HTTP_TCP_REUSE_ADDRESS.get(settings);
            serverBootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);
            serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, reuseAddress);

            bindServer();
            if (acceptChannelPredicate != null) {
                acceptChannelPredicate.setBoundAddress(boundAddress());
            }
            success = true;
        } finally {
            if (success == false) {
                doStop(); // otherwise we leak threads since we never moved to started
            }
        }
    }

    @Override
    protected HttpServerChannel bind(InetSocketAddress socketAddress) throws Exception {
        ChannelFuture future = serverBootstrap.bind(socketAddress).sync();
        Channel channel = future.channel();
        Netty4HttpServerChannel httpServerChannel = new Netty4HttpServerChannel(channel);
        channel.attr(HTTP_SERVER_CHANNEL_KEY).set(httpServerChannel);
        return httpServerChannel;
    }

    @Override
    protected void stopInternal() {
        if (sharedGroup != null) {
            sharedGroup.shutdown();
            sharedGroup = null;
        }
    }

    @Override
    public void onException(HttpChannel channel, Exception cause) {
        if (lifecycle.started() == false) {
            return;
        }

        if (SSLExceptionHelper.isNotSslRecordException(cause)) {
            logger.warn("received plaintext http traffic on an https channel, closing connection {}", channel);
            CloseableChannel.closeChannel(channel);
        } else if (SSLExceptionHelper.isCloseDuringHandshakeException(cause)) {
            logger.debug("connection {} closed during ssl handshake", channel);
            CloseableChannel.closeChannel(channel);
        } else if (SSLExceptionHelper.isInsufficientBufferRemainingException(cause)) {
            logger.debug("connection {} closed abruptly", channel);
            CloseableChannel.closeChannel(channel);
        } else if (SSLExceptionHelper.isReceivedCertificateUnknownException(cause)) {
            logger.warn("http client did not trust this server's certificate, closing connection {}", channel);
            CloseableChannel.closeChannel(channel);
        } else if (cause instanceof ReadTimeoutException) {
            super.onException(channel, new HttpReadTimeoutException(readTimeoutMillis, cause));
        } else {
            super.onException(channel, cause);
        }
    }

    public ChannelHandler configureServerChannelHandler() {
        return new HttpChannelHandler(this, handlingSettings, tlsConfig, acceptChannelPredicate, httpValidator, enabled);
    }

    static final AttributeKey<Netty4HttpChannel> HTTP_CHANNEL_KEY = AttributeKey.newInstance("es-http-channel");
    static final AttributeKey<Netty4HttpServerChannel> HTTP_SERVER_CHANNEL_KEY = AttributeKey.newInstance("es-http-server-channel");

    protected static class HttpChannelHandler extends ChannelInitializer<Channel> {

        private final Netty4HttpServerTransport transport;
        private final HttpHandlingSettings handlingSettings;
        private final TLSConfig tlsConfig;
        private final BiPredicate<String, InetSocketAddress> acceptChannelPredicate;
        private final HttpValidator httpValidator;
        private final IncrementalBulkService.Enabled enabled;

        protected HttpChannelHandler(
            final Netty4HttpServerTransport transport,
            final HttpHandlingSettings handlingSettings,
            final TLSConfig tlsConfig,
            @Nullable final BiPredicate<String, InetSocketAddress> acceptChannelPredicate,
            @Nullable final HttpValidator httpValidator,
            IncrementalBulkService.Enabled enabled
        ) {
            this.transport = transport;
            this.handlingSettings = handlingSettings;
            this.tlsConfig = tlsConfig;
            this.acceptChannelPredicate = acceptChannelPredicate;
            this.httpValidator = httpValidator;
            this.enabled = enabled;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            // auto-read must be disabled all the time
            ch.config().setAutoRead(false);

            Netty4HttpChannel nettyHttpChannel = new Netty4HttpChannel(ch);
            ch.attr(HTTP_CHANNEL_KEY).set(nettyHttpChannel);
            if (acceptChannelPredicate != null) {
                ch.pipeline()
                    .addLast(
                        "accept_channel_handler",
                        new AcceptChannelHandler(
                            acceptChannelPredicate,
                            HttpServerTransport.HTTP_PROFILE_NAME,
                            transport.getThreadPool().getThreadContext()
                        )
                    );
            }
            if (tlsConfig.isTLSEnabled()) {
                ch.pipeline().addLast("ssl", new SslHandler(tlsConfig.createServerSSLEngine()));
            }
            final var threadWatchdogActivityTracker = transport.threadWatchdog.getActivityTrackerForCurrentThread();
            ch.pipeline()
                .addLast(
                    "chunked_writer",
                    new Netty4WriteThrottlingHandler(transport.getThreadPool().getThreadContext(), threadWatchdogActivityTracker)
                )
                .addLast("byte_buf_sizer", NettyByteBufSizer.INSTANCE);
            if (transport.readTimeoutMillis > 0) {
                ch.pipeline().addLast("read_timeout", new ReadTimeoutHandler(transport.readTimeoutMillis, TimeUnit.MILLISECONDS));
            }
            final HttpRequestDecoder decoder;
            if (httpValidator != null) {
                decoder = new HttpRequestDecoder(
                    handlingSettings.maxInitialLineLength(),
                    handlingSettings.maxHeaderSize(),
                    handlingSettings.maxChunkSize()
                ) {
                    @Override
                    protected HttpMessage createMessage(String[] initialLine) throws Exception {
                        return HttpHeadersAuthenticatorUtils.wrapAsMessageWithAuthenticationContext(super.createMessage(initialLine));
                    }
                };
            } else {
                decoder = new HttpRequestDecoder(
                    handlingSettings.maxInitialLineLength(),
                    handlingSettings.maxHeaderSize(),
                    handlingSettings.maxChunkSize()
                );
            }
            decoder.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
            ch.pipeline().addLast("decoder", decoder); // parses the HTTP bytes request into HTTP message pieces

            // from this point in pipeline every handler must call ctx or channel #read() when ready to process next HTTP part
            if (Assertions.ENABLED) {
                // missing reads are hard to catch, but we can detect absence of reads within interval
                long missingReadIntervalMs = 10_000;
                ch.pipeline().addLast(new MissingReadDetector(transport.threadPool, missingReadIntervalMs));
            }

            if (httpValidator != null) {
                // runs a validation function on the first HTTP message piece which contains all the headers
                // if validation passes, the pieces of that particular request are forwarded, otherwise they are discarded
                ch.pipeline()
                    .addLast(
                        "header_validator",
                        HttpHeadersAuthenticatorUtils.getValidatorInboundHandler(
                            httpValidator,
                            transport.getThreadPool().getThreadContext()
                        )
                    );
            }
            // combines the HTTP message pieces into a single full HTTP request (with headers and body)
            final HttpObjectAggregator aggregator = new Netty4HttpAggregator(
                handlingSettings.maxContentLength(),
                httpPreRequest -> enabled.get() == false
                    || ((httpPreRequest.rawPath().endsWith("/_bulk") == false)
                        || httpPreRequest.rawPath().startsWith("/_xpack/monitoring/_bulk")),
                decoder
            );
            aggregator.setMaxCumulationBufferComponents(transport.maxCompositeBufferComponents);
            ch.pipeline()
                .addLast("decoder_compress", new HttpContentDecompressor()) // this handles request body decompression
                .addLast("encoder", new HttpResponseEncoder() {
                    @Override
                    protected boolean isContentAlwaysEmpty(HttpResponse msg) {
                        // non-chunked responses (Netty4HttpResponse extends Netty's DefaultFullHttpResponse) with chunked transfer
                        // encoding are only sent by us in response to HEAD requests and must always have an empty body
                        if (msg instanceof Netty4FullHttpResponse netty4FullHttpResponse && HttpUtil.isTransferEncodingChunked(msg)) {
                            assert netty4FullHttpResponse.content().isReadable() == false;
                            return true;
                        }
                        return super.isContentAlwaysEmpty(msg);
                    }
                })
                .addLast("aggregator", aggregator);
            if (handlingSettings.compression()) {
                ch.pipeline().addLast("encoder_compress", new HttpContentCompressor(handlingSettings.compressionLevel()) {
                    @Override
                    protected Result beginEncode(HttpResponse httpResponse, String acceptEncoding) throws Exception {
                        if (ChunkedZipResponse.ZIP_CONTENT_TYPE.equals(httpResponse.headers().get("content-type"))) {
                            return null;
                        } else {
                            return super.beginEncode(httpResponse, acceptEncoding);
                        }
                    }
                });
            }
            if (ResourceLeakDetector.isEnabled()) {
                ch.pipeline().addLast(new Netty4LeakDetectionHandler());
            }
            // See https://github.com/netty/netty/issues/15053: the combination of FlowControlHandler and HttpContentDecompressor above
            // can emit multiple chunks per read, but HttpBody.Stream requires chunks to arrive one-at-a-time so until that issue is
            // resolved we must add another flow controller here:
            ch.pipeline().addLast(new FlowControlHandler());
            ch.pipeline()
                .addLast(
                    "pipelining",
                    new Netty4HttpPipeliningHandler(transport.pipeliningMaxEvents, transport, threadWatchdogActivityTracker)
                );
            transport.serverAcceptedChannel(nettyHttpChannel);

            // make very first read call, since auto-read is disabled; following reads must come from the handlers
            ch.read();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            super.exceptionCaught(ctx, cause);
        }
    }

    @ChannelHandler.Sharable
    private static class ServerChannelExceptionHandler extends ChannelInboundHandlerAdapter {

        static final ServerChannelExceptionHandler INSTANCE = new ServerChannelExceptionHandler();

        private ServerChannelExceptionHandler() {}

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            Netty4HttpServerChannel httpServerChannel = ctx.channel().attr(HTTP_SERVER_CHANNEL_KEY).get();
            if (cause instanceof Error) {
                AbstractHttpServerTransport.onServerException(httpServerChannel, new Exception(cause));
            } else {
                AbstractHttpServerTransport.onServerException(httpServerChannel, (Exception) cause);
            }
        }
    }
}
