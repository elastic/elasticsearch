/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.core.internal.net.NetUtils;
import org.elasticsearch.http.AbstractHttpServerTransport;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpReadTimeoutException;
import org.elasticsearch.http.HttpServerChannel;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NettyAllocator;
import org.elasticsearch.transport.NettyByteBufSizer;
import org.elasticsearch.transport.SharedGroupFactory;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
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

    /*
     * Size in bytes of an individual message received by io.netty.handler.codec.MessageAggregator which accumulates the content for an
     * HTTP request. This number is used for estimating the maximum number of allowed buffers before the MessageAggregator's internal
     * collection of buffers is resized.
     *
     * By default we assume the Ethernet MTU (1500 bytes) but users can override it with a system property.
     */
    private static final ByteSizeValue MTU = new ByteSizeValue(Long.parseLong(System.getProperty("es.net.mtu", "1500")));

    private static final String SETTING_KEY_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS = "http.netty.max_composite_buffer_components";

    public static Setting<Integer> SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS =
        new Setting<>(SETTING_KEY_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS, (s) -> {
            ByteSizeValue maxContentLength = SETTING_HTTP_MAX_CONTENT_LENGTH.get(s);
            /*
             * Netty accumulates buffers containing data from all incoming network packets that make up one HTTP request in an instance of
             * io.netty.buffer.CompositeByteBuf (think of it as a buffer of buffers). Once its capacity is reached, the buffer will iterate
             * over its individual entries and put them into larger buffers (see io.netty.buffer.CompositeByteBuf#consolidateIfNeeded()
             * for implementation details). We want to to resize that buffer because this leads to additional garbage on the heap and also
             * increases the application's native memory footprint (as direct byte buffers hold their contents off-heap).
             *
             * With this setting we control the CompositeByteBuf's capacity (which is by default 1024, see
             * io.netty.handler.codec.MessageAggregator#DEFAULT_MAX_COMPOSITEBUFFER_COMPONENTS). To determine a proper default capacity for
             * that buffer, we need to consider that the upper bound for the size of HTTP requests is determined by `maxContentLength`. The
             * number of buffers that are needed depend on how often Netty reads network packets which depends on the network type (MTU).
             * We assume here that Elasticsearch receives HTTP requests via an Ethernet connection which has a MTU of 1500 bytes.
             *
             * Note that we are *not* pre-allocating any memory based on this setting but rather determine the CompositeByteBuf's capacity.
             * The tradeoff is between less (but larger) buffers that are contained in the CompositeByteBuf and more (but smaller) buffers.
             * With the default max content length of 100MB and a MTU of 1500 bytes we would allow 69905 entries.
             */
            long maxBufferComponentsEstimate = Math.round((double) (maxContentLength.getBytes() / MTU.getBytes()));
            // clamp value to the allowed range
            long maxBufferComponents = Math.max(2, Math.min(maxBufferComponentsEstimate, Integer.MAX_VALUE));
            return String.valueOf(maxBufferComponents);
            // Netty's CompositeByteBuf implementation does not allow less than two components.
        }, s -> Setting.parseInt(s, 2, Integer.MAX_VALUE, SETTING_KEY_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS), Property.NodeScope);

    public static final Setting<Integer> SETTING_HTTP_WORKER_COUNT = Setting.intSetting("http.netty.worker_count", 0, Property.NodeScope);

    public static final Setting<ByteSizeValue> SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE =
        Setting.byteSizeSetting("http.netty.receive_predictor_size", new ByteSizeValue(64, ByteSizeUnit.KB), Property.NodeScope);

    private final ByteSizeValue maxInitialLineLength;
    private final ByteSizeValue maxHeaderSize;
    private final ByteSizeValue maxChunkSize;

    private final int pipeliningMaxEvents;

    private final SharedGroupFactory sharedGroupFactory;
    private final RecvByteBufAllocator recvByteBufAllocator;
    private final int readTimeoutMillis;

    private final int maxCompositeBufferComponents;

    private volatile ServerBootstrap serverBootstrap;
    private volatile SharedGroupFactory.SharedGroup sharedGroup;

    public Netty4HttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays, ThreadPool threadPool,
                                     NamedXContentRegistry xContentRegistry, Dispatcher dispatcher, ClusterSettings clusterSettings,
                                     SharedGroupFactory sharedGroupFactory) {
        super(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher, clusterSettings);
        Netty4Utils.setAvailableProcessors(EsExecutors.NODE_PROCESSORS_SETTING.get(settings));
        NettyAllocator.logAllocatorDescriptionIfNeeded();
        this.sharedGroupFactory = sharedGroupFactory;

        this.maxChunkSize = SETTING_HTTP_MAX_CHUNK_SIZE.get(settings);
        this.maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.get(settings);
        this.maxInitialLineLength = SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings);
        this.pipeliningMaxEvents = SETTING_PIPELINING_MAX_EVENTS.get(settings);

        this.maxCompositeBufferComponents = SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS.get(settings);

        this.readTimeoutMillis = Math.toIntExact(SETTING_HTTP_READ_TIMEOUT.get(settings).getMillis());

        ByteSizeValue receivePredictor = SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE.get(settings);
        recvByteBufAllocator = new FixedRecvByteBufAllocator(receivePredictor.bytesAsInt());

        logger.debug("using max_chunk_size[{}], max_header_size[{}], max_initial_line_length[{}], max_content_length[{}], " +
                "receive_predictor[{}], max_composite_buffer_components[{}], pipelining_max_events[{}]",
            maxChunkSize, maxHeaderSize, maxInitialLineLength, maxContentLength, receivePredictor, maxCompositeBufferComponents,
            pipeliningMaxEvents);
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
            serverBootstrap.handler(new ServerChannelExceptionHandler(this));

            serverBootstrap.childOption(ChannelOption.TCP_NODELAY, SETTING_HTTP_TCP_NO_DELAY.get(settings));
            serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, SETTING_HTTP_TCP_KEEP_ALIVE.get(settings));

            if (SETTING_HTTP_TCP_KEEP_ALIVE.get(settings)) {
                // Netty logs a warning if it can't set the option, so try this only on supported platforms
                if (IOUtils.LINUX || IOUtils.MAC_OS_X) {
                    if (SETTING_HTTP_TCP_KEEP_IDLE.get(settings) >= 0) {
                        final SocketOption<Integer> keepIdleOption = NetUtils.getTcpKeepIdleSocketOptionOrNull();
                        if (keepIdleOption != null) {
                            serverBootstrap.childOption(NioChannelOption.of(keepIdleOption), SETTING_HTTP_TCP_KEEP_IDLE.get(settings));
                        }
                    }
                    if (SETTING_HTTP_TCP_KEEP_INTERVAL.get(settings) >= 0) {
                        final SocketOption<Integer> keepIntervalOption = NetUtils.getTcpKeepIntervalSocketOptionOrNull();
                        if (keepIntervalOption != null) {
                            serverBootstrap.childOption(NioChannelOption.of(keepIntervalOption),
                                SETTING_HTTP_TCP_KEEP_INTERVAL.get(settings));
                        }
                    }
                    if (SETTING_HTTP_TCP_KEEP_COUNT.get(settings) >= 0) {
                        final SocketOption<Integer> keepCountOption = NetUtils.getTcpKeepCountSocketOptionOrNull();
                        if (keepCountOption != null) {
                            serverBootstrap.childOption(NioChannelOption.of(keepCountOption), SETTING_HTTP_TCP_KEEP_COUNT.get(settings));
                        }
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
        if (cause instanceof ReadTimeoutException) {
            super.onException(channel, new HttpReadTimeoutException(readTimeoutMillis, cause));
        } else {
            super.onException(channel, cause);
        }
    }

    public ChannelHandler configureServerChannelHandler() {
        return new HttpChannelHandler(this, handlingSettings);
    }

    static final AttributeKey<Netty4HttpChannel> HTTP_CHANNEL_KEY = AttributeKey.newInstance("es-http-channel");
    static final AttributeKey<Netty4HttpServerChannel> HTTP_SERVER_CHANNEL_KEY = AttributeKey.newInstance("es-http-server-channel");

    protected static class HttpChannelHandler extends ChannelInitializer<Channel> {

        private final Netty4HttpServerTransport transport;
        private final Netty4HttpRequestCreator requestCreator;
        private final Netty4HttpRequestHandler requestHandler;
        private final Netty4HttpResponseCreator responseCreator;
        private final HttpHandlingSettings handlingSettings;

        protected HttpChannelHandler(final Netty4HttpServerTransport transport, final HttpHandlingSettings handlingSettings) {
            this.transport = transport;
            this.handlingSettings = handlingSettings;
            this.requestCreator =  new Netty4HttpRequestCreator();
            this.requestHandler = new Netty4HttpRequestHandler(transport);
            this.responseCreator = new Netty4HttpResponseCreator();
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            Netty4HttpChannel nettyHttpChannel = new Netty4HttpChannel(ch);
            ch.attr(HTTP_CHANNEL_KEY).set(nettyHttpChannel);
            ch.pipeline().addLast("byte_buf_sizer", NettyByteBufSizer.INSTANCE);
            ch.pipeline().addLast("read_timeout", new ReadTimeoutHandler(transport.readTimeoutMillis, TimeUnit.MILLISECONDS));
            final HttpRequestDecoder decoder = new HttpRequestDecoder(
                handlingSettings.getMaxInitialLineLength(),
                handlingSettings.getMaxHeaderSize(),
                handlingSettings.getMaxChunkSize());
            decoder.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
            ch.pipeline().addLast("decoder", decoder);
            ch.pipeline().addLast("decoder_compress", new HttpContentDecompressor());
            ch.pipeline().addLast("encoder", new HttpResponseEncoder());
            final HttpObjectAggregator aggregator = new HttpObjectAggregator(handlingSettings.getMaxContentLength());
            aggregator.setMaxCumulationBufferComponents(transport.maxCompositeBufferComponents);
            ch.pipeline().addLast("aggregator", aggregator);
            if (handlingSettings.isCompression()) {
                ch.pipeline().addLast("encoder_compress", new HttpContentCompressor(handlingSettings.getCompressionLevel()));
            }
            ch.pipeline().addLast("request_creator", requestCreator);
            ch.pipeline().addLast("response_creator", responseCreator);
            ch.pipeline().addLast("pipelining", new Netty4HttpPipeliningHandler(logger, transport.pipeliningMaxEvents));
            ch.pipeline().addLast("handler", requestHandler);
            transport.serverAcceptedChannel(nettyHttpChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            super.exceptionCaught(ctx, cause);
        }
    }

    @ChannelHandler.Sharable
    private static class ServerChannelExceptionHandler extends ChannelInboundHandlerAdapter {

        private final Netty4HttpServerTransport transport;

        private ServerChannelExceptionHandler(Netty4HttpServerTransport transport) {
            this.transport = transport;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            Netty4HttpServerChannel httpServerChannel = ctx.channel().attr(HTTP_SERVER_CHANNEL_KEY).get();
            if (cause instanceof Error) {
                transport.onServerException(httpServerChannel, new Exception(cause));
            } else {
                transport.onServerException(httpServerChannel, (Exception) cause);
            }
        }
    }
}
