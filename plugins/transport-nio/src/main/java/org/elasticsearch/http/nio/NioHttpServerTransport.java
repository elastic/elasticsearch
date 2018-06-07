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

package org.elasticsearch.http.nio;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.timeout.ReadTimeoutException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.AbstractHttpServerTransport;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.http.nio.cors.NioCorsConfig;
import org.elasticsearch.http.nio.cors.NioCorsConfigBuilder;
import org.elasticsearch.nio.BytesChannelContext;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.EventHandler;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioChannel;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioServerSocketChannel;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_MAX_AGE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_RESET_COOKIES;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_ALIVE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_NO_DELAY;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_REUSE_ADDRESS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_SEND_BUFFER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;
import static org.elasticsearch.http.nio.cors.NioCorsHandler.ANY_ORIGIN;

public class NioHttpServerTransport extends AbstractHttpServerTransport {

    public static final Setting<Integer> NIO_HTTP_ACCEPTOR_COUNT =
        intSetting("http.nio.acceptor_count", 1, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> NIO_HTTP_WORKER_COUNT =
        new Setting<>("http.nio.worker_count",
            (s) -> Integer.toString(EsExecutors.numberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "http.nio.worker_count"), Setting.Property.NodeScope);

    private final BigArrays bigArrays;
    private final ThreadPool threadPool;
    private final NamedXContentRegistry xContentRegistry;

    private final HttpHandlingSettings httpHandlingSettings;

    private final boolean tcpNoDelay;
    private final boolean tcpKeepAlive;
    private final boolean reuseAddress;
    private final int tcpSendBufferSize;
    private final int tcpReceiveBufferSize;

    private final Set<NioServerSocketChannel> serverChannels = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<NioSocketChannel> socketChannels = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private NioGroup nioGroup;
    private HttpChannelFactory channelFactory;
    private final NioCorsConfig corsConfig;

    public NioHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays, ThreadPool threadPool,
                                  NamedXContentRegistry xContentRegistry, HttpServerTransport.Dispatcher dispatcher) {
        super(settings, networkService, threadPool, dispatcher);
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;

        ByteSizeValue maxChunkSize = SETTING_HTTP_MAX_CHUNK_SIZE.get(settings);
        ByteSizeValue maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.get(settings);
        ByteSizeValue maxInitialLineLength = SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings);
        int pipeliningMaxEvents = SETTING_PIPELINING_MAX_EVENTS.get(settings);
        this.httpHandlingSettings = new HttpHandlingSettings(Math.toIntExact(maxContentLength.getBytes()),
            Math.toIntExact(maxChunkSize.getBytes()),
            Math.toIntExact(maxHeaderSize.getBytes()),
            Math.toIntExact(maxInitialLineLength.getBytes()),
            SETTING_HTTP_RESET_COOKIES.get(settings),
            SETTING_HTTP_COMPRESSION.get(settings),
            SETTING_HTTP_COMPRESSION_LEVEL.get(settings),
            SETTING_HTTP_DETAILED_ERRORS_ENABLED.get(settings),
            pipeliningMaxEvents);
        this.corsConfig = buildCorsConfig(settings);

        this.tcpNoDelay = SETTING_HTTP_TCP_NO_DELAY.get(settings);
        this.tcpKeepAlive = SETTING_HTTP_TCP_KEEP_ALIVE.get(settings);
        this.reuseAddress = SETTING_HTTP_TCP_REUSE_ADDRESS.get(settings);
        this.tcpSendBufferSize = Math.toIntExact(SETTING_HTTP_TCP_SEND_BUFFER_SIZE.get(settings).getBytes());
        this.tcpReceiveBufferSize = Math.toIntExact(SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE.get(settings).getBytes());


        logger.debug("using max_chunk_size[{}], max_header_size[{}], max_initial_line_length[{}], max_content_length[{}]," +
                " pipelining_max_events[{}]",
            maxChunkSize, maxHeaderSize, maxInitialLineLength, maxContentLength, pipeliningMaxEvents);
    }

    BigArrays getBigArrays() {
        return bigArrays;
    }

    public Logger getLogger() {
        return logger;
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            int acceptorCount = NIO_HTTP_ACCEPTOR_COUNT.get(settings);
            int workerCount = NIO_HTTP_WORKER_COUNT.get(settings);
            nioGroup = new NioGroup(daemonThreadFactory(this.settings, HTTP_SERVER_ACCEPTOR_THREAD_NAME_PREFIX), acceptorCount,
                daemonThreadFactory(this.settings, HTTP_SERVER_WORKER_THREAD_NAME_PREFIX), workerCount,
                (s) -> new EventHandler(this::nonChannelExceptionCaught, s));
            channelFactory = new HttpChannelFactory();
            this.boundAddress = createBoundHttpAddress();

            if (logger.isInfoEnabled()) {
                logger.info("{}", boundAddress);
            }

            success = true;
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        } finally {
            if (success == false) {
                doStop(); // otherwise we leak threads since we never moved to started
            }
        }
    }

    @Override
    protected void doStop() {
        synchronized (serverChannels) {
            if (serverChannels.isEmpty() == false) {
                try {
                    closeChannels(new ArrayList<>(serverChannels));
                } catch (Exception e) {
                    logger.error("unexpected exception while closing http server channels", e);
                }
                serverChannels.clear();
            }
        }

        try {
            closeChannels(new ArrayList<>(socketChannels));
        } catch (Exception e) {
            logger.warn("unexpected exception while closing http channels", e);
        }
        socketChannels.clear();

        try {
            nioGroup.close();
        } catch (Exception e) {
            logger.warn("unexpected exception while stopping nio group", e);
        }
    }

    @Override
    protected void doClose() throws IOException {
    }

    @Override
    protected TransportAddress bindAddress(InetAddress hostAddress) {
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = port.iterate(portNumber -> {
            try {
                synchronized (serverChannels) {
                    InetSocketAddress address = new InetSocketAddress(hostAddress, portNumber);
                    NioServerSocketChannel channel = nioGroup.bindServerChannel(address, channelFactory);
                    serverChannels.add(channel);
                    boundSocket.set(channel.getLocalAddress());
                }
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
            return true;
        });
        if (success == false) {
            throw new BindHttpException("Failed to bind to [" + port.getPortRangeString() + "]", lastException.get());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Bound http to address {{}}", NetworkAddress.format(boundSocket.get()));
        }
        return new TransportAddress(boundSocket.get());
    }

    @Override
    public HttpStats stats() {
        return new HttpStats(serverChannels.size(), socketChannels.size());
    }

    protected void exceptionCaught(NioSocketChannel channel, Exception cause) {
        if (cause instanceof ReadTimeoutException) {
            if (logger.isTraceEnabled()) {
                logger.trace("Read timeout [{}]", channel.getRemoteAddress());
            }
            channel.close();
        } else {
            if (lifecycle.started() == false) {
                // ignore
                return;
            }
            if (NetworkExceptionHelper.isCloseConnectionException(cause) == false) {
                logger.warn(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "caught exception while handling client http traffic, closing connection {}", channel),
                    cause);
                channel.close();
            } else {
                logger.debug(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "caught exception while handling client http traffic, closing connection {}", channel),
                    cause);
                channel.close();
            }
        }
    }

    protected void nonChannelExceptionCaught(Exception ex) {
        logger.warn(new ParameterizedMessage("exception caught on transport layer [thread={}]", Thread.currentThread().getName()), ex);
    }

    static NioCorsConfig buildCorsConfig(Settings settings) {
        if (SETTING_CORS_ENABLED.get(settings) == false) {
            return NioCorsConfigBuilder.forOrigins().disable().build();
        }
        String origin = SETTING_CORS_ALLOW_ORIGIN.get(settings);
        final NioCorsConfigBuilder builder;
        if (Strings.isNullOrEmpty(origin)) {
            builder = NioCorsConfigBuilder.forOrigins();
        } else if (origin.equals(ANY_ORIGIN)) {
            builder = NioCorsConfigBuilder.forAnyOrigin();
        } else {
            Pattern p = RestUtils.checkCorsSettingForRegex(origin);
            if (p == null) {
                builder = NioCorsConfigBuilder.forOrigins(RestUtils.corsSettingAsArray(origin));
            } else {
                builder = NioCorsConfigBuilder.forPattern(p);
            }
        }
        if (SETTING_CORS_ALLOW_CREDENTIALS.get(settings)) {
            builder.allowCredentials();
        }
        String[] strMethods = Strings.tokenizeToStringArray(SETTING_CORS_ALLOW_METHODS.get(settings), ",");
        HttpMethod[] methods = Arrays.stream(strMethods)
            .map(HttpMethod::valueOf)
            .toArray(HttpMethod[]::new);
        return builder.allowedRequestMethods(methods)
            .maxAge(SETTING_CORS_MAX_AGE.get(settings))
            .allowedRequestHeaders(Strings.tokenizeToStringArray(SETTING_CORS_ALLOW_HEADERS.get(settings), ","))
            .shortCircuit()
            .build();
    }

    private void closeChannels(List<NioChannel> channels) {
        List<ActionFuture<Void>> futures = new ArrayList<>(channels.size());

        for (NioChannel channel : channels) {
            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            channel.addCloseListener(ActionListener.toBiConsumer(future));
            futures.add(future);
            channel.close();
        }

        List<RuntimeException> closeExceptions  = new ArrayList<>();
        for (ActionFuture<Void> f : futures) {
            try {
                f.actionGet();
            } catch (RuntimeException e) {
                closeExceptions.add(e);
            }
        }

        ExceptionsHelper.rethrowAndSuppress(closeExceptions);
    }

    private void acceptChannel(NioSocketChannel socketChannel) {
        socketChannels.add(socketChannel);
    }

    private class HttpChannelFactory extends ChannelFactory<NioServerSocketChannel, NioSocketChannel> {

        private HttpChannelFactory() {
            super(new RawChannelFactory(tcpNoDelay, tcpKeepAlive, reuseAddress, tcpSendBufferSize, tcpReceiveBufferSize));
        }

        @Override
        public NioSocketChannel createChannel(NioSelector selector, SocketChannel channel) throws IOException {
            NioSocketChannel nioChannel = new NioSocketChannel(channel);
            HttpReadWriteHandler httpReadWritePipeline = new HttpReadWriteHandler(nioChannel,NioHttpServerTransport.this,
                httpHandlingSettings, xContentRegistry, corsConfig, threadPool.getThreadContext());
            Consumer<Exception> exceptionHandler = (e) -> exceptionCaught(nioChannel, e);
            SocketChannelContext context = new BytesChannelContext(nioChannel, selector, exceptionHandler, httpReadWritePipeline,
                InboundChannelBuffer.allocatingInstance());
            nioChannel.setContext(context);
            return nioChannel;
        }

        @Override
        public NioServerSocketChannel createServerChannel(NioSelector selector, ServerSocketChannel channel) throws IOException {
            NioServerSocketChannel nioChannel = new NioServerSocketChannel(channel);
            Consumer<Exception> exceptionHandler = (e) -> logger.error(() ->
                new ParameterizedMessage("exception from server channel caught on transport layer [{}]", channel), e);
            Consumer<NioSocketChannel> acceptor = NioHttpServerTransport.this::acceptChannel;
            ServerChannelContext context = new ServerChannelContext(nioChannel, this, selector, acceptor, exceptionHandler);
            nioChannel.setContext(context);
            return nioChannel;
        }

    }
}
