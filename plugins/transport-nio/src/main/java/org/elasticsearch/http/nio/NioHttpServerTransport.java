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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.AbstractHttpServerTransport;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpServerChannel;
import org.elasticsearch.http.nio.cors.NioCorsConfig;
import org.elasticsearch.http.nio.cors.NioCorsConfigBuilder;
import org.elasticsearch.nio.BytesChannelContext;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.EventHandler;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_MAX_AGE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_ALIVE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_NO_DELAY;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_REUSE_ADDRESS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_SEND_BUFFER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;
import static org.elasticsearch.http.nio.cors.NioCorsHandler.ANY_ORIGIN;

public class NioHttpServerTransport extends AbstractHttpServerTransport {
    private static final Logger logger = LogManager.getLogger(NioHttpServerTransport.class);

    public static final Setting<Integer> NIO_HTTP_ACCEPTOR_COUNT =
        intSetting("http.nio.acceptor_count", 1, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> NIO_HTTP_WORKER_COUNT =
        new Setting<>("http.nio.worker_count",
            (s) -> Integer.toString(EsExecutors.numberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "http.nio.worker_count"), Setting.Property.NodeScope);

    protected final PageCacheRecycler pageCacheRecycler;
    protected final NioCorsConfig corsConfig;

    protected final boolean tcpNoDelay;
    protected final boolean tcpKeepAlive;
    protected final boolean reuseAddress;
    protected final int tcpSendBufferSize;
    protected final int tcpReceiveBufferSize;

    private NioGroup nioGroup;
    private ChannelFactory<NioHttpServerChannel, NioHttpChannel> channelFactory;

    public NioHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays,
                                  PageCacheRecycler pageCacheRecycler, ThreadPool threadPool, NamedXContentRegistry xContentRegistry,
                                  Dispatcher dispatcher) {
        super(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher);
        this.pageCacheRecycler = pageCacheRecycler;

        ByteSizeValue maxChunkSize = SETTING_HTTP_MAX_CHUNK_SIZE.get(settings);
        ByteSizeValue maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.get(settings);
        ByteSizeValue maxInitialLineLength = SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings);
        int pipeliningMaxEvents = SETTING_PIPELINING_MAX_EVENTS.get(settings);
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
                (s) -> new EventHandler(this::onNonChannelException, s));
            channelFactory = channelFactory();
            bindServer();
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
    protected void stopInternal() {
        try {
            nioGroup.close();
        } catch (Exception e) {
            logger.warn("unexpected exception while stopping nio group", e);
        }
    }

    @Override
    protected HttpServerChannel bind(InetSocketAddress socketAddress) throws IOException {
        return nioGroup.bindServerChannel(socketAddress, channelFactory);
    }

    protected ChannelFactory<NioHttpServerChannel, NioHttpChannel> channelFactory() {
        return new HttpChannelFactory();
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
            try {
                Pattern p = RestUtils.checkCorsSettingForRegex(origin);
                if (p == null) {
                    builder = NioCorsConfigBuilder.forOrigins(RestUtils.corsSettingAsArray(origin));
                } else {
                    builder = NioCorsConfigBuilder.forPattern(p);
                }
            } catch (PatternSyntaxException e) {
                throw new SettingsException("Bad regex in [" + SETTING_CORS_ALLOW_ORIGIN.getKey() + "]: [" + origin + "]", e);
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

    protected void acceptChannel(NioSocketChannel socketChannel) {
        super.serverAcceptedChannel((HttpChannel) socketChannel);
    }

    private class HttpChannelFactory extends ChannelFactory<NioHttpServerChannel, NioHttpChannel> {

        private HttpChannelFactory() {
            super(new RawChannelFactory(tcpNoDelay, tcpKeepAlive, reuseAddress, tcpSendBufferSize, tcpReceiveBufferSize));
        }

        @Override
        public NioHttpChannel createChannel(NioSelector selector, SocketChannel channel) throws IOException {
            NioHttpChannel httpChannel = new NioHttpChannel(channel);
            java.util.function.Supplier<InboundChannelBuffer.Page> pageSupplier = () -> {
                Recycler.V<byte[]> bytes = pageCacheRecycler.bytePage(false);
                return new InboundChannelBuffer.Page(ByteBuffer.wrap(bytes.v()), bytes::close);
            };
            HttpReadWriteHandler httpReadWritePipeline = new HttpReadWriteHandler(httpChannel,NioHttpServerTransport.this,
                handlingSettings, corsConfig);
            Consumer<Exception> exceptionHandler = (e) -> onException(httpChannel, e);
            SocketChannelContext context = new BytesChannelContext(httpChannel, selector, exceptionHandler, httpReadWritePipeline,
                new InboundChannelBuffer(pageSupplier));
            httpChannel.setContext(context);
            return httpChannel;
        }

        @Override
        public NioHttpServerChannel createServerChannel(NioSelector selector, ServerSocketChannel channel) throws IOException {
            NioHttpServerChannel httpServerChannel = new NioHttpServerChannel(channel);
            Consumer<Exception> exceptionHandler = (e) -> onServerException(httpServerChannel, e);
            Consumer<NioSocketChannel> acceptor = NioHttpServerTransport.this::acceptChannel;
            ServerChannelContext context = new ServerChannelContext(httpServerChannel, this, selector, acceptor, exceptionHandler);
            httpServerChannel.setContext(context);
            return httpServerChannel;
        }

    }
}
