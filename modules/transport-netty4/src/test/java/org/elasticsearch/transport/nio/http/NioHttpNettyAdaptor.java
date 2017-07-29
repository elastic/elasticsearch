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


package org.elasticsearch.transport.nio.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.http.netty4.cors.Netty4CorsHandler;
import org.elasticsearch.http.netty4.pipelining.HttpPipeliningHandler;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;

public class NioHttpNettyAdaptor {


    private final NioHttpTransport transport;
    private final Netty4CorsConfig config;
    private final int maxContentLength;
    private final NioHttpRequestHandler requestHandler;
    private final Netty4CorsConfig corsConfig;
    private final boolean compression;
    private final int compressionLevel;
    private final boolean detailedErrorsEnabled;
    private final boolean corsEnabled;
    private boolean pipelining;
    private final int pipeliningMaxEvents;
    private final int maxChunkSize;
    private final int maxHeaderSize;
    private final int maxInitialLineLength;

    protected NioHttpNettyAdaptor(NioHttpTransport transport, NamedXContentRegistry xContentRegistry, ThreadContext threadContext,
                                  Settings settings, Netty4CorsConfig config, int maxContentLength) {
        this.transport = transport;
        this.config = config;
        this.maxContentLength = maxContentLength;
        this.detailedErrorsEnabled = SETTING_HTTP_DETAILED_ERRORS_ENABLED.get(settings);

        this.requestHandler = new NioHttpRequestHandler(transport, xContentRegistry, detailedErrorsEnabled, threadContext, pipelining);

        this.compression = SETTING_HTTP_COMPRESSION.get(settings);
        this.compressionLevel = SETTING_HTTP_COMPRESSION_LEVEL.get(settings);
        this.pipelining = SETTING_PIPELINING.get(settings);
        this.pipeliningMaxEvents = SETTING_PIPELINING_MAX_EVENTS.get(settings);
        this.corsConfig = config;
        this.maxChunkSize = Math.toIntExact(SETTING_HTTP_MAX_CHUNK_SIZE.get(settings).getBytes());
        this.maxHeaderSize = Math.toIntExact(SETTING_HTTP_MAX_HEADER_SIZE.get(settings).getBytes());
        this.maxInitialLineLength = Math.toIntExact(SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings).getBytes());
        this.corsEnabled = SETTING_CORS_ENABLED.get(settings);
    }

    protected void initChannel(NioSocketChannel channel) throws Exception {
        pipelining = false;

        EmbeddedChannel ch = new ESNettyChannel(channel);

        final HttpRequestDecoder decoder = new HttpRequestDecoder(maxInitialLineLength, maxHeaderSize, maxChunkSize);
        ch.pipeline().addLast(decoder);
        ch.pipeline().addLast(new HttpContentDecompressor());
        ch.pipeline().addLast(new HttpResponseEncoder());
        decoder.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
        final HttpObjectAggregator aggregator = new HttpObjectAggregator(maxContentLength);
//        if (maxCompositeBufferComponents != -1) {
//            aggregator.setMaxCumulationBufferComponents(maxCompositeBufferComponents);
//        }
        ch.pipeline().addLast(aggregator);
        if (compression) {
            ch.pipeline().addLast("encoder_compress", new HttpContentCompressor(compressionLevel));
        }
        if (corsConfig.isCorsSupportEnabled()) {
            ch.pipeline().addLast("cors", new Netty4CorsHandler(config));
        }
        if (pipelining) {
            ch.pipeline().addLast("pipelining", new HttpPipeliningHandler(pipeliningMaxEvents));
        }
        ch.pipeline().addLast("handler", requestHandler);
    }

    public static class ESNettyChannel extends EmbeddedChannel {

        private final NioSocketChannel nioSocketChannel;

        private ESNettyChannel(NioSocketChannel nioSocketChannel) {
            this.nioSocketChannel = nioSocketChannel;
        }

        public NioSocketChannel getNioSocketChannel() {
            return nioSocketChannel;
        }
    }
}
