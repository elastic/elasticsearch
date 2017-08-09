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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.http.netty4.cors.Netty4CorsHandler;
import org.elasticsearch.http.netty4.pipelining.HttpPipeliningHandler;
import org.elasticsearch.transport.netty4.ByteBufBytesReferenceTests;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.util.LinkedList;
import java.util.function.BiConsumer;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;
import static org.elasticsearch.http.netty4.Netty4HttpServerTransport.SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS;

public class NioHttpNettyAdaptorFactory {

    private final BiConsumer<NioSocketChannel, Throwable> exceptionHandler;
    private final Netty4CorsConfig corsConfig;
    private final int maxContentLength;
    private final boolean compression;
    private final int compressionLevel;
    private boolean pipelining;
    private final int pipeliningMaxEvents;
    private final int maxChunkSize;
    private final int maxHeaderSize;
    private final int maxInitialLineLength;
    private final int maxCompositeBufferComponents;

    protected NioHttpNettyAdaptorFactory(Settings settings, BiConsumer<NioSocketChannel, Throwable> exceptionHandler,
                                         Netty4CorsConfig config, int maxContentLength) {
        this.exceptionHandler = exceptionHandler;
        this.maxContentLength = maxContentLength;
        this.corsConfig = config;

        this.compression = SETTING_HTTP_COMPRESSION.get(settings);
        this.compressionLevel = SETTING_HTTP_COMPRESSION_LEVEL.get(settings);
        this.pipelining = SETTING_PIPELINING.get(settings);
        this.pipeliningMaxEvents = SETTING_PIPELINING_MAX_EVENTS.get(settings);
        this.maxChunkSize = Math.toIntExact(SETTING_HTTP_MAX_CHUNK_SIZE.get(settings).getBytes());
        this.maxHeaderSize = Math.toIntExact(SETTING_HTTP_MAX_HEADER_SIZE.get(settings).getBytes());
        this.maxInitialLineLength = Math.toIntExact(SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings).getBytes());
        this.maxCompositeBufferComponents = SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS.get(settings);
    }

    protected EmbeddedChannel getAdaptor(NioSocketChannel channel) {
        EmbeddedChannel ch = new EmbeddedChannel();
        // TODO: Implement Netty allocator that allocates our byte references
        ch.config().setAllocator(UnpooledByteBufAllocator.DEFAULT);

        final HttpRequestDecoder decoder = new HttpRequestDecoder(maxInitialLineLength, maxHeaderSize, maxChunkSize);
        decoder.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);

        ch.pipeline().addLast(decoder);
        ch.pipeline().addLast("writer", new ChannelOutboundHandlerAdapter() {

            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                channel.getWriteContext().sendMessage(Netty4Utils.toBytesReference((ByteBuf) msg), new ActionListener<NioChannel>() {
                    @Override
                    public void onResponse(NioChannel nioChannel) {
                        promise.setSuccess();
                        // We should only be using unpooled buffers. So releasing only removes them to ensure they can
                        // be GCed.
                        ch.releaseOutbound();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        promise.setFailure(e);
                        // We should only be using unpooled buffers. So releasing only removes them to ensure they can
                        // be GCed.
                        ch.releaseOutbound();
                    }
                });

                // This is a little tricky. The embedded channel will complete the promise once it writes the message
                // to its outbound buffer. We do not want to complete the promise until the message is sent. So we
                // intercept the promise and pass a different promise back to the rest of the pipeline.
                ctx.write(msg, ch.newPromise());
            }
        });
        ch.pipeline().addLast(new HttpContentDecompressor());
        ch.pipeline().addLast(new HttpResponseEncoder());
        final HttpObjectAggregator aggregator = new HttpObjectAggregator(maxContentLength);
        if (maxCompositeBufferComponents != -1) {
            aggregator.setMaxCumulationBufferComponents(maxCompositeBufferComponents);
        }
        ch.pipeline().addLast(aggregator);
        if (compression) {
            ch.pipeline().addLast("encoder_compress", new HttpContentCompressor(compressionLevel));
        }
        if (corsConfig.isCorsSupportEnabled()) {
            ch.pipeline().addLast("cors", new Netty4CorsHandler(corsConfig));
        }
        if (pipelining) {
            ch.pipeline().addLast("pipelining", new HttpPipeliningHandler(pipeliningMaxEvents));
        }
        ch.pipeline().addLast("read_exception_handler", new ChannelInboundHandlerAdapter() {
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                exceptionHandler.accept(channel, cause);
            }
        });
        ch.pipeline().addLast("close_adaptor", new ChannelOutboundHandlerAdapter() {

            @Override
            public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
                channel.closeAsync();
            }

        });

        return ch;
    }
}
