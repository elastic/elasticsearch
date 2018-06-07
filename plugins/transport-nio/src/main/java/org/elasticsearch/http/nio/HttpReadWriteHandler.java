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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.http.nio.cors.NioCorsConfig;
import org.elasticsearch.http.nio.cors.NioCorsHandler;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.ReadWriteHandler;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.WriteOperation;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;

public class HttpReadWriteHandler implements ReadWriteHandler {

    private final NettyAdaptor adaptor;
    private final NioSocketChannel nioChannel;
    private final NioHttpServerTransport transport;
    private final HttpHandlingSettings settings;
    private final NamedXContentRegistry xContentRegistry;
    private final NioCorsConfig corsConfig;
    private final ThreadContext threadContext;

    HttpReadWriteHandler(NioSocketChannel nioChannel, NioHttpServerTransport transport, HttpHandlingSettings settings,
                         NamedXContentRegistry xContentRegistry, NioCorsConfig corsConfig, ThreadContext threadContext) {
        this.nioChannel = nioChannel;
        this.transport = transport;
        this.settings = settings;
        this.xContentRegistry = xContentRegistry;
        this.corsConfig = corsConfig;
        this.threadContext = threadContext;

        List<ChannelHandler> handlers = new ArrayList<>(5);
        HttpRequestDecoder decoder = new HttpRequestDecoder(settings.getMaxInitialLineLength(), settings.getMaxHeaderSize(),
            settings.getMaxChunkSize());
        decoder.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
        handlers.add(decoder);
        handlers.add(new HttpContentDecompressor());
        handlers.add(new HttpResponseEncoder());
        handlers.add(new HttpObjectAggregator(settings.getMaxContentLength()));
        if (settings.isCompression()) {
            handlers.add(new HttpContentCompressor(settings.getCompressionLevel()));
        }
        if (settings.isCorsEnabled()) {
            handlers.add(new NioCorsHandler(corsConfig));
        }
        handlers.add(new NioHttpPipeliningHandler(transport.getLogger(), settings.getPipeliningMaxEvents()));

        adaptor = new NettyAdaptor(handlers.toArray(new ChannelHandler[0]));
        adaptor.addCloseListener((v, e) -> nioChannel.close());
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        int bytesConsumed = adaptor.read(channelBuffer.sliceBuffersTo(channelBuffer.getIndex()));
        Object message;
        while ((message = adaptor.pollInboundMessage()) != null) {
            handleRequest(message);
        }

        return bytesConsumed;
    }

    @Override
    public WriteOperation createWriteOperation(SocketChannelContext context, Object message, BiConsumer<Void, Exception> listener) {
        assert message instanceof NioHttpResponse : "This channel only supports messages that are of type: "
            + NioHttpResponse.class + ". Found type: " + message.getClass() + ".";
        return new HttpWriteOperation(context, (NioHttpResponse) message, listener);
    }

    @Override
    public List<FlushOperation> writeToBytes(WriteOperation writeOperation) {
        adaptor.write(writeOperation);
        return pollFlushOperations();
    }

    @Override
    public List<FlushOperation> pollFlushOperations() {
        ArrayList<FlushOperation> copiedOperations = new ArrayList<>(adaptor.getOutboundCount());
        FlushOperation flushOperation;
        while ((flushOperation = adaptor.pollOutboundOperation()) != null) {
            copiedOperations.add(flushOperation);
        }
        return copiedOperations;
    }

    @Override
    public void close() throws IOException {
        try {
            adaptor.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void handleRequest(Object msg) {
        final HttpPipelinedRequest<FullHttpRequest> pipelinedRequest = (HttpPipelinedRequest<FullHttpRequest>) msg;
        FullHttpRequest request = pipelinedRequest.getRequest();

        try {
            final FullHttpRequest copiedRequest =
                new DefaultFullHttpRequest(
                    request.protocolVersion(),
                    request.method(),
                    request.uri(),
                    Unpooled.copiedBuffer(request.content()),
                    request.headers(),
                    request.trailingHeaders());

            Exception badRequestCause = null;

            /*
             * We want to create a REST request from the incoming request from Netty. However, creating this request could fail if there
             * are incorrectly encoded parameters, or the Content-Type header is invalid. If one of these specific failures occurs, we
             * attempt to create a REST request again without the input that caused the exception (e.g., we remove the Content-Type header,
             * or skip decoding the parameters). Once we have a request in hand, we then dispatch the request as a bad request with the
             * underlying exception that caused us to treat the request as bad.
             */
            final NioHttpRequest httpRequest;
            {
                NioHttpRequest innerHttpRequest;
                try {
                    innerHttpRequest = new NioHttpRequest(xContentRegistry, copiedRequest);
                } catch (final RestRequest.ContentTypeHeaderException e) {
                    badRequestCause = e;
                    innerHttpRequest = requestWithoutContentTypeHeader(copiedRequest, badRequestCause);
                } catch (final RestRequest.BadParameterException e) {
                    badRequestCause = e;
                    innerHttpRequest = requestWithoutParameters(copiedRequest);
                }
                httpRequest = innerHttpRequest;
            }

            /*
             * We now want to create a channel used to send the response on. However, creating this channel can fail if there are invalid
             * parameter values for any of the filter_path, human, or pretty parameters. We detect these specific failures via an
             * IllegalArgumentException from the channel constructor and then attempt to create a new channel that bypasses parsing of
             * these parameter values.
             */
            final NioHttpChannel channel;
            {
                NioHttpChannel innerChannel;
                int sequence = pipelinedRequest.getSequence();
                BigArrays bigArrays = transport.getBigArrays();
                try {
                    innerChannel = new NioHttpChannel(nioChannel, bigArrays, httpRequest, sequence, settings, corsConfig, threadContext);
                } catch (final IllegalArgumentException e) {
                    if (badRequestCause == null) {
                        badRequestCause = e;
                    } else {
                        badRequestCause.addSuppressed(e);
                    }
                    final NioHttpRequest innerRequest =
                        new NioHttpRequest(
                            xContentRegistry,
                            Collections.emptyMap(), // we are going to dispatch the request as a bad request, drop all parameters
                            copiedRequest.uri(),
                            copiedRequest);
                    innerChannel = new NioHttpChannel(nioChannel, bigArrays, innerRequest, sequence, settings, corsConfig, threadContext);
                }
                channel = innerChannel;
            }

            if (request.decoderResult().isFailure()) {
                transport.dispatchBadRequest(httpRequest, channel, request.decoderResult().cause());
            } else if (badRequestCause != null) {
                transport.dispatchBadRequest(httpRequest, channel, badRequestCause);
            } else {
                transport.dispatchRequest(httpRequest, channel);
            }
        } finally {
            // As we have copied the buffer, we can release the request
            request.release();
        }
    }

    private NioHttpRequest requestWithoutContentTypeHeader(final FullHttpRequest request, final Exception badRequestCause) {
        final HttpHeaders headersWithoutContentTypeHeader = new DefaultHttpHeaders();
        headersWithoutContentTypeHeader.add(request.headers());
        headersWithoutContentTypeHeader.remove("Content-Type");
        final FullHttpRequest requestWithoutContentTypeHeader =
            new DefaultFullHttpRequest(
                request.protocolVersion(),
                request.method(),
                request.uri(),
                request.content(),
                headersWithoutContentTypeHeader, // remove the Content-Type header so as to not parse it again
                request.trailingHeaders()); // Content-Type can not be a trailing header
        try {
            return new NioHttpRequest(xContentRegistry, requestWithoutContentTypeHeader);
        } catch (final RestRequest.BadParameterException e) {
            badRequestCause.addSuppressed(e);
            return requestWithoutParameters(requestWithoutContentTypeHeader);
        }
    }

    private NioHttpRequest requestWithoutParameters(final FullHttpRequest request) {
        // remove all parameters as at least one is incorrectly encoded
        return new NioHttpRequest(xContentRegistry, Collections.emptyMap(), request.uri(), request);
    }
}
