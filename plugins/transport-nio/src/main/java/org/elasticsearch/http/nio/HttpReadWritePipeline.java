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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.FlushProducer;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.WriteOperation;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.Collections;

public class HttpReadWritePipeline implements SocketChannelContext.ReadConsumer, FlushProducer {

    private final NettyAdaptor adaptor;
    private final NioHttpServerTransport transport;
    private final NamedXContentRegistry xContentRegistry;
    private final ThreadContext threadContext;
    private boolean detailedErrorsEnabled;

    public HttpReadWritePipeline(NioHttpServerTransport transport, NamedXContentRegistry xContentRegistry, ThreadContext threadContext) {
        this.transport = transport;
        this.xContentRegistry = xContentRegistry;
        this.threadContext = threadContext;
        // TODO: These use all the default settings level. The settings still need to be implemented.
        HttpRequestDecoder decoder = new HttpRequestDecoder(
            Math.toIntExact(new ByteSizeValue(4, ByteSizeUnit.KB).getBytes()),
            Math.toIntExact(new ByteSizeValue(8, ByteSizeUnit.KB).getBytes()),
            Math.toIntExact(new ByteSizeValue(8, ByteSizeUnit.KB).getBytes()));
        decoder.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
        HttpContentDecompressor decompressor = new HttpContentDecompressor();
        HttpResponseEncoder encoder = new HttpResponseEncoder();
        HttpObjectAggregator aggregator = new HttpObjectAggregator(Math.toIntExact(new ByteSizeValue(100, ByteSizeUnit.MB).getBytes()));
        HttpContentCompressor compressor = new HttpContentCompressor(3);

        adaptor = new NettyAdaptor(decoder, decompressor, encoder, aggregator, compressor);
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        int bytesConsumed = adaptor.read(channelBuffer.sliceBuffersTo(channelBuffer.getIndex()));
        // Handle requests
        return bytesConsumed;
    }

    @Override
    public void produceWrites(WriteOperation writeOperation) {
        adaptor.write(writeOperation);
    }

    @Override
    public FlushOperation pollFlushOperation() {
        return adaptor.pollFlushOperations();
    }

    @Override
    public void close() throws IOException {
        try {
            adaptor.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected void handleRequest(Object msg) throws Exception {
        final FullHttpRequest request = (FullHttpRequest) msg;

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
         * IllegalArgumentException from the channel constructor and then attempt to create a new channel that bypasses parsing of these
         * parameter values.
         */
        final NioHttpChannel channel;
        {
            NioHttpChannel innerChannel;
            try {
                innerChannel = new NioHttpChannel(transport, httpRequest, detailedErrorsEnabled, threadContext);
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
                innerChannel = new NioHttpChannel(transport, innerRequest, detailedErrorsEnabled, threadContext);
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
