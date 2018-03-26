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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@ChannelHandler.Sharable
class Netty4HttpRequestHandler extends SimpleChannelInboundHandler<Object> {

    private final Netty4HttpServerTransport serverTransport;
    private final boolean httpPipeliningEnabled;
    private final boolean detailedErrorsEnabled;
    private final ThreadContext threadContext;

    Netty4HttpRequestHandler(Netty4HttpServerTransport serverTransport, boolean detailedErrorsEnabled, ThreadContext threadContext) {
        this.serverTransport = serverTransport;
        this.httpPipeliningEnabled = serverTransport.pipelining;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.threadContext = threadContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        final FullHttpRequest request;
        final HttpPipelinedRequest pipelinedRequest;
        if (this.httpPipeliningEnabled && msg instanceof HttpPipelinedRequest) {
            pipelinedRequest = (HttpPipelinedRequest) msg;
            request = (FullHttpRequest) pipelinedRequest.last();
        } else {
            pipelinedRequest = null;
            request = (FullHttpRequest) msg;
        }

        final FullHttpRequest copy =
                new DefaultFullHttpRequest(
                        request.protocolVersion(),
                        request.method(),
                        request.uri(),
                        Unpooled.copiedBuffer(request.content()),
                        request.headers(),
                        request.trailingHeaders());
        final Netty4HttpRequest httpRequest;
        try {
            httpRequest = new Netty4HttpRequest(serverTransport.xContentRegistry, copy, ctx.channel());
        } catch (final Exception e) {
            // we use suppliers here because if acquiring these values blows up, we want error handling in handleException to takeover
            final Supplier<List<String>> contentTypes = () -> request.headers().getAll("Content-Type");
            final Supplier<String> accept = () -> request.headers().get("Accept");
            handleException(e, ctx, copy, pipelinedRequest, request.uri(), ToXContent.EMPTY_PARAMS, contentTypes, accept);
            return;
        }

        final Netty4HttpChannel channel;
        try {
            channel = new Netty4HttpChannel(serverTransport, httpRequest, pipelinedRequest, detailedErrorsEnabled, threadContext);
        } catch (final Exception e) {
            // we use suppliers here because if acquiring these values blows up, we want error handling in handleException to takeover
            final Supplier<List<String>> contentTypes = () -> httpRequest.getAllHeaderValues("Content-Type");
            final Supplier<String> accept = () -> httpRequest.header("Accept");
            handleException(e, ctx, copy, pipelinedRequest, httpRequest.rawPath(), httpRequest, contentTypes, accept);
            return;
        }

        if (request.decoderResult().isSuccess()) {
            serverTransport.dispatchRequest(httpRequest, channel);
        } else {
            assert request.decoderResult().isFailure();
            serverTransport.dispatchBadRequest(httpRequest, channel, request.decoderResult().cause());
        }
    }

    private void handleException(
            final Exception e,
            final ChannelHandlerContext ctx,
            final FullHttpRequest copy,
            final HttpPipelinedRequest pipelinedRequest,
            final String rawPath,
            final ToXContent.Params params,
            final Supplier<List<String>> contentType,
            final Supplier<String> accept) throws IOException {
        // we failed to construct a channel so we use direct access to underlying channel to write a response to the client
        boolean success = false;
        try {
            final AtomicReference<BytesStreamOutput> bytesStreamOutput = new AtomicReference<>();
            final Supplier<BytesStreamOutput> bytesOutput = () -> {
                if (bytesStreamOutput.get() == null) {
                    bytesStreamOutput.set(new ReleasableBytesStreamOutput(serverTransport.bigArrays));
                }
                return bytesStreamOutput.get();
            };
            final XContentType requestContentType;
            XContentType parsedRequestContentType;
            try {
                parsedRequestContentType = RestRequest.parseContentType(contentType.get());
            } catch (final IllegalArgumentException inner) {
                e.addSuppressed(inner);
                parsedRequestContentType = null;
            }
            requestContentType = parsedRequestContentType;
            final CheckedSupplier<XContentBuilder, IOException> supplier =
                    () -> AbstractRestChannel.newBuilder(requestContentType, false, null, accept.get(), false, false, bytesOutput);
            final BytesRestResponse response = new BytesRestResponse(params, rawPath, supplier, detailedErrorsEnabled, e);
            Netty4HttpChannel.sendResponse(ctx.channel(), serverTransport, copy, pipelinedRequest, response, threadContext, bytesOutput);
            success = true;
        } finally {
            if (success == false && pipelinedRequest != null) {
                pipelinedRequest.release();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Netty4Utils.maybeDie(cause);
        serverTransport.exceptionCaught(ctx, cause);
    }

}
