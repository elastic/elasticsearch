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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.Collections;

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

        boolean success = false;
        try {

            final FullHttpRequest copy =
                    new DefaultFullHttpRequest(
                            request.protocolVersion(),
                            request.method(),
                            request.uri(),
                            Unpooled.copiedBuffer(request.content()),
                            request.headers(),
                            request.trailingHeaders());

            Exception badRequestCause = null;

            final Netty4HttpRequest httpRequest;
            {
                Netty4HttpRequest innerHttpRequest;
                try {
                    innerHttpRequest = new Netty4HttpRequest(serverTransport.xContentRegistry, copy, ctx.channel());
                } catch (final RestRequest.ContentTypeHeaderException e) {
                    badRequestCause = e;
                    innerHttpRequest = requestWithoutContentTypeHeader(copy, ctx.channel(), badRequestCause);
                } catch (final RestRequest.BadParameterException e) {
                    badRequestCause = e;
                    innerHttpRequest = requestWithoutParameters(copy, ctx.channel());
                }
                httpRequest = innerHttpRequest;
            }

            final Netty4HttpChannel channel;
            {
                Netty4HttpChannel innerChannel;
                try {
                    innerChannel =
                            new Netty4HttpChannel(serverTransport, httpRequest, pipelinedRequest, detailedErrorsEnabled, threadContext);
                } catch (final IllegalArgumentException e) {
                    if (badRequestCause == null) {
                        badRequestCause = e;
                    } else {
                        badRequestCause.addSuppressed(e);
                    }
                    final Netty4HttpRequest innerRequest =
                            new Netty4HttpRequest(
                                    serverTransport.xContentRegistry,
                                    Collections.emptyMap(),
                                    copy.uri(),
                                    copy,
                                    ctx.channel());
                    innerChannel =
                            new Netty4HttpChannel(serverTransport, innerRequest, pipelinedRequest, detailedErrorsEnabled, threadContext);
                }
                channel = innerChannel;
            }

            if (request.decoderResult().isFailure()) {
                serverTransport.dispatchBadRequest(httpRequest, channel, request.decoderResult().cause());
            } else if (badRequestCause != null) {
                serverTransport.dispatchBadRequest(httpRequest, channel, badRequestCause);
            } else {
                serverTransport.dispatchRequest(httpRequest, channel);
            }
            success = true;
        } finally {
            if (success == false && pipelinedRequest != null) {
                pipelinedRequest.release();
            }
        }
    }

    private Netty4HttpRequest requestWithoutContentTypeHeader(
            final FullHttpRequest request, final Channel channel, final Exception badRequestCause) {
        final HttpHeaders headersWithoutContentTypeHeader = new DefaultHttpHeaders();
        headersWithoutContentTypeHeader.add(request.headers());
        headersWithoutContentTypeHeader.remove("Content-Type");
        final FullHttpRequest requestWithoutContentTypeHeader =
                new DefaultFullHttpRequest(
                        request.protocolVersion(),
                        request.method(),
                        request.uri(),
                        request.content(),
                        headersWithoutContentTypeHeader,
                        request.trailingHeaders()); // Content-Type can not be a trailing header
        try {
            return new Netty4HttpRequest(serverTransport.xContentRegistry, requestWithoutContentTypeHeader, channel);
        } catch (final RestRequest.BadParameterException e) {
            badRequestCause.addSuppressed(e);
            return requestWithoutParameters(requestWithoutContentTypeHeader, channel);
        }
    }

    private Netty4HttpRequest requestWithoutParameters(final FullHttpRequest request, final Channel channel) {
        return new Netty4HttpRequest(serverTransport.xContentRegistry, Collections.emptyMap(), request.uri(), request, channel);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Netty4Utils.maybeDie(cause);
        serverTransport.exceptionCaught(ctx, cause);
    }

}
