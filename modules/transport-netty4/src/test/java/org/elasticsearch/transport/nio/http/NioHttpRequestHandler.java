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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.netty4.Netty4HttpChannel;
import org.elasticsearch.http.netty4.Netty4HttpRequest;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;

public class NioHttpRequestHandler extends SimpleChannelInboundHandler<Object> {

    private final NioHttpTransport transport;
    private final NamedXContentRegistry xContentRegistry;
    private final ThreadContext threadContext;
    private final boolean detailedErrorsEnabled;
    private final boolean httpPipeliningEnabled;

    public NioHttpRequestHandler(NioHttpTransport transport, NamedXContentRegistry xContentRegistry, boolean detailedErrorsEnabled,
                                 ThreadContext threadContext, boolean httpPipeliningEnabled) {
        this.transport = transport;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.threadContext = threadContext;
        this.httpPipeliningEnabled = httpPipeliningEnabled;
        this.xContentRegistry = xContentRegistry;
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
        final Netty4HttpRequest httpRequest = new Netty4HttpRequest(xContentRegistry, copy, ctx.channel());
        final Netty4HttpChannel channel = new Netty4HttpChannel(null, httpRequest, pipelinedRequest, detailedErrorsEnabled, threadContext);

        if (request.decoderResult().isSuccess()) {
            transport.dispatchRequest(httpRequest, channel);
        } else {
            assert request.decoderResult().isFailure();
            transport.dispatchBadRequest(httpRequest, channel, request.decoderResult().cause());
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        NioHttpNettyAdaptor.ESNettyChannel channel = (NioHttpNettyAdaptor.ESNettyChannel) ctx.channel();
        transport.exceptionCaught(channel.getNioSocketChannel(), cause);
    }
}
