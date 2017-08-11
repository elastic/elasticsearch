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
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.netty4.Netty4HttpRequest;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

public class NioHttpRequestHandler {

    private final NioHttpTransport transport;
    private final NamedXContentRegistry xContentRegistry;
    private final ThreadContext threadContext;
    private final Netty4CorsConfig corsConfig;
    private final boolean detailedErrorsEnabled;
    private final boolean httpPipeliningEnabled;
    private final boolean resetCookies;

    public NioHttpRequestHandler(NioHttpTransport transport, NamedXContentRegistry xContentRegistry, boolean detailedErrorsEnabled,
                                 ThreadContext threadContext, boolean httpPipeliningEnabled, Netty4CorsConfig corsConfig,
                                 boolean resetCookies) {
        this.transport = transport;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.threadContext = threadContext;
        this.httpPipeliningEnabled = httpPipeliningEnabled;
        this.xContentRegistry = xContentRegistry;
        this.corsConfig = corsConfig;
        this.resetCookies = resetCookies;
    }

    public void handleMessage(NioSocketChannel channel, Channel nettyChannel, Object msg) {
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
        final Netty4HttpRequest httpRequest = new Netty4HttpRequest(xContentRegistry, copy, nettyChannel);
        final NioHttpChannel httpChannel = new NioHttpChannel(httpRequest, channel, pipelinedRequest, detailedErrorsEnabled, threadContext, corsConfig,
            resetCookies);

        if (request.decoderResult().isSuccess()) {
            transport.dispatchRequest(httpRequest, httpChannel);
        } else {
            assert request.decoderResult().isFailure();
            transport.dispatchBadRequest(httpRequest, httpChannel, request.decoderResult().cause());
        }
    }
}
