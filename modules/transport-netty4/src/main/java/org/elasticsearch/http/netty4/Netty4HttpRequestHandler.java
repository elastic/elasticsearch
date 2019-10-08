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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.http.HttpPipelinedRequest;

@ChannelHandler.Sharable
class Netty4HttpRequestHandler extends SimpleChannelInboundHandler<HttpPipelinedRequest<FullHttpRequest>> {

    private final Netty4HttpServerTransport serverTransport;

    Netty4HttpRequestHandler(Netty4HttpServerTransport serverTransport) {
        this.serverTransport = serverTransport;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpPipelinedRequest<FullHttpRequest> msg) {
        Netty4HttpChannel channel = ctx.channel().attr(Netty4HttpServerTransport.HTTP_CHANNEL_KEY).get();
        FullHttpRequest request = msg.getRequest();
        final FullHttpRequest copiedRequest;
        try {
            copiedRequest =
                new DefaultFullHttpRequest(
                    request.protocolVersion(),
                    request.method(),
                    request.uri(),
                    Unpooled.copiedBuffer(request.content()),
                    request.headers(),
                    request.trailingHeaders());
        } finally {
            // As we have copied the buffer, we can release the request
            request.release();
        }
        Netty4HttpRequest httpRequest = new Netty4HttpRequest(copiedRequest, msg.getSequence());

        if (request.decoderResult().isFailure()) {
            Throwable cause = request.decoderResult().cause();
            if (cause instanceof Error) {
                ExceptionsHelper.maybeDieOnAnotherThread(cause);
                serverTransport.incomingRequestError(httpRequest, channel, new Exception(cause));
            } else {
                serverTransport.incomingRequestError(httpRequest, channel, (Exception) cause);
            }
        } else {
            serverTransport.incomingRequest(httpRequest, channel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ExceptionsHelper.maybeDieOnAnotherThread(cause);
        Netty4HttpChannel channel = ctx.channel().attr(Netty4HttpServerTransport.HTTP_CHANNEL_KEY).get();
        if (cause instanceof Error) {
            serverTransport.onException(channel, new Exception(cause));
        } else {
            serverTransport.onException(channel, (Exception) cause);
        }
    }
}
