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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.http.CorsHandler;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.http.HttpUtils;

@ChannelHandler.Sharable
class Netty4HttpRequestHandler extends SimpleChannelInboundHandler<HttpPipelinedRequest> {

    private static final ActionListener<Void> NO_OP = ActionListener.wrap(() -> {});

    private final Netty4HttpServerTransport serverTransport;
    private final CorsHandler corsHandler;

    Netty4HttpRequestHandler(Netty4HttpServerTransport serverTransport, CorsHandler corsHandler) {
        this.serverTransport = serverTransport;
        this.corsHandler = corsHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpPipelinedRequest httpRequest) {
        final Netty4HttpChannel channel = ctx.channel().attr(Netty4HttpServerTransport.HTTP_CHANNEL_KEY).get();
        boolean success = false;
        try {
            HttpResponse earlyResponse = corsHandler.handleInbound(httpRequest);
            if (earlyResponse != null) {
                channel.sendResponse(earlyResponse, earlyResponseListener(httpRequest, channel));
                httpRequest.release();
            } else {
                serverTransport.incomingRequest(httpRequest, channel);
            }
            success = true;
        } finally {
            if (success == false) {
                httpRequest.release();
            }
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

    private static ActionListener<Void> earlyResponseListener(HttpRequest request, HttpChannel httpChannel) {
        if (HttpUtils.shouldCloseConnection(request)) {
            return ActionListener.wrap(() -> CloseableChannel.closeChannel(httpChannel));
        } else {
            return NO_OP;
        }
    }
}
