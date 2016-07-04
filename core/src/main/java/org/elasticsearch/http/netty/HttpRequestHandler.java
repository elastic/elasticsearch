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

package org.elasticsearch.http.netty;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty.pipelining.OrderedUpstreamMessageEvent;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 *
 */
@ChannelHandler.Sharable
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

    private final NettyHttpServerTransport serverTransport;
    private final boolean httpPipeliningEnabled;
    private final boolean detailedErrorsEnabled;
    private final ThreadContext threadContext;

    public HttpRequestHandler(NettyHttpServerTransport serverTransport, boolean detailedErrorsEnabled, ThreadContext threadContext) {
        this.serverTransport = serverTransport;
        this.httpPipeliningEnabled = serverTransport.pipelining;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.threadContext = threadContext;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        HttpRequest request;
        OrderedUpstreamMessageEvent oue = null;
        if (this.httpPipeliningEnabled && e instanceof OrderedUpstreamMessageEvent) {
            oue = (OrderedUpstreamMessageEvent) e;
            request = (HttpRequest) oue.getMessage();
        } else {
            request = (HttpRequest) e.getMessage();
        }

        threadContext.copyHeaders(request.headers());
        // the netty HTTP handling always copy over the buffer to its own buffer, either in NioWorker internally
        // when reading, or using a cumalation buffer
        NettyHttpRequest httpRequest = new NettyHttpRequest(request, e.getChannel());
        NettyHttpChannel channel = new NettyHttpChannel(serverTransport, httpRequest, oue, detailedErrorsEnabled);
        serverTransport.dispatchRequest(httpRequest, channel);
        super.messageReceived(ctx, e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        serverTransport.exceptionCaught(ctx, e);
    }
}
