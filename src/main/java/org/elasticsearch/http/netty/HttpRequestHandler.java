/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.HttpRequest;


/**
 *
 */
@ChannelHandler.Sharable
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

    private final NettyHttpServerTransport serverTransport;

    public HttpRequestHandler(NettyHttpServerTransport serverTransport) {
        this.serverTransport = serverTransport;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        HttpRequest request = (HttpRequest) e.getMessage();
        // the netty HTTP handling always copy over the buffer to its own buffer, either in NioWorker internally
        // when reading, or using a cumalation buffer
        serverTransport.dispatchRequest(new NettyHttpRequest(request), new NettyHttpChannel(serverTransport, e.getChannel(), request));
        super.messageReceived(ctx, e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        serverTransport.exceptionCaught(ctx, e);
    }
}
