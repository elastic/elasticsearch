/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */


package org.noop.essecure.http;

import org.noop.essecure.http.HttpSnoopClient.HttpListener;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

public class HttpSnoopClientHandler extends SimpleChannelInboundHandler<HttpObject> {

    private final HttpListener listener;
    private String debugMessage = "";
    private String responseData = "";

    public HttpSnoopClientHandler(HttpListener listener)
    {
        this.listener = listener;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) msg;

            debugMessage+=("STATUS: " + response.status());
            debugMessage+=("VERSION: " + response.protocolVersion());

            if (!response.headers().isEmpty()) {
                for (CharSequence name: response.headers().names()) {
                    for (CharSequence value: response.headers().getAll(name)) {
                        debugMessage+=("HEADER: " + name + " = " + value);
                    }
                }
            }

            if (HttpUtil.isTransferEncodingChunked(response)) {
                debugMessage+=("CHUNKED CONTENT {");
            } else {
                debugMessage+=("CONTENT {");
            }
        }
        if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;

            responseData += (content.content().toString(CharsetUtil.UTF_8));

            if (content instanceof LastHttpContent) {
                debugMessage+=("} END OF CONTENT");
                ctx.close();
                listener.OnSuccess("", responseData, debugMessage);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
        listener.OnFail("", debugMessage);
    }
}
