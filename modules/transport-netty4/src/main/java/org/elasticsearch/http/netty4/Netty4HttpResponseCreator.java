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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpResponse;

import java.util.List;

@ChannelHandler.Sharable
class Netty4HttpResponseCreator extends MessageToMessageEncoder<Netty4HttpResponse> {

    public static final int ONE_TWENTY_EIGHT_KB = 128 * 1024;

    @Override
    protected void encode(ChannelHandlerContext ctx, Netty4HttpResponse msg, List<Object> out) {
        HttpResponse response = new DefaultHttpResponse(msg.protocolVersion(), msg.status(), msg.headers());
        out.add(response);
        ByteBuf content = msg.content();
        while (content.readableBytes() > ONE_TWENTY_EIGHT_KB) {
            out.add(new DefaultHttpContent(content.readRetainedSlice(ONE_TWENTY_EIGHT_KB)));
        }
        out.add(new DefaultLastHttpContent(content.readRetainedSlice(content.readableBytes())));
    }
}
