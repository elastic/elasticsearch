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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import org.elasticsearch.http.HttpRequestMemoryController;

import java.util.List;

class Netty4HttpRequestAggregator2 extends HttpObjectAggregator {

    private final HttpRequestMemoryController memoryController;

    Netty4HttpRequestAggregator2(int maxContentLength, HttpRequestMemoryController memoryController) {
        super(maxContentLength);
        this.memoryController = memoryController;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
        assert assertPreDecode(msg);
        super.decode(ctx, msg, out);
        if (out.isEmpty() == false) {
            for (int i = 0; i < out.size(); ++i) {

            }
        }
    }

    @Override
    protected FullHttpMessage beginAggregation(HttpMessage start, ByteBuf content) throws Exception {
        assert start instanceof HttpRequest;
        memoryController.startRequest(((HttpRequest) start).uri());
        return super.beginAggregation(start, content);
    }

    @Override
    protected void aggregate(FullHttpMessage aggregated, HttpContent content) throws Exception {
        memoryController.bytesReceived(content.content().readableBytes());
        super.aggregate(aggregated, content);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            super.channelInactive(ctx);
        } finally {
            memoryController.close();
        }
    }

    @Override
    protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
        super.handleOversizedMessage(ctx, oversized);
    }

    @Override
    protected boolean ignoreContentAfterContinueResponse(Object msg) {
        return super.ignoreContentAfterContinueResponse(msg);
    }

    private boolean assertPreDecode(HttpObject msg) throws Exception {
        if (isStartMessage(msg)) {
//            assert curr
        }

        return true;
    }
}
