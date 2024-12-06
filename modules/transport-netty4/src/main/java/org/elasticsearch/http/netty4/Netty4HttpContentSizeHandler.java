/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;

/**
 * A wrapper around {@link HttpObjectAggregator}. Handles "Expect: 100-continue" and oversized content.
 * Unfortunately, Netty does not provide handlers for oversized messages beyond HttpObjectAggregator.
 * TODO: move to {@link org.elasticsearch.rest.RestController}
 */
public class Netty4HttpContentSizeHandler extends HttpObjectAggregator {
    private boolean ignoreFollowingContent = false;
    private int contentLength = 0;
    private HttpRequest currentRequest;

    public Netty4HttpContentSizeHandler(int maxContentLength) {
        super(maxContentLength);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert msg instanceof HttpObject;
        handle(ctx, (HttpObject) msg);
    }

    private void handle(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        if (msg instanceof HttpRequest request) {
            var continueResponse = newContinueResponse(request, maxContentLength(), ctx.pipeline());
            if (continueResponse != null) {
                // there are 3 responses expected: 100, 413, 417
                // on 100 we pass request further and reply to client to continue
                // on 413/417 we ignore following content
                ctx.writeAndFlush(continueResponse);
                var resp = (FullHttpResponse) continueResponse;
                if (resp.status() != HttpResponseStatus.CONTINUE) {
                    ignoreFollowingContent = true;
                    return;
                }
                HttpUtil.set100ContinueExpected(request, false);
            } else {
                if (HttpUtil.getContentLength(request, 0) > maxContentLength()) {
                    handleOversizedMessage(ctx, request);
                    ignoreFollowingContent = true;
                    return;
                }
            }
            ignoreFollowingContent = false;
            contentLength = 0;
            currentRequest = request;
            ctx.fireChannelRead(msg);
        } else {
            var httpContent = (HttpContent) msg;
            if (ignoreFollowingContent) {
                httpContent.release();
            } else {
                contentLength += httpContent.content().readableBytes();
                if (contentLength > maxContentLength()) {
                    ignoreFollowingContent = true;
                    httpContent.release();
                    HttpUtil.setKeepAlive(currentRequest, false);
                    handleOversizedMessage(ctx, currentRequest);
                    return;
                }
                ctx.fireChannelRead(msg);
            }
        }
    }
}
