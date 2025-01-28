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
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;

import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.netty4.internal.HttpHeadersAuthenticatorUtils;

import java.util.function.Predicate;

/**
 * A wrapper around {@link HttpObjectAggregator}. Provides optional content aggregation based on
 * predicate. {@link HttpObjectAggregator} also handles Expect: 100-continue and oversized content.
 * Unfortunately, Netty does not provide handlers for oversized messages beyond HttpObjectAggregator.
 */
public class Netty4HttpAggregator extends HttpObjectAggregator {
    private static final Predicate<HttpPreRequest> IGNORE_TEST = (req) -> req.uri().startsWith("/_test/request-stream") == false;

    private final Predicate<HttpPreRequest> decider;
    private boolean aggregating = true;
    private boolean ignoreContentAfterContinueResponse = false;

    public Netty4HttpAggregator(int maxContentLength, Predicate<HttpPreRequest> decider) {
        super(maxContentLength);
        this.decider = decider;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert msg instanceof HttpObject;
        if (msg instanceof HttpRequest request) {
            var preReq = HttpHeadersAuthenticatorUtils.asHttpPreRequest(request);
            aggregating = (decider.test(preReq) && IGNORE_TEST.test(preReq)) || request.decoderResult().isFailure();
        }
        if (aggregating || msg instanceof FullHttpRequest) {
            super.channelRead(ctx, msg);
        } else {
            handle(ctx, (HttpObject) msg);
        }
    }

    private void handle(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpRequest request) {
            var continueResponse = newContinueResponse(request, maxContentLength(), ctx.pipeline());
            if (continueResponse != null) {
                // there are 3 responses expected: 100, 413, 417
                // on 100 we pass request further and reply to client to continue
                // on 413/417 we ignore following content
                ctx.writeAndFlush(continueResponse);
                var resp = (FullHttpResponse) continueResponse;
                if (resp.status() != HttpResponseStatus.CONTINUE) {
                    ignoreContentAfterContinueResponse = true;
                    return;
                }
                HttpUtil.set100ContinueExpected(request, false);
            }
            ignoreContentAfterContinueResponse = false;
            ctx.fireChannelRead(msg);
        } else {
            var httpContent = (HttpContent) msg;
            if (ignoreContentAfterContinueResponse) {
                httpContent.release();
            } else {
                ctx.fireChannelRead(msg);
            }
        }
    }
}
