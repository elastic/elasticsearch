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
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.netty4.internal.HttpHeadersAuthenticatorUtils;

import java.util.function.Predicate;

/**
 * A wrapper around {@link HttpObjectAggregator}. Provides optional content aggregation based on
 * predicate. {@link HttpObjectAggregator} also handles Expect: 100-continue and oversized content.
 * Provides content size handling for non-aggregated requests too.
 */
public class Netty4HttpAggregator extends HttpObjectAggregator {
    private static final Predicate<HttpPreRequest> IGNORE_TEST = (req) -> req.uri().startsWith("/_test/request-stream") == false;

    private final Predicate<HttpPreRequest> decider;
    private final Netty4HttpContentSizeHandler streamContentSizeHandler;
    private boolean aggregating = true;

    public Netty4HttpAggregator(int maxContentLength, Predicate<HttpPreRequest> decider, HttpRequestDecoder decoder) {
        super(maxContentLength);
        this.decider = decider;
        this.streamContentSizeHandler = new Netty4HttpContentSizeHandler(decoder, maxContentLength);
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
            if (msg instanceof LastHttpContent == false) {
                ctx.read(); // HttpObjectAggregator is tricky with auto-read off, it might not call read again, calling on its behalf
            }
        } else {
            streamContentSizeHandler.channelRead(ctx, msg);
        }
    }
}
