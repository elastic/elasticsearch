/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;

import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.netty4.internal.HttpHeadersAuthenticatorUtils;

import java.util.function.Predicate;

/**
 * <p>
 * A wrapper around {@link HttpObjectAggregator}. Provides optional content aggregation based on
 * predicate. {@link HttpObjectAggregator} also handles Expect: 100-continue and oversized content.
 * Unfortunately, Netty does not provide handlers for oversized messages beyond HttpObjectAggregator.
 * </p>
 * <p>
 * This wrapper cherry-pick methods from underlying handlers.
 * {@link HttpObjectAggregator#newContinueResponse(HttpMessage, int, ChannelPipeline)} provides
 * handling for Expect: 100-continue. {@link HttpObjectAggregator#handleOversizedMessage(ChannelHandlerContext, HttpMessage)}
 * provides handling for requests that already in fly and reached limit, for example chunked encoding.
 * </p>
 *
 */
public class Netty4HttpAggregator extends HttpObjectAggregator {
    private static final Predicate<HttpPreRequest> IGNORE_TEST = (req) -> req.uri().startsWith("/_test/request-stream") == false;

    private final Predicate<HttpPreRequest> decider;
    private boolean aggregating = true;
    private long currentContentLength = 0;
    private HttpRequest currentRequest;
    private boolean handlingOversized = false;
    private boolean ignoreContentAfterContinueResponse = false;

    public Netty4HttpAggregator(int maxContentLength) {
        this(maxContentLength, IGNORE_TEST);
    }

    public Netty4HttpAggregator(int maxContentLength, Predicate<HttpPreRequest> decider) {
        super(maxContentLength);
        this.decider = decider;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert msg instanceof HttpObject;
        if (msg instanceof HttpRequest request) {
            var preReq = HttpHeadersAuthenticatorUtils.asHttpPreRequest(request);
            aggregating = decider.test(preReq);
        }
        if (aggregating || msg instanceof FullHttpRequest) {
            super.channelRead(ctx, msg);
        } else {
            handle(ctx, (HttpObject) msg);
        }
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
                    ignoreContentAfterContinueResponse = true;
                    return;
                }
                HttpUtil.set100ContinueExpected(request, false);
            }
            currentRequest = request;
            currentContentLength = 0;
            ignoreContentAfterContinueResponse = false;
            ctx.fireChannelRead(msg);

        } else {
            var httpContent = (HttpContent) msg;
            if (ignoreContentAfterContinueResponse) {
                httpContent.release();
                return;
            }
            currentContentLength += httpContent.content().readableBytes();
            if (currentContentLength > maxContentLength()) {
                if (handlingOversized == false) {
                    handlingOversized = true;
                    // magic: passing full request into handleOversizedMessage will close connection
                    var fullReq = new DefaultFullHttpRequest(
                        currentRequest.protocolVersion(),
                        currentRequest.method(),
                        currentRequest.uri(),
                        Unpooled.EMPTY_BUFFER,
                        currentRequest.headers(),
                        EmptyHttpHeaders.INSTANCE
                    );
                    handleOversizedMessage(ctx, fullReq);
                }
                // if we're already handling oversized message connection will be closed soon
                // we can discard following content
                httpContent.release();
            } else {
                ctx.fireChannelRead(msg);
            }
        }
    }
}
