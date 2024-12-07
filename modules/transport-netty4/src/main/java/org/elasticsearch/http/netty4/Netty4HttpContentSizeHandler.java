/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.core.SuppressForbidden;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;

/**
 * Provides handling for Expect header and content size. Implements HTTP1.1 spec.
 * Allows {@code Expect: 100-continue} header only. Other Expect headers will be rejected with
 * {@code 417 Expectation Failed} reason.
 * <br>
 * Replies {@code 100 Continue} to requests with allowed maxContentLength.
 * <br>
 * Replies {@code 413 Request Entity Too Large} when content size exceeds maxContentLength.
 * Clients sending oversized requests with Expect: 100-continue included are allowed to reuse same
 * connection as long as they dont send content after rejection. Otherwise, when client started to
 * send oversized content, we cannot safely accept it. Connection will be closed.
 * <br><br>
 * TODO: move to RestController to allow content limits per RestHandler.
 *  Ideally we should be able to handle Continue and oversized request in the RestController.
 *  But that introduces a few challenges, basically re-implementation of HTTP protocol at the RestController:
 * <ul>
 *     <li>
 *         100 Continue is interim response, means RestChannel will send 2 responses for a single request. See
 *         <a href="https://www.rfc-editor.org/rfc/rfc9110.html#status.100">rfc9110.html#status.100</a>
 *     </li>
 *     <li>
 *         RestChannel should be able to close underlying HTTP channel connection.
 *     </li>
 * </ul>
 */
@SuppressForbidden(reason = "use of default ChannelFutureListener's CLOSE and CLOSE_ON_FAILURE")
public class Netty4HttpContentSizeHandler extends ChannelInboundHandlerAdapter {

    // copied from HttpObjectAggregator
    private static final FullHttpResponse CONTINUE = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.CONTINUE,
        Unpooled.EMPTY_BUFFER
    );
    private static final FullHttpResponse EXPECTATION_FAILED = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.EXPECTATION_FAILED,
        Unpooled.EMPTY_BUFFER
    );
    private static final FullHttpResponse TOO_LARGE_CLOSE = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
        Unpooled.EMPTY_BUFFER
    );
    private static final FullHttpResponse TOO_LARGE = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
        Unpooled.EMPTY_BUFFER
    );

    static {
        EXPECTATION_FAILED.headers().set(CONTENT_LENGTH, 0);
        TOO_LARGE.headers().set(CONTENT_LENGTH, 0);

        TOO_LARGE_CLOSE.headers().set(CONTENT_LENGTH, 0);
        TOO_LARGE_CLOSE.headers().set(CONNECTION, HttpHeaderValues.CLOSE);
    }

    private final HttpRequestDecoder decoder;
    private final int maxContentLength;
    private boolean ignoreFollowingContent = false;
    private boolean contentNotAllowed = false;
    private int contentLength = 0;

    public Netty4HttpContentSizeHandler(HttpRequestDecoder decoder, int maxContentLength) {
        this.decoder = decoder;
        this.maxContentLength = maxContentLength;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof HttpObject;
        if (msg instanceof HttpRequest request) {
            handleRequest(ctx, request);
        } else {
            handleContent(ctx, (HttpContent) msg);
        }
    }

    private void replyAndForbidFollowingContent(ChannelHandlerContext ctx, FullHttpResponse errResponse) {
        decoder.reset(); // reset decoder to skip following content
        ctx.writeAndFlush(errResponse.retainedDuplicate()).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        contentNotAllowed = true; // some content might be already in the pipeline, we need to catch it
        ignoreFollowingContent = true;
    }

    private void handleRequest(ChannelHandlerContext ctx, HttpRequest request) {
        final var expectValue = request.headers().get(HttpHeaderNames.EXPECT);

        boolean isContinueExpected = false;
        if (expectValue != null) {
            if (HttpHeaderValues.CONTINUE.toString().equalsIgnoreCase(expectValue)) {
                isContinueExpected = true;
            } else {
                replyAndForbidFollowingContent(ctx, EXPECTATION_FAILED);
                return;
            }
        }

        boolean isOversized = HttpUtil.getContentLength(request, -1) > maxContentLength;
        if (isOversized) {
            if (isContinueExpected) {
                // Client is allowed to send content without waiting for Continue.
                // See https://www.rfc-editor.org/rfc/rfc9110.html#section-10.1.1-11.3
                //
                // Mark following content as forbidden to prevent unbounded content after Expect failed.
                replyAndForbidFollowingContent(ctx, TOO_LARGE);
            } else {
                // Client is sending oversized content, we cannot safely take it. Closing channel.
                ctx.writeAndFlush(TOO_LARGE_CLOSE.retainedDuplicate()).addListener(ChannelFutureListener.CLOSE);
                ignoreFollowingContent = true;
            }
        } else {
            if (isContinueExpected) {
                ctx.writeAndFlush(CONTINUE.retainedDuplicate());
                HttpUtil.set100ContinueExpected(request, false);
            }
            ignoreFollowingContent = false;
            contentNotAllowed = false;
            contentLength = 0;
            ctx.fireChannelRead(request);
        }
    }

    private void handleContent(ChannelHandlerContext ctx, HttpContent httpContent) {
        if (contentNotAllowed && httpContent != LastHttpContent.EMPTY_LAST_CONTENT) {
            httpContent.release();
            ctx.close();
        } else if (ignoreFollowingContent) {
            httpContent.release();
        } else {
            contentLength += httpContent.content().readableBytes();
            if (contentLength > maxContentLength) {
                ignoreFollowingContent = true;
                httpContent.release();
                // Client is sending oversized content, we cannot safely take it. Closing channel.
                ctx.writeAndFlush(TOO_LARGE_CLOSE.retainedDuplicate()).addListener(ChannelFutureListener.CLOSE);
                return;
            }
            ctx.fireChannelRead(httpContent);
        }
    }
}
