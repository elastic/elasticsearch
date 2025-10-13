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
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
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

import org.elasticsearch.core.SuppressForbidden;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;

/**
 * Provides handling for 'Expect' header and content size. Implements HTTP1.1 spec.
 * Allows {@code Expect: 100-continue} header only. Other 'Expect' headers will be rejected with
 * {@code 417 Expectation Failed} reason.
 * <br>
 * Replies {@code 100 Continue} to requests with allowed maxContentLength.
 * <br>
 * Replies {@code 413 Request Entity Too Large} when content size exceeds maxContentLength.
 *
 * Channel can be reused for requests with "Expect:100-Continue" header that exceed allowed content length,
 * as long as request does not include content. If oversized request already contains content then
 * we cannot safely proceed and connection will be closed.
 * <br><br>
 * TODO: move to RestController to allow content limits per RestHandler.
 *  Ideally we should be able to handle Continue and oversized request in the RestController.
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

    // copied from netty's HttpObjectAggregator
    static final FullHttpResponse CONTINUE = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.CONTINUE,
        Unpooled.EMPTY_BUFFER
    );
    static final FullHttpResponse EXPECTATION_FAILED_CLOSE = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.EXPECTATION_FAILED,
        Unpooled.EMPTY_BUFFER,
        new DefaultHttpHeaders().add(CONTENT_LENGTH, 0).add(CONNECTION, HttpHeaderValues.CLOSE),
        EmptyHttpHeaders.INSTANCE
    );
    static final FullHttpResponse TOO_LARGE_CLOSE = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
        Unpooled.EMPTY_BUFFER,
        new DefaultHttpHeaders().add(CONTENT_LENGTH, 0).add(CONNECTION, HttpHeaderValues.CLOSE),
        EmptyHttpHeaders.INSTANCE
    );
    static final FullHttpResponse TOO_LARGE = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
        Unpooled.EMPTY_BUFFER,
        new DefaultHttpHeaders().add(CONTENT_LENGTH, 0),
        EmptyHttpHeaders.INSTANCE
    );

    private final int maxContentLength;
    private final HttpRequestDecoder decoder; // need to reset decoder after sending 413
    private int currentContentLength; // chunked encoding does not provide content length, need to track actual length
    private boolean ignoreContent;

    public Netty4HttpContentSizeHandler(HttpRequestDecoder decoder, int maxContentLength) {
        this.maxContentLength = maxContentLength;
        this.decoder = decoder;
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

    private void handleRequest(ChannelHandlerContext ctx, HttpRequest request) {
        ignoreContent = true;
        if (request.decoderResult().isFailure()) {
            ctx.fireChannelRead(request);
            return;
        }

        final var expectValue = request.headers().get(HttpHeaderNames.EXPECT);
        boolean isContinueExpected = false;
        // Only "Expect: 100-Continue" header is supported
        if (expectValue != null) {
            if (HttpHeaderValues.CONTINUE.toString().equalsIgnoreCase(expectValue)) {
                isContinueExpected = true;
            } else {
                ctx.writeAndFlush(EXPECTATION_FAILED_CLOSE.retainedDuplicate()).addListener(ChannelFutureListener.CLOSE);
                ctx.read();
                return;
            }
        }

        boolean isOversized = HttpUtil.getContentLength(request, -1) > maxContentLength;
        if (isOversized) {
            if (isContinueExpected) {
                // Client is allowed to send content without waiting for Continue.
                // See https://www.rfc-editor.org/rfc/rfc9110.html#section-10.1.1-11.3
                // this content will result in HttpRequestDecoder failure and send downstream
                decoder.reset();
            }
            ctx.writeAndFlush(TOO_LARGE.retainedDuplicate()).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
            ctx.read();
        } else {
            ignoreContent = false;
            currentContentLength = 0;
            if (isContinueExpected) {
                ctx.writeAndFlush(CONTINUE.retainedDuplicate());
                HttpUtil.set100ContinueExpected(request, false);
            }
            ctx.fireChannelRead(request);
        }
    }

    private void handleContent(ChannelHandlerContext ctx, HttpContent msg) {
        if (ignoreContent) {
            msg.release();
            ctx.read();
        } else {
            currentContentLength += msg.content().readableBytes();
            if (currentContentLength > maxContentLength) {
                msg.release();
                ctx.writeAndFlush(TOO_LARGE_CLOSE.retainedDuplicate()).addListener(ChannelFutureListener.CLOSE);
                ctx.read();
            } else {
                ctx.fireChannelRead(msg);
            }
        }
    }

}
