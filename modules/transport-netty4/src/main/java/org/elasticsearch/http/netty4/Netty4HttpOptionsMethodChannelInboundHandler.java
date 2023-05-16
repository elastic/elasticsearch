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
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;

/**
 * This channel inbound handler drops the content of HTTP requests with the OPTIONS method, for optimisation purposes.
 * The HTTP specifications doesn't currently define any use for the body of an OPTIONS request,
 * see <a href="https://www.rfc-editor.org/rfc/rfc7231#section-4.3.7">RFC 7231, 4.3.7</a>, and
 * Elasticsearch completely ignores it.
 */
public final class Netty4HttpOptionsMethodChannelInboundHandler extends SimpleChannelInboundHandler<HttpObject> {

    private boolean dropContent;

    public Netty4HttpOptionsMethodChannelInboundHandler() {
        super(false);
        dropContent = false;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpRequest httpRequest) {
            if (httpRequest.method() == HttpMethod.OPTIONS) {
                dropContent = true;
            } else {
                dropContent = false;
            }
        }
        if (dropContent) {
            if (msg instanceof HttpRequest httpRequest) {
                httpRequest.headers()
                    // also remove "Representational Metadata" headers https://www.rfc-editor.org/rfc/rfc7231#section-3.1.2.1
                    .remove(HttpHeaderNames.CONTENT_TYPE)
                    .remove(HttpHeaderNames.CONTENT_ENCODING)
                    .remove(HttpHeaderNames.CONTENT_LANGUAGE)
                    .remove(HttpHeaderNames.CONTENT_LOCATION)
                    // <a href="https://www.rfc-editor.org/rfc/rfc7230#section-3.3.2">RFC 7230, 3.3.2</a>
                    // requests that don't contain a payload body and the method semantics doesn't anticipate a body
                    // should not set the "Content-Length", and, by inference, neither the "Transfer-Encoding" headers
                    .remove(HttpHeaderNames.CONTENT_LENGTH)
                    .remove(HttpHeaderNames.TRANSFER_ENCODING);
            }
            // Forward empty {@code HttpContent} content pieces rather than not forward them at all.
            // This is preferable because it keeps the logic simpler (eg not having to worry about forwarding the {@code LastHttpContent}).
            if (msg instanceof HttpContent toReplace) {
                // release content
                ReferenceCountUtil.release(msg);
                // replace the released buffer
                msg = toReplace.replace(Unpooled.EMPTY_BUFFER);
            }
        }
        ctx.fireChannelRead(msg);
    }
}
