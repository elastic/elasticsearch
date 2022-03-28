/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty5;

import io.netty5.channel.ChannelHandler;
import io.netty5.channel.ChannelHandlerContext;
import io.netty5.handler.codec.MessageToMessageDecoder;
import io.netty5.handler.codec.http.FullHttpRequest;

import org.elasticsearch.ExceptionsHelper;

@ChannelHandler.Sharable
final class Netty5HttpRequestCreator extends MessageToMessageDecoder<FullHttpRequest> {

    static final Netty5HttpRequestCreator INSTANCE = new Netty5HttpRequestCreator();

    private Netty5HttpRequestCreator() {}

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg) {
        if (msg.decoderResult().isFailure()) {
            final Throwable cause = msg.decoderResult().cause();
            final Exception nonError;
            if (cause instanceof Error) {
                ExceptionsHelper.maybeDieOnAnotherThread(cause);
                nonError = new Exception(cause);
            } else {
                nonError = (Exception) cause;
            }
            ctx.fireChannelRead(new Netty5HttpRequest(msg, nonError));
        } else {
            ctx.fireChannelRead(new Netty5HttpRequest(msg));
        }
    }
}
