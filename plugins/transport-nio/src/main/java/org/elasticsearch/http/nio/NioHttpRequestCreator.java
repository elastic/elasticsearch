/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.nio;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;

import org.elasticsearch.ExceptionsHelper;

import java.util.List;

@ChannelHandler.Sharable
class NioHttpRequestCreator extends MessageToMessageDecoder<FullHttpRequest> {

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) {
        if (msg.decoderResult().isFailure()) {
            final Throwable cause = msg.decoderResult().cause();
            final Exception nonError;
            if (cause instanceof Exception exception) {
                nonError = exception;
            } else {
                ExceptionsHelper.maybeDieOnAnotherThread(cause);
                nonError = new Exception(cause);
            }
            out.add(new NioHttpRequest(msg.retain(), nonError));
        } else {
            out.add(new NioHttpRequest(msg.retain()));
        }
    }
}
