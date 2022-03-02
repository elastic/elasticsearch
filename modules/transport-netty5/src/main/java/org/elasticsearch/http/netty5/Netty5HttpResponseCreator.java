/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty5;

import io.netty.buffer.api.Buffer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpResponse;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.transport.netty5.NettyAllocator;

import java.util.List;

/**
 * Split up large responses to prevent batch compression down the pipeline.
 */
@ChannelHandler.Sharable
final class Netty5HttpResponseCreator extends MessageToMessageEncoder<Netty4HttpResponse> {

    static final Netty5HttpResponseCreator INSTANCE = new Netty5HttpResponseCreator();

    private Netty5HttpResponseCreator() {}

    private static final String DO_NOT_SPLIT = "es.unsafe.do_not_split_http_responses";

    private static final boolean DO_NOT_SPLIT_HTTP_RESPONSES;
    private static final int SPLIT_THRESHOLD;

    static {
        DO_NOT_SPLIT_HTTP_RESPONSES = Booleans.parseBoolean(System.getProperty(DO_NOT_SPLIT), false);
        // Netty will add some header bytes if it compresses this message. So we downsize slightly.
        SPLIT_THRESHOLD = (int) (NettyAllocator.suggestedMaxAllocationSize() * 0.99);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Netty4HttpResponse msg, List<Object> out) {
        if (DO_NOT_SPLIT_HTTP_RESPONSES || msg.payload().readableBytes() <= SPLIT_THRESHOLD) {
            out.add(msg);
        } else {
            HttpResponse response = new DefaultHttpResponse(msg.protocolVersion(), msg.status(), msg.headers());
            out.add(response);
            Buffer content = msg.payload();
            while (content.readableBytes() > SPLIT_THRESHOLD) {
                out.add(new DefaultHttpContent(content.readSplit(SPLIT_THRESHOLD)));
            }
            out.add(new DefaultLastHttpContent(content.readSplit(content.readableBytes())));
        }
    }
}
