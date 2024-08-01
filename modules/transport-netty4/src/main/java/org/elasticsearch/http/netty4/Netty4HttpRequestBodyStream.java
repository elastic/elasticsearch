/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.http.HttpBody;
import org.elasticsearch.transport.netty4.Netty4Utils;

/**
 * Implementation of HTTP request content stream. This implementation relies on
 * {@link io.netty.channel.ChannelConfig#setAutoRead(boolean)} to stop arrival of new chunks.
 * Otherwise, downstream might receive much more data than requested.
 * This can be achieved with {@link io.netty.handler.flow.FlowControlHandler} placed before stream
 * constructor in {@link io.netty.channel.ChannelPipeline} and after {@link io.netty.handler.codec.http.HttpRequestDecoder}.
 */
public class Netty4HttpRequestBodyStream implements HttpBody.Stream {

    private final Channel channel;
    private HttpBody.ChunkHandler handler;
    private CompositeByteBuf aggregate;
    private int requestedBytes = 0;

    public Netty4HttpRequestBodyStream(Channel channel) {
        this.channel = channel;
        channel.config().setAutoRead(false);
    }

    @Override
    public ChunkHandler handler() {
        return handler;
    }

    @Override
    public void setHandler(ChunkHandler chunkHandler) {
        this.handler = chunkHandler;
    }

    @Override
    public void requestBytes(int bytes) {
        assert handler != null : "handler must be set before requesting next chunk";
        requestedBytes += bytes;
        channel.read();
    }

    public void handleNettyContent(HttpContent httpContent) {
        assert handler != null : "handler must be set before processing http content";
        var isLast = httpContent instanceof LastHttpContent;
        var content = httpContent.content();

        if (content.readableBytes() >= requestedBytes || isLast) {
            ByteBuf sendBuf;
            if (aggregate == null) {
                sendBuf = content;
            } else {
                aggregate.addComponent(true, content);
                sendBuf = aggregate;
                aggregate = null;
            }
            requestedBytes = 0;
            handler.onNext(Netty4Utils.toReleasableBytesReference(sendBuf), isLast);
            if (isLast) {
                channel.config().setAutoRead(true);
            }
        } else {
            if (aggregate == null) {
                aggregate = channel.alloc().compositeBuffer();
            }
            aggregate.addComponent(true, content);
            requestedBytes -= content.readableBytes();
            channel.read();
        }
    }

}
