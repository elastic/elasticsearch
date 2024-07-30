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
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpContent;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.function.Consumer;

/**
 * Implementation of HTTP request content stream. This implementation relies on
 * {@link io.netty.channel.ChannelConfig#setAutoRead(boolean)} to stop arrival of new chunks.
 * Otherwise, downstream might receive much more data than requested.
 * This can be achieved with {@link io.netty.handler.flow.FlowControlHandler} placed before stream
 * constructor in {@link io.netty.channel.ChannelPipeline} and after {@link io.netty.handler.codec.http.HttpRequestDecoder}.
 */
public class Netty4HttpRequestContentStream implements HttpContent.Stream {

    private final Channel channel;
    private Consumer<Chunk> handler;
    private CompositeByteBuf aggregate;
    private int requestedBytes = 0;
    private boolean cancelled = false;

    public Netty4HttpRequestContentStream(Channel channel) {
        this.channel = channel;
        channel.config().setAutoRead(false);
    }

    @Override
    public Consumer<Chunk> handler() {
        return handler;
    }

    @Override
    public void setHandler(Consumer<Chunk> chunkHandler) {
        this.handler = chunkHandler;
    }

    @Override
    public void request(int bytes) {
        assert handler != null : "handler must be set before requesting next chunk";
        requestedBytes += bytes;
        channel.read();
    }

    @Override
    public void cancel() {
        cancelled = true;
        channel.config().setAutoRead(true);
    }

    public void handleNettyContent(io.netty.handler.codec.http.HttpContent httpContent) {
        if (cancelled) {
            httpContent.release();
            return;
        }

        assert handler != null : "handler must be set before processing http content";
        var isLast = httpContent instanceof LastHttpContent;
        var content = httpContent.content();

        if (aggregate == null && content.readableBytes() >= requestedBytes) {
            // network chunk is large enough and there is no ongoing aggregation
            requestedBytes = 0;
            handler.accept(new Netty4ContentChunk(content, isLast));
        } else if (content.readableBytes() >= requestedBytes) {
            // there is ongoing aggregation, and we reached requestBytes limit, send aggregated chunk downstream
            aggregate.addComponent(true, content);
            requestedBytes = 0;
            handler.accept(new Netty4ContentChunk(aggregate, isLast));
            aggregate = null;
        } else {
            // accumulation step
            if (aggregate == null) {
                aggregate = channel.alloc().compositeBuffer();
            }
            aggregate.addComponent(true, content);
            requestedBytes -= content.readableBytes();
            channel.read();
        }
        if (isLast) {
            if (aggregate != null) {
                handler.accept(new Netty4ContentChunk(aggregate, true));
            }
            channel.config().setAutoRead(true);
        }
    }

    static class Netty4ContentChunk implements Chunk {

        private final ByteBuf nettyBuffer;
        private final BytesReference bytes;
        private final boolean isLast;

        Netty4ContentChunk(ByteBuf nettyBuffer, boolean isLast) {
            this.nettyBuffer = nettyBuffer;
            this.bytes = Netty4Utils.toBytesReference(nettyBuffer);
            this.isLast = isLast;
        }

        @Override
        public BytesReference bytes() {
            return bytes;
        }

        @Override
        public void release() {
            nettyBuffer.release();
        }

        @Override
        public boolean isLast() {
            return isLast;
        }
    }
}
