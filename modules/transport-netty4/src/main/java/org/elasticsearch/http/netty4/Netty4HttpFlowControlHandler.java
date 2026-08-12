/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayDeque;

/**
 * Releases at most one message per {@code read()}, followed by one {@code channelReadComplete}, to the handlers below.
 * The decoder and {@link io.netty.handler.codec.http.HttpContentDecompressor} above emit any number of messages per read,
 * while {@link Netty4HttpPipeliningHandler} below can only accept one: it hands each request to the REST layer, which may
 * fork before setting a body handler, so a chunk arriving alongside its own request would have nowhere to go.
 * <p>
 * A queued run of content chunks is combined into a single message rather than released one at a time.
 * {@link org.elasticsearch.http.HttpBody.Stream} promises one chunk per {@code next()} but not how that chunk was
 * assembled, so combining costs one round trip through the REST layer per network read instead of one per wire chunk.
 * <p>
 * This handler also clears {@code Transfer-Encoding: chunked} from requests that clients send with an empty body, which
 * needs the single message of lookahead the queue already provides.
 */
class Netty4HttpFlowControlHandler extends ChannelDuplexHandler {

    private final ArrayDeque<HttpObject> queue = new ArrayDeque<>(4);

    /**
     * Whether a downstream {@code read()} is still owed a message. Saturating rather than counting, so that repeated
     * reads with nothing to give still owe exactly one message; this is what makes
     * {@link org.elasticsearch.http.HttpBody.Stream#next()} deduplicate.
     */
    private boolean readPending;

    @Override
    public void read(ChannelHandlerContext ctx) {
        if (readPending) {
            return;
        }
        readPending = true;
        if (emit(ctx) == false) {
            ctx.read();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof HttpObject : "unexpected inbound message [" + msg.getClass() + "]";
        queue.addLast((HttpObject) msg);
        if (readPending) {
            emit(ctx);
        }
        // No read here: only channelReadComplete tells us the batch is drained, so a handler added above must either
        // forward that event or, if it withholds messages of its own, fire one after releasing them the way
        // Netty4HttpHeaderValidator does. Reading here instead would fetch a message the rest of the batch was about
        // to deliver anyway.
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        if (readPending && emit(ctx) == false) {
            ctx.read();
        }
        // not forwarded: emit() synthesises one read-complete per released message
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        releaseQueued();
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        releaseQueued();
    }

    private boolean emit(ChannelHandlerContext ctx) {
        final HttpObject msg = poll(ctx);
        if (msg == null) {
            return false;
        }
        readPending = false; // cleared before firing, so the flag is already correct if a handler below reads back into us
        ctx.fireChannelRead(msg);
        ctx.fireChannelReadComplete();
        return true;
    }

    /**
     * @return the next message to release, or {@code null} if nothing can be released yet because the queue is empty or
     *         its head is a chunked request whose first content chunk has not arrived
     */
    private HttpObject poll(ChannelHandlerContext ctx) {
        final HttpObject head = queue.peekFirst();
        if (head == null) {
            return null;
        }
        if (head instanceof HttpRequest request) {
            final boolean needsEmptyBodyCheck = request.decoderResult().isSuccess() && HttpUtil.isTransferEncodingChunked(request);
            if (needsEmptyBodyCheck && queue.size() < 2) {
                return null;
            }
            queue.pollFirst();
            if (needsEmptyBodyCheck && isEmptyLastContent(queue.peekFirst())) {
                HttpUtil.setTransferEncodingChunked(request, false);
            }
            return request;
        }
        return coalesceContent(ctx);
    }

    /**
     * Drains the run of {@link HttpContent} at the head of the queue into one message. The run includes the first
     * {@link LastHttpContent}, so that the combined message still terminates the body, and never crosses an
     * {@link HttpRequest}. A run of one is returned untouched.
     */
    private HttpContent coalesceContent(ChannelHandlerContext ctx) {
        final HttpContent first = (HttpContent) queue.pollFirst();
        if (first instanceof LastHttpContent || queue.peekFirst() instanceof HttpContent == false) {
            return first;
        }

        // A loose upper bound on the run: over-sizing is free because the component array is capped at 16 regardless,
        // whereas under-sizing would make the composite consolidate, which copies. Allocating via ctx keeps the result
        // leak-aware, which is the point of Netty4LeakDetectionHandler above.
        final CompositeByteBuf composite = ctx.alloc().compositeBuffer(queue.size() + 1);
        addComponent(composite, first.content());
        LastHttpContent last = null;
        while (last == null && queue.peekFirst() instanceof HttpContent next) {
            queue.pollFirst();
            addComponent(composite, next.content());
            if (next instanceof LastHttpContent lastContent) {
                last = lastContent;
            }
        }

        if (last == null) {
            return new DefaultHttpContent(composite);
        }
        final DefaultLastHttpContent coalesced = new DefaultLastHttpContent(composite);
        coalesced.trailingHeaders().set(last.trailingHeaders());
        coalesced.setDecoderResult(last.decoderResult());
        return coalesced;
    }

    /**
     * Hands one component buffer to the composite, which takes ownership of it, so the {@link HttpContent} wrapper it
     * came from must not be released afterwards. Empty buffers are dropped rather than added: an interior empty
     * component makes {@link CompositeByteBuf#nioBuffers} yield a buffer with no backing array, which
     * {@link org.elasticsearch.transport.netty4.Netty4Utils#toBytesReference} cannot read.
     */
    private static void addComponent(CompositeByteBuf composite, ByteBuf component) {
        if (component.isReadable()) {
            composite.addComponent(true, component);
        } else {
            component.release();
        }
    }

    private static boolean isEmptyLastContent(HttpObject msg) {
        return msg instanceof LastHttpContent last && last.content().readableBytes() == 0;
    }

    private void releaseQueued() {
        HttpObject queued;
        while ((queued = queue.pollFirst()) != null) {
            ReferenceCountUtil.release(queued);
        }
    }
}
