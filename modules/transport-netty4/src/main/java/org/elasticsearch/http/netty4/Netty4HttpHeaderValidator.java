/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.Transports;

import java.util.ArrayDeque;

import static org.elasticsearch.http.netty4.Netty4HttpHeaderValidator.State.DROPPING_DATA_PERMANENTLY;
import static org.elasticsearch.http.netty4.Netty4HttpHeaderValidator.State.DROPPING_DATA_UNTIL_NEXT_REQUEST;
import static org.elasticsearch.http.netty4.Netty4HttpHeaderValidator.State.FORWARDING_DATA_UNTIL_NEXT_REQUEST;
import static org.elasticsearch.http.netty4.Netty4HttpHeaderValidator.State.QUEUEING_DATA;
import static org.elasticsearch.http.netty4.Netty4HttpHeaderValidator.State.WAITING_TO_START;

public class Netty4HttpHeaderValidator extends ChannelInboundHandlerAdapter {

    public static final TriConsumer<HttpRequest, Channel, ActionListener<Void>> NOOP_VALIDATOR = ((
        httpRequest,
        channel,
        listener) -> listener.onResponse(null));

    private final TriConsumer<HttpRequest, Channel, ActionListener<Void>> validator;
    private final ThreadContext threadContext;
    private ArrayDeque<HttpObject> pending = new ArrayDeque<>(4);
    private State state = WAITING_TO_START;

    public Netty4HttpHeaderValidator(TriConsumer<HttpRequest, Channel, ActionListener<Void>> validator, ThreadContext threadContext) {
        this.validator = validator;
        this.threadContext = threadContext;
    }

    State getState() {
        return state;
    }

    @SuppressWarnings("fallthrough")
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert msg instanceof HttpObject;
        final HttpObject httpObject = (HttpObject) msg;

        switch (state) {
            case WAITING_TO_START:
                assert pending.isEmpty();
                pending.add(ReferenceCountUtil.retain(httpObject));
                requestStart(ctx);
                assert state == QUEUEING_DATA;
                break;
            case QUEUEING_DATA:
                pending.add(ReferenceCountUtil.retain(httpObject));
                break;
            case FORWARDING_DATA_UNTIL_NEXT_REQUEST:
                assert pending.isEmpty();
                if (httpObject instanceof LastHttpContent) {
                    state = WAITING_TO_START;
                }
                ctx.fireChannelRead(httpObject);
                break;
            case DROPPING_DATA_UNTIL_NEXT_REQUEST:
                assert pending.isEmpty();
                if (httpObject instanceof LastHttpContent) {
                    state = WAITING_TO_START;
                }
                // fall-through
            case DROPPING_DATA_PERMANENTLY:
                assert pending.isEmpty();
                ReferenceCountUtil.release(httpObject); // consume without enqueuing
                break;
        }

        setAutoReadForState(ctx, state);
    }

    private void requestStart(ChannelHandlerContext ctx) {
        assert state == WAITING_TO_START;

        if (pending.isEmpty()) {
            return;
        }

        final HttpObject httpObject = pending.getFirst();
        final HttpRequest httpRequest;
        if (httpObject instanceof HttpRequest && httpObject.decoderResult().isSuccess()) {
            // a properly decoded HTTP start message is expected to begin validation
            // anything else is probably an error that the downstream HTTP message aggregator will have to handle
            httpRequest = (HttpRequest) httpObject;
        } else {
            httpRequest = null;
        }

        state = QUEUEING_DATA;

        if (httpRequest == null) {
            // this looks like a malformed request and will forward without validation
            ctx.channel().eventLoop().submit(() -> forwardFullRequest(ctx));
        } else {
            Transports.assertDefaultThreadContext(threadContext);
            // this prevents thread-context changes to propagate to the validation listener
            // atm, the validation listener submits to the event loop executor, which doesn't know about the ES thread-context,
            // so this is just a defensive play, in case the code inside the listener changes to not use the event loop executor
            ContextPreservingActionListener<Void> contextPreservingActionListener = new ContextPreservingActionListener<>(
                threadContext.wrapRestorable(threadContext.newStoredContext(false)),
                ActionListener.wrap(aVoid ->
                // Always use "Submit" to prevent reentrancy concerns if we are still on event loop
                ctx.channel().eventLoop().submit(() -> forwardFullRequest(ctx)),
                    e -> ctx.channel().eventLoop().submit(() -> forwardRequestWithDecoderExceptionAndNoContent(ctx, e))
                )
            );
            // this prevents thread-context changes to propagate beyond the validation, as netty worker threads are reused
            try (ThreadContext.StoredContext ignore = threadContext.newStoredContext(false)) {
                validator.apply(httpRequest, ctx.channel(), contextPreservingActionListener);
            }
        }
    }

    private void forwardFullRequest(ChannelHandlerContext ctx) {
        Transports.assertDefaultThreadContext(threadContext);
        assert ctx.channel().eventLoop().inEventLoop();
        assert ctx.channel().config().isAutoRead() == false;
        assert state == QUEUEING_DATA;

        boolean fullRequestForwarded = forwardData(ctx, pending);

        assert fullRequestForwarded || pending.isEmpty();
        if (fullRequestForwarded) {
            state = WAITING_TO_START;
            requestStart(ctx);
        } else {
            state = FORWARDING_DATA_UNTIL_NEXT_REQUEST;
        }

        assert state == WAITING_TO_START || state == QUEUEING_DATA || state == FORWARDING_DATA_UNTIL_NEXT_REQUEST;
        setAutoReadForState(ctx, state);
    }

    private void forwardRequestWithDecoderExceptionAndNoContent(ChannelHandlerContext ctx, Exception e) {
        Transports.assertDefaultThreadContext(threadContext);
        assert ctx.channel().eventLoop().inEventLoop();
        assert ctx.channel().config().isAutoRead() == false;
        assert state == QUEUEING_DATA;

        HttpObject messageToForward = pending.getFirst();
        boolean fullRequestDropped = dropData(pending);
        if (messageToForward instanceof HttpContent) {
            // if the request to forward contained data (which got dropped), replace with empty data
            messageToForward = ((HttpContent) messageToForward).replace(Unpooled.EMPTY_BUFFER);
        }
        messageToForward.setDecoderResult(DecoderResult.failure(e));
        ctx.fireChannelRead(messageToForward);

        assert fullRequestDropped || pending.isEmpty();
        if (fullRequestDropped) {
            state = WAITING_TO_START;
            requestStart(ctx);
        } else {
            state = DROPPING_DATA_UNTIL_NEXT_REQUEST;
        }

        assert state == WAITING_TO_START || state == QUEUEING_DATA || state == DROPPING_DATA_UNTIL_NEXT_REQUEST;
        setAutoReadForState(ctx, state);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        state = DROPPING_DATA_PERMANENTLY;
        while (true) {
            if (dropData(pending) == false) {
                break;
            }
        }
        super.channelInactive(ctx);
    }

    private static boolean forwardData(ChannelHandlerContext ctx, ArrayDeque<HttpObject> pending) {
        final int pendingMessages = pending.size();
        try {
            HttpObject toForward;
            while ((toForward = pending.poll()) != null) {
                ctx.fireChannelRead(toForward);
                ReferenceCountUtil.release(toForward); // reference cnt incremented when enqueued
                if (toForward instanceof LastHttpContent) {
                    return true;
                }
            }
            return false;
        } finally {
            maybeResizePendingDown(pendingMessages, pending);
        }
    }

    private static boolean dropData(ArrayDeque<HttpObject> pending) {
        final int pendingMessages = pending.size();
        try {
            HttpObject toDrop;
            while ((toDrop = pending.poll()) != null) {
                ReferenceCountUtil.release(toDrop, 2); // 1 for enqueuing, 1 for consuming
                if (toDrop instanceof LastHttpContent) {
                    return true;
                }
            }
            return false;
        } finally {
            maybeResizePendingDown(pendingMessages, pending);
        }
    }

    private static void maybeResizePendingDown(int largeSize, ArrayDeque<HttpObject> pending) {
        if (pending.size() <= 4 && largeSize > 32) {
            // Prevent the ArrayDeque from becoming forever large due to a single large message.
            ArrayDeque<HttpObject> old = pending;
            pending = new ArrayDeque<>(4);
            pending.addAll(old);
        }
    }

    private static void setAutoReadForState(ChannelHandlerContext ctx, State state) {
        ctx.channel().config().setAutoRead((state == QUEUEING_DATA || state == DROPPING_DATA_PERMANENTLY) == false);
    }

    enum State {
        WAITING_TO_START,
        QUEUEING_DATA,
        FORWARDING_DATA_UNTIL_NEXT_REQUEST,
        DROPPING_DATA_UNTIL_NEXT_REQUEST,
        DROPPING_DATA_PERMANENTLY
    }
}
