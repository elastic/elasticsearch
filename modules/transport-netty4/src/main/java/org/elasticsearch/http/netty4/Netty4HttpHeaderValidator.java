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
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.TriConsumer;

import java.util.ArrayDeque;

public class Netty4HttpHeaderValidator extends ChannelInboundHandlerAdapter {

    public static final TriConsumer<HttpRequest, Channel, ActionListener<Void>> NOOP_VALIDATOR = ((
        httpRequest,
        channel,
        listener) -> listener.onResponse(null));

    private final TriConsumer<HttpRequest, Channel, ActionListener<Void>> validator;
    private ArrayDeque<HttpObject> pending = new ArrayDeque<>(4);
    private STATE state = STATE.WAITING_TO_START;

    public Netty4HttpHeaderValidator(TriConsumer<HttpRequest, Channel, ActionListener<Void>> validator) {
        this.validator = validator;
    }

    STATE getState() {
        return state;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert msg instanceof HttpObject;
        final HttpObject httpObject = (HttpObject) msg;

        if (state == STATE.WAITING_TO_START) {
            assert pending.isEmpty();
            pending.add(ReferenceCountUtil.retain(httpObject));
            requestStart(ctx);
        } else if (state == STATE.QUEUEING_DATA) {
            pending.add(ReferenceCountUtil.retain(httpObject));
        } else if (state == STATE.HANDLING_QUEUED_DATA) {
            pending.add(ReferenceCountUtil.retain(httpObject));
            // Immediately return as this can only happen from a reentrant read(). We do not want to change
            // autoread in this case.
            return;
        } else if (state == STATE.FORWARDING_DATA) {
            assert pending.isEmpty();
            if (httpObject instanceof LastHttpContent) {
                state = STATE.WAITING_TO_START;
            }
            ctx.fireChannelRead(httpObject);
        } else if (state == STATE.DROPPING_DATA_PERMANENTLY || state == STATE.DROPPING_DATA_UNTIL_NEXT_REQUEST) {
            assert pending.isEmpty();
            ReferenceCountUtil.release(httpObject); // consume
            if (state == STATE.DROPPING_DATA_UNTIL_NEXT_REQUEST && httpObject instanceof LastHttpContent) {
                state = STATE.WAITING_TO_START;
            }
        } else {
            throw new AssertionError("Unknown state: " + state);
        }

        setAutoReadForState(ctx, state);
    }

    private void requestStart(ChannelHandlerContext ctx) {
        assert pending.isEmpty() == false;
        assert state == STATE.WAITING_TO_START;

        HttpRequest httpRequest = forwardUntilFirstProperRequestStart(ctx, pending);
        if (httpRequest == null) {
            return;
        }

        state = STATE.QUEUEING_DATA;
        validator.apply(httpRequest, ctx.channel(), new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                // Always use "Submit" to prevent reentrancy concerns if we are still on event loop
                ctx.channel().eventLoop().submit(() -> validationSuccess(ctx));
            }

            @Override
            public void onFailure(Exception e) {
                // Always use "Submit" to prevent reentrancy concerns if we are still on event loop
                ctx.channel().eventLoop().submit(() -> validationFailure(ctx, e));
            }
        });
    }

    private void validationSuccess(ChannelHandlerContext ctx) {
        assert ctx.channel().eventLoop().inEventLoop();
        assert state == STATE.QUEUEING_DATA;

        state = STATE.HANDLING_QUEUED_DATA;
        boolean fullRequestForwarded = forwardData(ctx, pending);

        assert fullRequestForwarded || pending.isEmpty();
        if (fullRequestForwarded) {
            state = STATE.WAITING_TO_START;
            if (pending.isEmpty() == false) {
                requestStart(ctx);
            }
        } else {
            state = STATE.FORWARDING_DATA;
        }

        setAutoReadForState(ctx, state);
    }

    private void validationFailure(ChannelHandlerContext ctx, Exception e) {
        assert ctx.channel().eventLoop().inEventLoop();
        assert state == STATE.QUEUEING_DATA;

        state = STATE.HANDLING_QUEUED_DATA;
        HttpMessage messageToForward = (HttpMessage) pending.remove();
        boolean fullRequestConsumed = messageToForward instanceof LastHttpContent;
        if (fullRequestConsumed == false) {
            fullRequestConsumed = dropData(pending);
        }
        if (messageToForward instanceof HttpContent toRelease) {
            // drop the original content
            toRelease.release(2); // 1 for enqueuing, 1 for consuming
            // replace with empty content
            messageToForward = (HttpMessage) toRelease.replace(Unpooled.EMPTY_BUFFER);
        }
        messageToForward.setDecoderResult(DecoderResult.failure(e));
        ctx.fireChannelRead(messageToForward);

        assert fullRequestConsumed || pending.isEmpty();
        if (fullRequestConsumed) {
            state = STATE.WAITING_TO_START;
            if (pending.isEmpty() == false) {
                requestStart(ctx);
            }
        } else {
            state = STATE.DROPPING_DATA_UNTIL_NEXT_REQUEST;
        }

        setAutoReadForState(ctx, state);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        state = STATE.DROPPING_DATA_PERMANENTLY;
        while (true) {
            if (dropData(pending) == false) {
                break;
            }
        }
        super.channelInactive(ctx);
    }

    private static HttpRequest forwardUntilFirstProperRequestStart(ChannelHandlerContext ctx, ArrayDeque<HttpObject> pending) {
        assert pending.isEmpty() == false;
        boolean isStartMessage;
        HttpObject httpObject;
        do {
            httpObject = pending.getFirst();
            isStartMessage = httpObject instanceof HttpRequest;
            if (isStartMessage && httpObject.decoderResult().isSuccess()) {
                return (HttpRequest) httpObject;
            }
            // a properly decoded HTTP start message is expected to begin validation
            // anything else is probably an error that the downstream HTTP message aggregator will have to handle
            ctx.fireChannelRead(pending.pollFirst());
            ReferenceCountUtil.release(httpObject); // reference count was increased when enqueued
        } while (pending.isEmpty() == false);
        return null;
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

    private static void setAutoReadForState(ChannelHandlerContext ctx, STATE state) {
        ctx.channel().config().setAutoRead((state == STATE.QUEUEING_DATA || state == STATE.DROPPING_DATA_PERMANENTLY) == false);
    }

    enum STATE {
        WAITING_TO_START,
        QUEUEING_DATA,
        // This is an intermediate state in case an event handler down the line triggers a reentrant read().
        // Data will be intermittently queued for later handling while in this state.
        HANDLING_QUEUED_DATA,
        FORWARDING_DATA,
        DROPPING_DATA_UNTIL_NEXT_REQUEST,
        DROPPING_DATA_PERMANENTLY
    }
}
