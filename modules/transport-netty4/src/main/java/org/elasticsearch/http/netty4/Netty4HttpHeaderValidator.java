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

        ctx.channel().config().setAutoRead(shouldRead());
    }

    private void requestStart(ChannelHandlerContext ctx) {
        assert pending.isEmpty() == false;
        assert state == STATE.WAITING_TO_START;

        HttpObject httpObject = pending.getFirst();
        boolean isStartMessage = httpObject instanceof HttpRequest;

        if (isStartMessage == false || httpObject.decoderResult().isSuccess() == false) {
            // a properly decoded HTTP start message is expected
            // anything else is probably an error that the HTTP message aggregator will have to handle
            ctx.fireChannelRead(pending.pollFirst());
            ReferenceCountUtil.release(httpObject); // reference count was increased when enqueued
            // state = STATE.WAITING_TO_START keep waiting for a valid HTTP start message
            // TODO forward more than a single request
            return;
        }

        state = STATE.QUEUEING_DATA;
        validator.apply((HttpRequest) httpObject, ctx.channel(), new ActionListener<>() {
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
        int pendingMessages = pending.size();
        boolean fullRequestForwarded = forwardData(ctx);
        if (pending.size() <= 4 && pendingMessages > 32) {
            // Prevent the ArrayDeque from becoming forever large due to a single large message.
            ArrayDeque<HttpObject> old = pending;
            pending = new ArrayDeque<>(4);
            pending.addAll(old);
        }

        if (fullRequestForwarded) {
            state = STATE.WAITING_TO_START;
            if (pending.isEmpty() == false) {
                requestStart(ctx);
            }
        } else {
            state = STATE.FORWARDING_DATA;
        }

        ctx.channel().config().setAutoRead(shouldRead());
    }

    private void validationFailure(ChannelHandlerContext ctx, Exception e) {
        assert ctx.channel().eventLoop().inEventLoop();
        assert state == STATE.QUEUEING_DATA;

        state = STATE.HANDLING_QUEUED_DATA;
        HttpMessage messageToForward = (HttpMessage) pending.remove();
        boolean fullRequestConsumed;
        if (messageToForward instanceof LastHttpContent toRelease) {
            // drop the original content
            toRelease.release(2); // 1 for enqueuing, 1 for consuming
            // replace with empty content
            messageToForward = (HttpMessage) toRelease.replace(Unpooled.EMPTY_BUFFER);
            fullRequestConsumed = true;
        } else {
            fullRequestConsumed = dropData();
        }
        messageToForward.setDecoderResult(DecoderResult.failure(e));
        ctx.fireChannelRead(messageToForward);

        assert fullRequestConsumed || pending.isEmpty();

        if (pending.isEmpty()) {
            if (fullRequestConsumed) {
                state = STATE.WAITING_TO_START;
            } else {
                state = STATE.DROPPING_DATA_UNTIL_NEXT_REQUEST;
            }
        } else {
            state = STATE.WAITING_TO_START;
            requestStart(ctx);
        }

        ctx.channel().config().setAutoRead(shouldRead());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        state = STATE.DROPPING_DATA_PERMANENTLY;
        while (true) {
            if (dropData() == false) {
                break;
            }
        }
        super.channelInactive(ctx);
    }

    private boolean forwardData(ChannelHandlerContext ctx) {
        HttpObject toForward;
        while ((toForward = pending.poll()) != null) {
            ctx.fireChannelRead(toForward);
            ReferenceCountUtil.release(toForward); // reference cnt incremented when enqueued
            if (toForward instanceof LastHttpContent) {
                return true;
            }
        }
        return false;
    }

    private boolean dropData() {
        HttpObject toDrop;
        while ((toDrop = pending.poll()) != null) {
            ReferenceCountUtil.release(toDrop, 2); // 1 for enqueuing, 1 for consuming
            if (toDrop instanceof LastHttpContent) {
                return true;
            }
        }
        return false;
    }

    private boolean shouldRead() {
        return (state == STATE.QUEUEING_DATA || state == STATE.DROPPING_DATA_PERMANENTLY) == false;
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
