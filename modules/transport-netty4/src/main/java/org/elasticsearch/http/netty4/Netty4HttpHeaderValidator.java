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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

import org.elasticsearch.action.ActionListener;

import java.util.ArrayDeque;
import java.util.function.BiConsumer;

public class Netty4HttpHeaderValidator extends ChannelInboundHandlerAdapter {

    private final BiConsumer<HttpMessage, ActionListener<Void>> validator;
    private final ArrayDeque<HttpObject> pending = new ArrayDeque<>(4);
    private STATE state = STATE.NO_HEADER;

    public Netty4HttpHeaderValidator(BiConsumer<HttpMessage, ActionListener<Void>> validator) {
        this.validator = validator;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        handleMessage(ctx, msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        state = STATE.NO_HEADER;
        super.channelReadComplete(ctx);
    }

    private void handlePendingMessages(ChannelHandlerContext ctx) {
        for (Object msg : pending) {
            ctx.fireChannelRead(msg);
        }
    }

    private void handleMessage(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof HttpObject;
        final HttpObject httpObject = (HttpObject) msg;

        if (state == STATE.NO_HEADER) {
            // We failed in the decoding step for other reasons. Pass down the pipeline.
            if (httpObject.decoderResult().isFailure()) {
                ctx.fireChannelRead(msg);
                return;
            }

            pending.add(httpObject);
            state = STATE.WAITING_FOR_VALIDATION;
            // TODO: Check
            assert httpObject instanceof HttpMessage;
            pauseData(ctx);
            validator.accept((HttpMessage) httpObject, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    EventLoop eventLoop = ctx.channel().eventLoop();
                    if (eventLoop.inEventLoop()) {
                        validationSuccess(ctx);
                    } else {
                        eventLoop.submit(() -> validationSuccess(ctx));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    EventLoop eventLoop = ctx.channel().eventLoop();
                    if (eventLoop.inEventLoop()) {
                        validationFailure(ctx, e);
                    } else {
                        eventLoop.submit(() -> validationFailure(ctx, e));
                    }
                }
            });

        } else if (state == STATE.WAITING_FOR_VALIDATION) {
            pending.add(httpObject);
        } else if (state == STATE.VALIDATION_SUCCEEDED) {
            assert pending.isEmpty();
            if (httpObject instanceof LastHttpContent) {
                state = STATE.NO_HEADER;
            }
            ctx.fireChannelRead(httpObject);
        } else if (state == STATE.VALIDATION_FAILED) {
            assert pending.isEmpty();
            if (httpObject instanceof LastHttpContent) {
                state = STATE.NO_HEADER;
            }
            ReferenceCountUtil.release(httpObject);
        }
    }

    private void pauseData(ChannelHandlerContext ctx) {
        assert ctx.channel().eventLoop().inEventLoop();
        ctx.channel().config().setAutoRead(false);
    }

    private void validationSuccess(ChannelHandlerContext ctx) {
        assert ctx.channel().eventLoop().inEventLoop();
        state = STATE.VALIDATION_SUCCEEDED;
        ctx.channel().config().setAutoRead(true);
        handlePendingMessages(ctx);
    }

    private void validationFailure(ChannelHandlerContext ctx, Exception e) {
        assert ctx.channel().eventLoop().inEventLoop();
        state = STATE.VALIDATION_FAILED;
        HttpObject messageToForward = pending.remove();
        if (messageToForward instanceof LastHttpContent toRelease) {
            messageToForward = toRelease.replace(Unpooled.EMPTY_BUFFER);
            toRelease.release();
        } else {
            HttpObject toRelease;
            while ((toRelease = pending.peek()) != null) {
                HttpContent content = (HttpContent) toRelease;
                content.release();
                if (content instanceof LastHttpContent) {
                    break;
                }
            }
        }
        messageToForward.setDecoderResult(DecoderResult.failure(e));
        ctx.fireChannelRead(messageToForward);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        for (HttpObject msg : pending) {
            ReferenceCountUtil.release(msg);
        }
        super.channelInactive(ctx);
    }

    private enum STATE {
        NO_HEADER,
        WAITING_FOR_VALIDATION,
        VALIDATION_SUCCEEDED,
        VALIDATION_FAILED
    }
}
