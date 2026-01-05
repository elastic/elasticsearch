/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCounted;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty4.internal.HttpValidator;
import org.elasticsearch.transport.Transports;

import java.util.ArrayDeque;

public class Netty4HttpHeaderValidator extends ChannelDuplexHandler {

    private final HttpValidator validator;
    private final ThreadContext threadContext;
    private State state = State.PASSING;
    private final ArrayDeque<Object> buffer = new ArrayDeque<>();

    public Netty4HttpHeaderValidator(HttpValidator validator, ThreadContext threadContext) {
        this.validator = validator;
        this.threadContext = threadContext;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (state == State.VALIDATING || buffer.size() > 0) {
            // there's already some buffered messages that need to be processed before this one, so queue this one up behind them
            buffer.offerLast(msg);
            return;
        }

        assert msg instanceof HttpObject;
        final var httpObject = (HttpObject) msg;
        if (httpObject.decoderResult().isFailure()) {
            ctx.fireChannelRead(httpObject); // pass-through for decoding failures
        } else if (msg instanceof HttpRequest httpRequest) {
            validate(ctx, httpRequest);
        } else if (state == State.PASSING) {
            assert msg instanceof HttpContent;
            ctx.fireChannelRead(msg);
        } else {
            assert state == State.DROPPING : state;
            assert msg instanceof HttpContent;
            final var httpContent = (HttpContent) msg;
            httpContent.release();
            ctx.read();
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        if (buffer.size() == 0) {
            ctx.fireChannelReadComplete();
        } // else we're buffering messages so will manage the read-complete messages ourselves
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        assert ctx.channel().eventLoop().inEventLoop();
        if (state != State.VALIDATING) {
            if (buffer.size() > 0) {
                final var message = buffer.pollFirst();
                if (message instanceof HttpRequest httpRequest) {
                    if (httpRequest.decoderResult().isFailure()) {
                        ctx.fireChannelRead(message); // pass-through for decoding failures
                        ctx.fireChannelReadComplete(); // downstream will have to call read() again when it's ready
                    } else {
                        validate(ctx, httpRequest);
                    }
                } else {
                    assert message instanceof HttpContent;
                    assert state == State.PASSING : state; // DROPPING releases any buffered chunks up-front
                    ctx.fireChannelRead(message);
                    ctx.fireChannelReadComplete(); // downstream will have to call read() again when it's ready
                }
            } else {
                ctx.read();
            }
        }
    }

    void validate(ChannelHandlerContext ctx, HttpRequest httpRequest) {
        final var validationResultListener = new ValidationResultListener(ctx, httpRequest);
        SubscribableListener.newForked(validationResultListener::doValidate)
            .addListener(
                validationResultListener,
                // dispatch back to event loop unless validation completed already in which case we can just continue on this thread
                // straight away, avoiding the need to buffer any subsequent messages
                ctx.channel().eventLoop(),
                null
            );
    }

    private class ValidationResultListener implements ActionListener<Void> {

        private final ChannelHandlerContext ctx;
        private final HttpRequest httpRequest;

        ValidationResultListener(ChannelHandlerContext ctx, HttpRequest httpRequest) {
            this.ctx = ctx;
            this.httpRequest = httpRequest;
        }

        void doValidate(ActionListener<Void> listener) {
            assert Transports.assertDefaultThreadContext(threadContext);
            assert ctx.channel().eventLoop().inEventLoop();
            assert state == State.PASSING || state == State.DROPPING : state;
            state = State.VALIDATING;
            try (var ignore = threadContext.newEmptyContext()) {
                validator.validate(
                    httpRequest,
                    ctx.channel(),
                    new ContextPreservingActionListener<>(threadContext::newEmptyContext, listener)
                );
            }
        }

        @Override
        public void onResponse(Void unused) {
            assert Transports.assertDefaultThreadContext(threadContext);
            assert ctx.channel().eventLoop().inEventLoop();
            assert state == State.VALIDATING : state;
            state = State.PASSING;
            fireChannelRead();
        }

        @Override
        public void onFailure(Exception e) {
            assert Transports.assertDefaultThreadContext(threadContext);
            assert ctx.channel().eventLoop().inEventLoop();
            assert state == State.VALIDATING : state;
            httpRequest.setDecoderResult(DecoderResult.failure(e));
            state = State.DROPPING;
            while (buffer.isEmpty() == false && buffer.peekFirst() instanceof HttpRequest == false) {
                assert buffer.peekFirst() instanceof HttpContent;
                ((ReferenceCounted) buffer.pollFirst()).release();
            }
            fireChannelRead();
        }

        private void fireChannelRead() {
            ctx.fireChannelRead(httpRequest);
            ctx.fireChannelReadComplete(); // downstream needs to read() again
        }
    }

    private enum State {
        PASSING,
        VALIDATING,
        DROPPING
    }

}
