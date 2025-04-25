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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.http.netty4.internal.HttpValidator;
import org.elasticsearch.transport.Transports;

public class Netty4HttpHeaderValidator extends ChannelDuplexHandler {

    private final HttpValidator validator;
    private final ThreadContext threadContext;
    private State state;

    public Netty4HttpHeaderValidator(HttpValidator validator, ThreadContext threadContext) {
        this.validator = validator;
        this.threadContext = threadContext;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert msg instanceof HttpObject;
        var httpObject = (HttpObject) msg;
        if (httpObject.decoderResult().isFailure()) {
            ctx.fireChannelRead(httpObject); // pass-through for decoding failures
        } else {
            if (msg instanceof HttpRequest request) {
                validate(ctx, request);
            } else {
                assert msg instanceof HttpContent;
                var content = (HttpContent) msg;
                if (state == State.DROPPING) {
                    content.release();
                    ctx.read();
                } else {
                    assert state == State.PASSING : "unexpected content before validation completed";
                    ctx.fireChannelRead(content);
                }
            }
        }
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        // until validation is completed we can ignore read calls,
        // once validation is finished HttpRequest will be fired and downstream can read from there
        if (state != State.VALIDATING) {
            ctx.read();
        }
    }

    void validate(ChannelHandlerContext ctx, HttpRequest request) {
        assert Transports.assertDefaultThreadContext(threadContext);
        state = State.VALIDATING;
        ActionListener.run(
            // this prevents thread-context changes to propagate to the validation listener
            // atm, the validation listener submits to the event loop executor, which doesn't know about the ES thread-context,
            // so this is just a defensive play, in case the code inside the listener changes to not use the event loop executor
            ActionListener.assertOnce(
                new ContextPreservingActionListener<Void>(
                    threadContext.wrapRestorable(threadContext.newStoredContext()),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            handleValidationResult(ctx, request, null);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            handleValidationResult(ctx, request, e);
                        }
                    }
                )
            ),
            listener -> {
                // this prevents thread-context changes to propagate beyond the validation, as netty worker threads are reused
                try (ThreadContext.StoredContext ignore = threadContext.newStoredContext()) {
                    validator.validate(request, ctx.channel(), listener);
                }
            }
        );
    }

    void handleValidationResult(ChannelHandlerContext ctx, HttpRequest request, @Nullable Exception validationError) {
        assert Transports.assertDefaultThreadContext(threadContext);
        // Always explicitly dispatch back to the event loop to prevent reentrancy concerns if we are still on event loop
        ctx.channel().eventLoop().execute(() -> {
            if (validationError != null) {
                request.setDecoderResult(DecoderResult.failure(validationError));
                state = State.DROPPING;
            } else {
                state = State.PASSING;
            }
            ctx.fireChannelRead(request);
        });
    }

    private enum State {
        PASSING,
        VALIDATING,
        DROPPING
    }

}
