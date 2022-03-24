/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty5;

import io.netty5.channel.ChannelHandlerAdapter;
import io.netty5.channel.ChannelHandlerContext;
import io.netty5.util.concurrent.Future;
import io.netty5.util.concurrent.Promise;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.http.HttpPipelinedResponse;
import org.elasticsearch.http.HttpPipeliningAggregator;

import java.nio.channels.ClosedChannelException;
import java.util.List;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 */
public class Netty5HttpPipeliningHandler extends ChannelHandlerAdapter {

    private final Logger logger;
    private final HttpPipeliningAggregator<Promise<Void>> aggregator;

    /**
     * Construct a new pipelining handler; this handler should be used downstream of HTTP decoding/aggregation.
     *
     * @param logger        for logging unexpected errors
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel connection; this is
     *                      required as events cannot queue up indefinitely
     */
    public Netty5HttpPipeliningHandler(Logger logger, final int maxEventsHeld) {
        this.logger = logger;
        this.aggregator = new HttpPipeliningAggregator<>(maxEventsHeld);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        assert msg instanceof Netty5HttpRequest : "Invalid message type: " + msg.getClass();
        HttpPipelinedRequest pipelinedRequest = aggregator.read(((Netty5HttpRequest) msg));
        ctx.fireChannelRead(pipelinedRequest);
    }

    @Override
    public Future<Void> write(final ChannelHandlerContext ctx, final Object msg) {
        assert msg instanceof HttpPipelinedResponse : "Invalid message type: " + msg.getClass();
        HttpPipelinedResponse response = (HttpPipelinedResponse) msg;
        boolean success = false;
        final Promise<Void> promise = ctx.newPromise();
        try {
            List<Tuple<HttpPipelinedResponse, Promise<Void>>> readyResponses = aggregator.write(response, promise);
            for (Tuple<HttpPipelinedResponse, Promise<Void>> readyResponse : readyResponses) {
                ctx.write(readyResponse.v1().getDelegateRequest()).addListener(future -> {
                    if (future.isFailed()) {
                        readyResponse.v2().tryFailure(future.cause());
                    } else {
                        readyResponse.v2().trySuccess(null);
                    }
                });
            }
            success = true;
        } catch (IllegalStateException e) {
            ctx.channel().close();
        } finally {
            if (success == false) {
                promise.setFailure(new ClosedChannelException());
            }
        }
        return promise.asFuture();
    }

    @Override
    public Future<Void> close(ChannelHandlerContext ctx) {
        List<Tuple<HttpPipelinedResponse, Promise<Void>>> inflightResponses = aggregator.removeAllInflightResponses();

        if (inflightResponses.isEmpty() == false) {
            ClosedChannelException closedChannelException = new ClosedChannelException();
            for (Tuple<HttpPipelinedResponse, Promise<Void>> inflightResponse : inflightResponses) {
                try {
                    inflightResponse.v2().setFailure(closedChannelException);
                } catch (RuntimeException e) {
                    logger.error("unexpected error while releasing pipelined http responses", e);
                }
            }
        }
        return ctx.close();
    }
}
