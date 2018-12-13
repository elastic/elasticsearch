/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.http.HttpPipeliningAggregator;

import java.nio.channels.ClosedChannelException;
import java.util.List;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 */
public class Netty4HttpPipeliningHandler extends ChannelDuplexHandler {

    private final Logger logger;
    private final HttpPipeliningAggregator<Netty4HttpResponse, ChannelPromise> aggregator;

    /**
     * Construct a new pipelining handler; this handler should be used downstream of HTTP decoding/aggregation.
     *
     * @param logger        for logging unexpected errors
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel connection; this is
     *                      required as events cannot queue up indefinitely
     */
    public Netty4HttpPipeliningHandler(Logger logger, final int maxEventsHeld) {
        this.logger = logger;
        this.aggregator = new HttpPipeliningAggregator<>(maxEventsHeld);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        assert msg instanceof FullHttpRequest : "Invalid message type: " + msg.getClass();
        HttpPipelinedRequest<FullHttpRequest> pipelinedRequest = aggregator.read(((FullHttpRequest) msg));
        ctx.fireChannelRead(pipelinedRequest);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
        assert msg instanceof Netty4HttpResponse : "Invalid message type: " + msg.getClass();
        Netty4HttpResponse response = (Netty4HttpResponse) msg;
        boolean success = false;
        try {
            List<Tuple<Netty4HttpResponse, ChannelPromise>> readyResponses = aggregator.write(response, promise);
            for (Tuple<Netty4HttpResponse, ChannelPromise> readyResponse : readyResponses) {
                ctx.write(readyResponse.v1(), readyResponse.v2());
            }
            success = true;
        } catch (IllegalStateException e) {
            ctx.channel().close();
        } finally {
            if (success == false) {
                promise.setFailure(new ClosedChannelException());
            }
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        List<Tuple<Netty4HttpResponse, ChannelPromise>> inflightResponses = aggregator.removeAllInflightResponses();

        if (inflightResponses.isEmpty() == false) {
            ClosedChannelException closedChannelException = new ClosedChannelException();
            for (Tuple<Netty4HttpResponse, ChannelPromise> inflightResponse : inflightResponses) {
                try {
                    inflightResponse.v2().setFailure(closedChannelException);
                } catch (RuntimeException e) {
                    logger.error("unexpected error while releasing pipelined http responses", e);
                }
            }
        }
        ctx.close(promise);
    }
}
