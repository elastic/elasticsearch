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
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.http.HttpPipelinedResponse;
import org.elasticsearch.http.HttpPipeliningAggregator;

import java.nio.channels.ClosedChannelException;
import java.util.List;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 */
public class Netty4HttpPipeliningHandler extends ChannelDuplexHandler {

    private final Logger logger;
    private final HttpPipeliningAggregator<ChannelPromise> aggregator;

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
        assert msg instanceof Netty4HttpRequest : "Invalid message type: " + msg.getClass();
        HttpPipelinedRequest pipelinedRequest = aggregator.read(((Netty4HttpRequest) msg));
        ctx.fireChannelRead(pipelinedRequest);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
        assert msg instanceof HttpPipelinedResponse : "Invalid message type: " + msg.getClass();
        HttpPipelinedResponse response = (HttpPipelinedResponse) msg;
        boolean success = false;
        try {
            List<Tuple<HttpPipelinedResponse, ChannelPromise>> readyResponses = aggregator.write(response, promise);
            for (Tuple<HttpPipelinedResponse, ChannelPromise> readyResponse : readyResponses) {
                ctx.write(readyResponse.v1().getDelegateRequest(), readyResponse.v2());
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
        List<Tuple<HttpPipelinedResponse, ChannelPromise>> inflightResponses = aggregator.removeAllInflightResponses();

        if (inflightResponses.isEmpty() == false) {
            ClosedChannelException closedChannelException = new ClosedChannelException();
            for (Tuple<HttpPipelinedResponse, ChannelPromise> inflightResponse : inflightResponses) {
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
