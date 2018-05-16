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

package org.elasticsearch.http.netty4.pipelining;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.http.HttpPipelinedResponse;
import org.elasticsearch.http.HttpPipeliningAggregator;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 */
public class HttpPipeliningHandler extends ChannelDuplexHandler {

    private final Logger logger;
    private final HttpPipeliningAggregator<FullHttpResponse, ChannelPromise> aggregator;

    /**
     * Construct a new pipelining handler; this handler should be used downstream of HTTP decoding/aggregation.
     *
     * @param logger        for logging unexpected errors
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel connection; this is
     *                      required as events cannot queue up indefinitely
     */
    public HttpPipeliningHandler(Logger logger, final int maxEventsHeld) {
        this.logger = logger;
        this.aggregator = new HttpPipeliningAggregator<>(maxEventsHeld);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        if (msg instanceof LastHttpContent) {
            HttpPipelinedRequest<LastHttpContent> pipelinedRequest = aggregator.read(((LastHttpContent) msg).retain());
            ctx.fireChannelRead(pipelinedRequest);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
        if (msg instanceof HttpPipelinedResponse) {
            synchronized (aggregator) {
                HttpPipelinedResponse<FullHttpResponse, ChannelPromise> response = (HttpPipelinedResponse<FullHttpResponse, ChannelPromise>) msg;
                boolean success = false;
                try {
                    ArrayList<HttpPipelinedResponse<FullHttpResponse, ChannelPromise>> responsesToWrite = aggregator.write(response);
                    success = true;
                    for (HttpPipelinedResponse<FullHttpResponse, ChannelPromise> responseToWrite : responsesToWrite) {
                        ctx.write(responseToWrite.getResponse(), responseToWrite.getListener());
                    }
                } catch (IllegalStateException e) {
                    Netty4Utils.closeChannels(Collections.singletonList(ctx.channel()));
                } finally {
                    if (success == false) {
                        response.getListener().setFailure(new ClosedChannelException());
                    }
                }
            }

        } else {
            ctx.write(msg, promise);
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        List<HttpPipelinedResponse<FullHttpResponse, ChannelPromise>> inflightResponses = aggregator.removeAllInflightResponses();

        if (inflightResponses.isEmpty() == false) {
            ClosedChannelException closedChannelException = new ClosedChannelException();
            for (HttpPipelinedResponse<FullHttpResponse, ChannelPromise> inflightResponse : inflightResponses) {
                try {
                    inflightResponse.getListener().setFailure(closedChannelException);
                } catch (RuntimeException e) {
                    logger.error("unexpected error while releasing pipelined http responses", e);
                }
            }
        }
        ctx.close(promise);
    }
}
