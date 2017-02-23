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
import io.netty.handler.codec.http.LastHttpContent;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.Collections;
import java.util.PriorityQueue;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 */
public class HttpPipeliningHandler extends ChannelDuplexHandler {

    // we use a priority queue so that responses are ordered by their sequence number
    private final PriorityQueue<HttpPipelinedResponse> holdingQueue;

    private final int maxEventsHeld;

    /*
     * The current read and write sequence numbers. Read sequence numbers are attached to requests in the order they are read from the
     * channel, and then transferred to responses. A response is not written to the channel context until its sequence number matches the
     * current write sequence, implying that all preceding messages have been written.
     */
    private int readSequence;
    private int writeSequence;

    /**
     * Construct a new pipelining handler; this handler should be used downstream of HTTP decoding/aggregation.
     *
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel connection; this is
     *                      required as events cannot queue up indefinitely
     */
    public HttpPipeliningHandler(final int maxEventsHeld) {
        this.maxEventsHeld = maxEventsHeld;
        this.holdingQueue = new PriorityQueue<>(1);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        if (msg instanceof LastHttpContent) {
            ctx.fireChannelRead(new HttpPipelinedRequest(((LastHttpContent) msg).retain(), readSequence++));
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
        if (msg instanceof HttpPipelinedResponse) {
            boolean channelShouldClose = false;

            synchronized (holdingQueue) {
                if (holdingQueue.size() < maxEventsHeld) {
                    holdingQueue.add((HttpPipelinedResponse) msg);

                    while (!holdingQueue.isEmpty()) {
                        /*
                         * Since the response with the lowest sequence number is the top of the priority queue, we know if its sequence
                         * number does not match the current write sequence number then we have not processed all preceding responses yet.
                         */
                        final HttpPipelinedResponse response = holdingQueue.peek();
                        if (response.sequence() != writeSequence) {
                            break;
                        }
                        holdingQueue.remove();
                        /*
                         * We must use the promise attached to the response; this is necessary since are going to hold a response until all
                         * responses that precede it in the pipeline are written first. Note that the promise from the method invocation is
                         * not ignored, it will already be attached to an existing response and consumed when that response is drained.
                         */
                        ctx.write(response.response(), response.promise());
                        writeSequence++;
                    }
                } else {
                    channelShouldClose = true;
                }
            }

            if (channelShouldClose) {
                try {
                    Netty4Utils.closeChannels(Collections.singletonList(ctx.channel()));
                } finally {
                    ((HttpPipelinedResponse) msg).release();
                    promise.setSuccess();
                }
            }
        } else {
            ctx.write(msg, promise);
        }
    }

}
