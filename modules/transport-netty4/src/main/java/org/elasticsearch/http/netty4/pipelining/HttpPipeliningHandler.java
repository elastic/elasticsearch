
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
import io.netty.util.ReferenceCountUtil;
import org.elasticsearch.action.termvectors.TermVectorsFilter;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their
 * corresponding requests. NOTE: A side effect of using this handler is that upstream HttpRequest objects will
 * cause the original message event to be effectively transformed into an OrderedUpstreamMessageEvent. Conversely
 * OrderedDownstreamChannelEvent objects are expected to be received for the correlating response objects.
 */
public class HttpPipeliningHandler extends ChannelDuplexHandler {

    private static final int INITIAL_EVENTS_HELD = 3;

    private final int maxEventsHeld;

    private int readSequence;
    private int writeSequence;

    private final Queue<HttpPipelinedResponse> holdingQueue;

    /**
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel
     *                      connection. This is required as events cannot queue up indefinitely; we would run out of
     *                      memory if this was the case.
     */
    public HttpPipeliningHandler(final int maxEventsHeld) {
        this.maxEventsHeld = maxEventsHeld;
        this.holdingQueue = new PriorityQueue<>(INITIAL_EVENTS_HELD);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof LastHttpContent) {
            ctx.fireChannelRead(new HttpPipelinedRequest(((LastHttpContent) msg).retain(), readSequence++));
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof HttpPipelinedResponse) {
            boolean channelShouldClose = false;

            synchronized (holdingQueue) {
                if (holdingQueue.size() < maxEventsHeld) {
                    holdingQueue.add((HttpPipelinedResponse) msg);

                    while (!holdingQueue.isEmpty()) {
                        final HttpPipelinedResponse response = holdingQueue.peek();
                        if (response.sequence() != writeSequence) {
                            break;
                        }
                        holdingQueue.remove();
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
