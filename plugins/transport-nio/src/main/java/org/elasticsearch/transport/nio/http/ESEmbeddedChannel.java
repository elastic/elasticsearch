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

package org.elasticsearch.transport.nio.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.LinkedList;
import java.util.Queue;

public class ESEmbeddedChannel extends EmbeddedChannel {

    private LinkedList<Tuple<BytesReference, ChannelPromise>> messages = new LinkedList<>();

    public ESEmbeddedChannel() {
        pipeline().addFirst("promise_captor", new ChannelOutboundHandlerAdapter() {

            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                // This is a little tricky. The embedded channel will complete the promise once it writes the message
                // to its outbound buffer. We do not want to complete the promise until the message is sent. So we
                // intercept the promise and pass a different promise back to the rest of the pipeline.

                try {
                    ByteBuf message = (ByteBuf) msg;
                    BytesReference bytesReference = Netty4Utils.toBytesReference(message);
                    promise.addListener((f) -> message.release());
                    messages.add(new Tuple<>(bytesReference, promise));
                } catch (Exception e) {
                    promise.setFailure(e);
                }
            }
        });
    }

    public Queue<Object> decode(ByteBuf inboundBytes) {
        writeInbound(inboundBytes);
        return inboundMessages();
    }

    public Tuple<BytesReference, ChannelPromise> popMessage() {
        return messages.pollFirst();
    }

    public boolean hasMessages() {
        return messages.size() > 0;
    }

    public void closeNettyChannel() {
        close();
    }
}
