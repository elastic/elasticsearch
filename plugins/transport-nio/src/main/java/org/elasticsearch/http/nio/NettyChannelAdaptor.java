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

package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.nio.BytesProducer;
import org.elasticsearch.nio.BytesWriteOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.WriteOperation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;


/**
 * This class adapts a netty channel for our usage. In particular, it captures writes at the end of the
 * pipeline and places them in a queue that can be accessed by our code.
 */
class NettyChannelAdaptor extends EmbeddedChannel implements BytesProducer, SocketChannelContext.ReadConsumer {

    // TODO: Explore if this can be made more efficient by generating less garbage
    private LinkedList<Tuple<BytesReference, ChannelPromise>> messages = new LinkedList<>();

    NettyChannelAdaptor() {
        pipeline().addFirst("promise_captor", new ChannelOutboundHandlerAdapter() {

            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                // This is a little tricky. The embedded channel will complete the promise once it writes the message
                // to its outbound buffer. We do not want to complete the promise until the message is sent. So we
                // intercept the promise and pass a different promise back to the rest of the pipeline.

                try {
                    // TODO: Ensure release on failure. I'm sure it is necessary here as it might be done
                    // TODO: in NioHttpChannel.
                    ByteBuf message = (ByteBuf) msg;
                    BytesReference bytesReference = ByteBufBytesReference.toBytesReference(message);
                    promise.addListener((f) -> message.release());
                    messages.add(new Tuple<>(bytesReference, promise));
                } catch (Exception e) {
                    promise.setFailure(e);
                }
            }
        });
    }

    Queue<Object> decode(ByteBuf inboundBytes) {
        writeInbound(inboundBytes);
        return inboundMessages();
    }

    Tuple<BytesReference, ChannelPromise> popMessage() {
        return messages.pollFirst();
    }

    boolean hasMessages() {
        return messages.size() > 0;
    }

    void closeNettyChannel() {
        close();
    }

    @Override
    public void writeMessage(WriteOperation writeOperation) throws IOException {
        writeAndFlush(null, (NettyActionListener) writeOperation.getListener());
    }

    @Override
    public BytesWriteOperation pollBytes() {
        Tuple<BytesReference, ChannelPromise> message = messages.pollFirst();
        if (message == null) {
            return null;
        } else {
            return new BytesWriteOperation(null, BytesReference.toByteBuffers(message.v1()), null);
        }
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        ByteBuf inboundBytes = toByteBuf(channelBuffer);

        int readDelta = inboundBytes.readableBytes();
        writeInbound(inboundBytes);

        Queue<Object> requests = inboundMessages();


        Object msg;
        while ((msg = requests.poll()) != null) {
//            requestHandler.handleMessage(null, this, msg);
        }

        // TODO: I'm not sure this is currently safe with recycling
        return readDelta;
    }

    private static ByteBuf toByteBuf(InboundChannelBuffer channelBuffer) {
        ByteBuffer[] preIndexBuffers = channelBuffer.sliceBuffersFrom(channelBuffer.getIndex());
        if (preIndexBuffers.length == 1) {
            return Unpooled.wrappedBuffer(preIndexBuffers[0]);
        } else {
            CompositeByteBuf byteBuf = Unpooled.compositeBuffer(preIndexBuffers.length);
            for (ByteBuffer buffer : preIndexBuffers) {
                ByteBuf component = Unpooled.wrappedBuffer(buffer);
                byteBuf.addComponent(true, component);
            }
            return byteBuf;
        }
    }
}
