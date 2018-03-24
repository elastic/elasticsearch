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
import org.elasticsearch.nio.BytesProducer;
import org.elasticsearch.nio.FlushOperation;
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
//    SocketChannelContext.WriteProducer {

    // TODO: Explore if this can be made more efficient by generating less garbage
    private final LinkedList<FlushOperation> byteOps = new LinkedList<>();
    private final SocketChannelContext channelContext = null;

    NettyChannelAdaptor() {
        pipeline().addFirst("promise_captor", new ChannelOutboundHandlerAdapter() {

            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                // This is a little tricky. The embedded channel will complete the promise once it writes the message
                // to its outbound buffer. We do not want to complete the promise until the message is sent. So we
                // intercept the promise and pass a different promise back to the rest of the pipeline.

                ByteBuf message = (ByteBuf) msg;
                try {
                    // TODO: Ensure release on failure. I'm not sure it is necessary here as it might be done
                    // TODO: in NioHttpChannel.
                    promise.addListener((f) -> message.release());
                    byteOps.add(new FlushOperation(message.nioBuffers(), new NettyListener(promise)));
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

    boolean hasMessages() {
        return byteOps.isEmpty() == false;
    }

    void closeNettyChannel() {
        close();
    }

    @Override
    public void writeMessage(WriteOperation writeOperation) throws IOException {
        writeAndFlush(null, (NettyListener) writeOperation.getListener());
    }

//    @Override
    public void produceWrites(WriteOperation writeOperation) {
        writeAndFlush(writeOperation.getObject(), (NettyListener) writeOperation.getListener());
    }

//    @Override
    public FlushOperation pollFlushOperation() {
        return null;
    }

    @Override
    public FlushOperation pollBytes() {
        return byteOps.pollFirst();
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
