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

package org.elasticsearch.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

@ChannelHandler.Sharable
public class NettyByteBufSizer extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) {
        // If max fast writeable bytes is greater than the number of readable bytes, this buffer is at least
        // twice as big as necessary to contain the data. If that is the case, allocate a new buffer and
        // copy.
        int estimatedSize = buf.maxFastWritableBytes() + buf.writerIndex();
        if (estimatedSize > 1024 && buf.maxFastWritableBytes() >= buf.readableBytes()) {
            ByteBuf newBuffer = ctx.alloc().heapBuffer(buf.readableBytes());
            newBuffer.writeBytes(buf);
            out.add(newBuffer);
        } else {
            out.add(buf.retain());
        }
    }
}
