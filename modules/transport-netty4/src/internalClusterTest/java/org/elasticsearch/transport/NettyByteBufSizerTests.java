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
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.elasticsearch.test.ESTestCase;

public class NettyByteBufSizerTests extends ESTestCase {

    public void testBuffersAreResized16KBResult() {
        NettyByteBufSizer sizer = new NettyByteBufSizer();
        ByteBuf buffer = Unpooled.buffer(64 * 1024);
        // Test less than 16KB, greater than 8KB
        int bytesWritten = 9000 + randomInt(4000);
        buffer.writeBytes(new byte[bytesWritten]);
        EmbeddedChannel channel = new EmbeddedChannel(sizer);
        channel.writeInbound(buffer);
        ByteBuf newBuf = channel.readInbound();
        assertEquals(0, buffer.refCnt());
        assertEquals(bytesWritten, newBuf.capacity());
        assertEquals(16 * 1024, newBuf.maxFastWritableBytes() + newBuf.writerIndex());
    }

    public void testBuffersAreResized32KBResult() {
        NettyByteBufSizer sizer = new NettyByteBufSizer();
        ByteBuf buffer = Unpooled.buffer(64 * 1024);
        // Test less than 32KB, greater than 16KB
        int bytesWritten = 17000 + randomInt(4000);
        buffer.writeBytes(new byte[bytesWritten]);
        EmbeddedChannel channel = new EmbeddedChannel(sizer);
        channel.writeInbound(buffer);
        ByteBuf newBuf = channel.readInbound();
        assertEquals(0, buffer.refCnt());
        assertEquals(bytesWritten, newBuf.capacity());
        assertEquals(32 * 1024, newBuf.maxFastWritableBytes() + newBuf.writerIndex());
    }

    public void testProperlySizeBuffersAreNotResized32KBStarting() {
        NettyByteBufSizer sizer = new NettyByteBufSizer();
        ByteBuf buffer = Unpooled.buffer(32 * 1024);
        int bytesWritten = 17000 + randomInt(4000);
        buffer.writeBytes(new byte[bytesWritten]);
        EmbeddedChannel channel = new EmbeddedChannel(sizer);
        channel.writeInbound(buffer);
        ByteBuf newBuf = channel.readInbound();
        assertEquals(1, buffer.refCnt());
        assertEquals(32 * 1024, newBuf.capacity());
    }

    public void testProperlySizeBuffersAreNotResized64KBStarting() {
        NettyByteBufSizer sizer = new NettyByteBufSizer();
        ByteBuf buffer = Unpooled.buffer(64 * 1024);
        int bytesWritten = 34000 + randomInt(4000);
        buffer.writeBytes(new byte[bytesWritten]);
        EmbeddedChannel channel = new EmbeddedChannel(sizer);
        channel.writeInbound(buffer);
        ByteBuf newBuf = channel.readInbound();
        assertEquals(1, buffer.refCnt());
        assertEquals(64 * 1024, newBuf.capacity());
    }
}
