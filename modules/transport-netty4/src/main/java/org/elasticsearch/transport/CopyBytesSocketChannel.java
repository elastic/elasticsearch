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
/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static io.netty.channel.internal.ChannelUtils.MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD;


/**
 * This class is adapted from {@link NioSocketChannel} class in the Netty project. It overrides the channel
 * read/write behavior to ensure that the bytes are always copied to a thread-local direct bytes buffer. This
 * happens BEFORE the call to the Java {@link SocketChannel} is issued.
 */
public class CopyBytesSocketChannel extends NioSocketChannel {

    private static final int MAX_BYTES_PER_WRITE = 1 << 20;

    private final WriteConfig writeConfig = new WriteConfig();
    private final ThreadLocal<ByteBuffer> ioBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE));

    public CopyBytesSocketChannel() {
        super();
    }

    CopyBytesSocketChannel(Channel parent, SocketChannel socket) {
        super(parent, socket);
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        SocketChannel ch = javaChannel();
        int writeSpinCount = config().getWriteSpinCount();
        do {
            if (in.isEmpty()) {
                // All written so clear OP_WRITE
                clearOpWrite();
                // Directly return here so incompleteWrite(...) is not called.
                return;
            }

            // Ensure the pending writes are made of ByteBufs only.
            int maxBytesPerGatheringWrite = writeConfig.getMaxBytesPerGatheringWrite();
            ByteBuffer[] nioBuffers = in.nioBuffers(1024, maxBytesPerGatheringWrite);
            int nioBufferCnt = in.nioBufferCount();

            if (nioBufferCnt == 0) {// We have something else beside ByteBuffers to write so fallback to normal writes.
                writeSpinCount -= doWrite0(in);
            } else {// Only one ByteBuf so use non-gathering write
                // Zero length buffers are not added to nioBuffers by ChannelOutboundBuffer, so there is no need
                // to check if the total size of all the buffers is non-zero.
                ByteBuffer ioBuffer = this.ioBuffer.get();
                ioBuffer.clear();
                copyBytes(nioBuffers, nioBufferCnt, ioBuffer);
                ioBuffer.flip();

                int attemptedBytes = ioBuffer.remaining();
                final int localWrittenBytes = ch.write(ioBuffer);
                if (localWrittenBytes <= 0) {
                    incompleteWrite(true);
                    return;
                }
                adjustMaxBytesPerGatheringWrite(attemptedBytes, localWrittenBytes, maxBytesPerGatheringWrite);
                in.removeBytes(localWrittenBytes);
                --writeSpinCount;
            }
        } while (writeSpinCount > 0);

        incompleteWrite(writeSpinCount < 0);
    }

    @Override
    protected int doReadBytes(ByteBuf byteBuf) throws Exception {
        final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
        allocHandle.attemptedBytesRead(byteBuf.writableBytes());
        ByteBuffer ioBuffer = getIoBuffer();
        ByteBuf wrapped = Unpooled.wrappedBuffer(ioBuffer);
        wrapped.clear();
        int bytesRead = wrapped.writeBytes(javaChannel(), allocHandle.attemptedBytesRead());
        if (bytesRead > 0) {
            byteBuf.writeBytes(wrapped, bytesRead);
        }
        return bytesRead;
    }

    private ByteBuffer getIoBuffer() {
        ByteBuffer ioBuffer = this.ioBuffer.get();
        ioBuffer.clear();
        return ioBuffer;
    }

    private void adjustMaxBytesPerGatheringWrite(int attempted, int written, int oldMaxBytesPerGatheringWrite) {
        // By default we track the SO_SNDBUF when ever it is explicitly set. However some OSes may dynamically change
        // SO_SNDBUF (and other characteristics that determine how much data can be written at once) so we should try
        // make a best effort to adjust as OS behavior changes.
        if (attempted == written) {
            if (attempted << 1 > oldMaxBytesPerGatheringWrite) {
                writeConfig.setMaxBytesPerGatheringWrite(attempted << 1);
            }
        } else if (attempted > MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD && written < attempted >>> 1) {
            writeConfig.setMaxBytesPerGatheringWrite(attempted >>> 1);
        }
    }

    private static void copyBytes(ByteBuffer[] source, int nioBufferCnt, ByteBuffer destination) {
        for (int i = 0; i < nioBufferCnt && destination.hasRemaining(); i++) {
            ByteBuffer buffer = source[i];
            int nBytesToCopy = Math.min(destination.remaining(), buffer.remaining());
            int initialLimit = buffer.limit();
            buffer.limit(buffer.position() + nBytesToCopy);
            destination.put(buffer);
            buffer.limit(initialLimit);
        }
    }

    private final class WriteConfig {

        private volatile int maxBytesPerGatheringWrite = MAX_BYTES_PER_WRITE;

        private WriteConfig() {
            calculateMaxBytesPerGatheringWrite();
        }

        void setMaxBytesPerGatheringWrite(int maxBytesPerGatheringWrite) {
            this.maxBytesPerGatheringWrite = maxBytesPerGatheringWrite;
        }

        int getMaxBytesPerGatheringWrite() {
            return maxBytesPerGatheringWrite;
        }

        private void calculateMaxBytesPerGatheringWrite() {
            // Multiply by 2 to give some extra space in case the OS can process write data faster than we can provide.
            int newSendBufferSize = config().getSendBufferSize() << 1;
            if (newSendBufferSize > 0) {
                setMaxBytesPerGatheringWrite(config().getSendBufferSize() << 1);
            }
        }
    }
}
