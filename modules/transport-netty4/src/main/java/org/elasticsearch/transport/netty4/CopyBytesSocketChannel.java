/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * =============================================================================
 *
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
package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static io.netty.channel.internal.ChannelUtils.MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD;

/**
 * This class is adapted from {@link NioSocketChannel} class in the Netty project. It overrides the channel
 * read/write behavior to ensure that the bytes are always copied to a thread-local direct bytes buffer. This
 * happens BEFORE the call to the Java {@link SocketChannel} is issued.
 *
 * The purpose of this class is to allow the disabling of netty direct buffer pooling while allowing us to
 * control how bytes end up being copied to direct memory. If we simply disabled netty pooling, we would rely
 * on the JDK's internal thread local buffer pooling. Instead, this class allows us to create a one thread
 * local buffer with a defined size.
 */
@SuppressForbidden(reason = "Channel#write")
public class CopyBytesSocketChannel extends Netty4NioSocketChannel {

    private static final int MAX_BYTES_PER_WRITE = StrictMath.toIntExact(
        ByteSizeValue.parseBytesSizeValue(System.getProperty("es.transport.buffer.size", "1m"), "es.transport.buffer.size").getBytes()
    );

    private static final ThreadLocal<ByteBuffer> ioBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE));
    private final WriteConfig writeConfig = new WriteConfig();

    public CopyBytesSocketChannel() {
        super();
    }

    CopyBytesSocketChannel(Channel parent, SocketChannel socket) {
        super(parent, socket);
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
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
            } else {
                // Zero length buffers are not added to nioBuffers by ChannelOutboundBuffer, so there is no need
                // to check if the total size of all the buffers is non-zero.
                ByteBuffer buffer = getIoBuffer();
                copyBytes(nioBuffers, nioBufferCnt, buffer);
                buffer.flip();

                int attemptedBytes = buffer.remaining();
                final int localWrittenBytes = writeToSocketChannel(javaChannel(), buffer);
                if (localWrittenBytes <= 0) {
                    incompleteWrite(true);
                    return;
                }
                adjustMaxBytesPerGatheringWrite(attemptedBytes, localWrittenBytes, maxBytesPerGatheringWrite);
                setWrittenBytes(nioBuffers, localWrittenBytes);
                in.removeBytes(localWrittenBytes);
                --writeSpinCount;
            }
        } while (writeSpinCount > 0);

        incompleteWrite(writeSpinCount < 0);
    }

    @Override
    protected int doReadBytes(ByteBuf byteBuf) throws Exception {
        final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
        int writeableBytes = Math.min(byteBuf.writableBytes(), MAX_BYTES_PER_WRITE);
        allocHandle.attemptedBytesRead(writeableBytes);
        ByteBuffer limit = getIoBuffer().limit(writeableBytes);
        int bytesRead = readFromSocketChannel(javaChannel(), limit);
        limit.flip();
        if (bytesRead > 0) {
            byteBuf.writeBytes(limit);
        }
        return bytesRead;
    }

    // Protected so that tests can verify behavior and simulate partial writes
    protected int writeToSocketChannel(SocketChannel socketChannel, ByteBuffer buffer) throws IOException {
        return socketChannel.write(buffer);
    }

    // Protected so that tests can verify behavior
    protected int readFromSocketChannel(SocketChannel socketChannel, ByteBuffer buffer) throws IOException {
        return socketChannel.read(buffer);
    }

    private static ByteBuffer getIoBuffer() {
        ByteBuffer buffer = CopyBytesSocketChannel.ioBuffer.get();
        buffer.clear();
        return buffer;
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
            if (buffer.hasArray()) {
                destination.put(buffer.array(), buffer.arrayOffset() + buffer.position(), nBytesToCopy);
            } else {
                int initialLimit = buffer.limit();
                int initialPosition = buffer.position();
                buffer.limit(buffer.position() + nBytesToCopy);
                destination.put(buffer);
                buffer.position(initialPosition);
                buffer.limit(initialLimit);
            }
        }
    }

    private static void setWrittenBytes(ByteBuffer[] source, int bytesWritten) {
        for (int i = 0; bytesWritten > 0; i++) {
            ByteBuffer buffer = source[i];
            int nBytes = Math.min(buffer.remaining(), bytesWritten);
            buffer.position(buffer.position() + nBytes);
            bytesWritten = bytesWritten - nBytes;
        }
    }

    private final class WriteConfig {

        private volatile int maxBytesPerGatheringWrite = MAX_BYTES_PER_WRITE;

        private WriteConfig() {
            calculateMaxBytesPerGatheringWrite();
        }

        void setMaxBytesPerGatheringWrite(int maxBytesPerGatheringWrite) {
            this.maxBytesPerGatheringWrite = Math.min(maxBytesPerGatheringWrite, MAX_BYTES_PER_WRITE);
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
