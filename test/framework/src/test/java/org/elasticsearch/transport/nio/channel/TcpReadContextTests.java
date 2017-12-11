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

package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.InboundChannelBuffer;
import org.elasticsearch.transport.nio.TcpReadHandler;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TcpReadContextTests extends ESTestCase {

    private TcpReadHandler handler;
    private int messageLength;
    private TcpNioSocketChannel channel;
    private TcpReadContext readContext;

    @Before
    public void init() {
        handler = mock(TcpReadHandler.class);

        messageLength = randomInt(96) + 4;
        channel = mock(TcpNioSocketChannel.class);
        Supplier<InboundChannelBuffer.Page> pageSupplier = () ->
            new InboundChannelBuffer.Page(ByteBuffer.allocate(BigArrays.BYTE_PAGE_SIZE), () -> {});
        readContext = new TcpReadContext(channel, handler, new InboundChannelBuffer(pageSupplier));
    }

    public void testSuccessfulRead() throws IOException {
        byte[] bytes = createMessage(messageLength);
        byte[] fullMessage = combineMessageAndHeader(bytes);

        final AtomicLong bufferCapacity = new AtomicLong();
        when(channel.read(any(InboundChannelBuffer.class))).thenAnswer(invocationOnMock -> {
            InboundChannelBuffer buffer = (InboundChannelBuffer) invocationOnMock.getArguments()[0];
            ByteBuffer byteBuffer = buffer.sliceBuffersFrom(buffer.getIndex())[0];
            bufferCapacity.set(buffer.getCapacity() - buffer.getIndex());
            byteBuffer.put(fullMessage);
            buffer.incrementIndex(fullMessage.length);
            return fullMessage.length;
        });

        readContext.read();

        verify(handler).handleMessage(new BytesArray(bytes), channel, messageLength);
        assertEquals(1024 * 16, bufferCapacity.get());

        BytesArray bytesArray = new BytesArray(new byte[10]);
        bytesArray.slice(5, 5);
        bytesArray.slice(5, 0);
    }

    public void testPartialRead() throws IOException {
        byte[] part1 = createMessage(messageLength);
        byte[] fullPart1 = combineMessageAndHeader(part1, messageLength + messageLength);
        byte[] part2 = createMessage(messageLength);

        final AtomicLong bufferCapacity = new AtomicLong();
        final AtomicReference<byte[]> bytes = new AtomicReference<>();

        when(channel.read(any(InboundChannelBuffer.class))).thenAnswer(invocationOnMock -> {
            InboundChannelBuffer buffer = (InboundChannelBuffer) invocationOnMock.getArguments()[0];
            ByteBuffer byteBuffer = buffer.sliceBuffersFrom(buffer.getIndex())[0];
            bufferCapacity.set(buffer.getCapacity() - buffer.getIndex());
            byteBuffer.put(bytes.get());
            buffer.incrementIndex(bytes.get().length);
            return bytes.get().length;
        });


        bytes.set(fullPart1);
        readContext.read();

        assertEquals(1024 * 16, bufferCapacity.get());
        verifyZeroInteractions(handler);

        bytes.set(part2);
        readContext.read();

        assertEquals(1024 * 16 - fullPart1.length, bufferCapacity.get());

        CompositeBytesReference reference = new CompositeBytesReference(new BytesArray(part1), new BytesArray(part2));
        verify(handler).handleMessage(reference, channel, messageLength + messageLength);
    }

    public void testReadThrowsIOException() throws IOException {
        IOException ioException = new IOException();
        when(channel.read(any())).thenThrow(ioException);

        try {
            readContext.read();
            fail("Expected exception");
        } catch (Exception ex) {
            assertSame(ioException, ex);
        }
    }

    public void closeClosesChannelBuffer() {
        InboundChannelBuffer buffer = mock(InboundChannelBuffer.class);
        TcpReadContext readContext = new TcpReadContext(channel, handler, buffer);

        readContext.close();

        verify(buffer).close();
    }

    private static byte[] combineMessageAndHeader(byte[] bytes) {
        return combineMessageAndHeader(bytes, bytes.length);
    }

    private static byte[] combineMessageAndHeader(byte[] bytes, int messageLength) {
        byte[] fullMessage = new byte[bytes.length + 6];
        ByteBuffer wrapped = ByteBuffer.wrap(fullMessage);
        wrapped.put((byte) 'E');
        wrapped.put((byte) 'S');
        wrapped.putInt(messageLength);
        wrapped.put(bytes);
        return fullMessage;
    }

    private static byte[] createMessage(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = randomByte();
        }
        return bytes;
    }
}
