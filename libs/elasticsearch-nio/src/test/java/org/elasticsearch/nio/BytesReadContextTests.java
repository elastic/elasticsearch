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

package org.elasticsearch.nio;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BytesReadContextTests extends ESTestCase {

    private ReadContext.ReadConsumer readConsumer;
    private NioSocketChannel channel;
    private BytesReadContext readContext;
    private InboundChannelBuffer channelBuffer;
    private int messageLength;

    @Before
    public void init() {
        readConsumer = mock(ReadContext.ReadConsumer.class);

        messageLength = randomInt(96) + 20;
        channel = mock(NioSocketChannel.class);
        Supplier<InboundChannelBuffer.Page> pageSupplier = () ->
            new InboundChannelBuffer.Page(ByteBuffer.allocate(BigArrays.BYTE_PAGE_SIZE), () -> {});
        channelBuffer = new InboundChannelBuffer(pageSupplier);
        readContext = new BytesReadContext(channel, readConsumer, channelBuffer);
    }

    public void testSuccessfulRead() throws IOException {
        byte[] bytes = createMessage(messageLength);

        when(channel.read(any(ByteBuffer[].class))).thenAnswer(invocationOnMock -> {
            ByteBuffer[] buffers = (ByteBuffer[]) invocationOnMock.getArguments()[0];
            buffers[0].put(bytes);
            return bytes.length;
        });

        when(readConsumer.consumeReads(channelBuffer)).thenReturn(messageLength, 0);

        assertEquals(messageLength, readContext.read());

        assertEquals(0, channelBuffer.getIndex());
        assertEquals(BigArrays.BYTE_PAGE_SIZE - bytes.length, channelBuffer.getCapacity());
        verify(readConsumer, times(2)).consumeReads(channelBuffer);
    }

    public void testMultipleReadsConsumed() throws IOException {
        byte[] bytes = createMessage(messageLength * 2);

        when(channel.read(any(ByteBuffer[].class))).thenAnswer(invocationOnMock -> {
            ByteBuffer[] buffers = (ByteBuffer[]) invocationOnMock.getArguments()[0];
            buffers[0].put(bytes);
            return bytes.length;
        });

        when(readConsumer.consumeReads(channelBuffer)).thenReturn(messageLength, messageLength, 0);

        assertEquals(bytes.length, readContext.read());

        assertEquals(0, channelBuffer.getIndex());
        assertEquals(BigArrays.BYTE_PAGE_SIZE - bytes.length, channelBuffer.getCapacity());
        verify(readConsumer, times(3)).consumeReads(channelBuffer);
    }

    public void testPartialRead() throws IOException {
        byte[] bytes = createMessage(messageLength);

        when(channel.read(any(ByteBuffer[].class))).thenAnswer(invocationOnMock -> {
            ByteBuffer[] buffers = (ByteBuffer[]) invocationOnMock.getArguments()[0];
            buffers[0].put(bytes);
            return bytes.length;
        });


        when(readConsumer.consumeReads(channelBuffer)).thenReturn(0, messageLength);

        assertEquals(messageLength, readContext.read());

        assertEquals(bytes.length, channelBuffer.getIndex());
        verify(readConsumer, times(1)).consumeReads(channelBuffer);

        when(readConsumer.consumeReads(channelBuffer)).thenReturn(messageLength * 2, 0);

        assertEquals(messageLength, readContext.read());

        assertEquals(0, channelBuffer.getIndex());
        assertEquals(BigArrays.BYTE_PAGE_SIZE - (bytes.length * 2), channelBuffer.getCapacity());
        verify(readConsumer, times(3)).consumeReads(channelBuffer);
    }

    public void testReadThrowsIOException() throws IOException {
        IOException ioException = new IOException();
        when(channel.read(any(ByteBuffer[].class))).thenThrow(ioException);

        IOException ex = expectThrows(IOException.class, () -> readContext.read());
        assertSame(ioException, ex);
    }

    public void closeClosesChannelBuffer() {
        InboundChannelBuffer buffer = mock(InboundChannelBuffer.class);
        BytesReadContext readContext = new BytesReadContext(channel, readConsumer, buffer);

        readContext.close();

        verify(buffer).close();
    }

    private static byte[] createMessage(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = randomByte();
        }
        return bytes;
    }
}
