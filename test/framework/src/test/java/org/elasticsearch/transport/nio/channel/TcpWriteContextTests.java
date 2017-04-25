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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.SocketSelector;
import org.elasticsearch.transport.nio.WriteOperation;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TcpWriteContextTests extends ESTestCase {

    private SocketSelector selector;
    private ActionListener<NioChannel> listener;
    private TcpWriteContext writeContext;
    private NioSocketChannel channel;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        selector = mock(SocketSelector.class);
        listener = mock(ActionListener.class);
        channel = mock(NioSocketChannel.class);
        writeContext = new TcpWriteContext(channel);

        when(channel.getSelector()).thenReturn(selector);
        when(selector.isOnCurrentThread()).thenReturn(true);
    }

    public void testWriteFailsIfChannelNotWritable() throws Exception {
        when(channel.isWritable()).thenReturn(false);

        writeContext.sendMessage(new BytesArray(generateBytes(10)), listener);

        verify(listener).onFailure(any(ClosedChannelException.class));
    }

    public void testSendMessageFromDifferentThreadIsQueuedWithSelector() throws Exception {
        byte[] bytes = generateBytes(10);
        BytesArray bytesArray = new BytesArray(bytes);
        ArgumentCaptor<WriteOperation> writeOpCaptor = ArgumentCaptor.forClass(WriteOperation.class);

        when(selector.isOnCurrentThread()).thenReturn(false);
        when(channel.isWritable()).thenReturn(true);

        writeContext.sendMessage(bytesArray, listener);

        verify(selector).queueWrite(writeOpCaptor.capture());
        WriteOperation writeOp = writeOpCaptor.getValue();

        assertSame(listener, writeOp.getListener());
        assertSame(channel, writeOp.getChannel());
        assertEquals(ByteBuffer.wrap(bytes), writeOp.getBuffers()[0]);
    }

    public void testSendMessageFromSameThreadIsQueuedInChannel() throws Exception {
        byte[] bytes = generateBytes(10);
        BytesArray bytesArray = new BytesArray(bytes);
        ArgumentCaptor<WriteOperation> writeOpCaptor = ArgumentCaptor.forClass(WriteOperation.class);

        when(channel.isWritable()).thenReturn(true);

        writeContext.sendMessage(bytesArray, listener);

        verify(selector).queueWriteInChannelBuffer(writeOpCaptor.capture());
        WriteOperation writeOp = writeOpCaptor.getValue();

        assertSame(listener, writeOp.getListener());
        assertSame(channel, writeOp.getChannel());
        assertEquals(ByteBuffer.wrap(bytes), writeOp.getBuffers()[0]);
    }

    public void testWriteIsQueuedInChannel() throws Exception {
        assertFalse(writeContext.hasQueuedWriteOps());

        writeContext.queueWriteOperations(new WriteOperation(channel,  new BytesArray(generateBytes(10)), listener));

        assertTrue(writeContext.hasQueuedWriteOps());
    }

    public void testWriteOpsCanBeCleared() throws Exception {
        assertFalse(writeContext.hasQueuedWriteOps());

        writeContext.queueWriteOperations(new WriteOperation(channel,  new BytesArray(generateBytes(10)), listener));

        assertTrue(writeContext.hasQueuedWriteOps());

        ClosedChannelException e = new ClosedChannelException();
        writeContext.clearQueuedWriteOps(e);

        verify(listener).onFailure(e);

        assertFalse(writeContext.hasQueuedWriteOps());
    }

    public void testQueuedWriteIsFlushedInFlushCall() throws Exception {
        ConsumeAllChannel consumeAllChannel = new ConsumeAllChannel();
        byte[] expectedBytes = generateBytes(10);

        assertFalse(writeContext.hasQueuedWriteOps());

        writeContext.queueWriteOperations(new WriteOperation(channel,  new BytesArray(expectedBytes), listener));

        assertTrue(writeContext.hasQueuedWriteOps());

        when(channel.write(any(ByteBuffer.class))).thenAnswer(invocationOnMock ->
            consumeAllChannel.write((ByteBuffer) invocationOnMock.getArguments()[0]));
        writeContext.flushChannel();

        verify(listener).onResponse(channel);
        assertArrayEquals(expectedBytes, consumeAllChannel.bytes);
        assertFalse(writeContext.hasQueuedWriteOps());
    }

    public void testQueuedWriteWithMultipleBuffersIsFlushedInFlushCall() throws Exception {
        ConsumeAllChannel consumeAllChannel = new ConsumeAllChannel();
        byte[] expectedBytes = generateBytes(10);
        byte[] expectedBytes2 = generateBytes(10);
        CompositeBytesReference reference = new CompositeBytesReference(new BytesArray(expectedBytes), new BytesArray(expectedBytes2));

        assertFalse(writeContext.hasQueuedWriteOps());

        writeContext.queueWriteOperations(new WriteOperation(channel,  reference, listener));

        assertTrue(writeContext.hasQueuedWriteOps());

        when(channel.vectorizedWrite(any())).thenAnswer(invocationOnMock ->
            consumeAllChannel.vectorizedWrite((ByteBuffer[]) invocationOnMock.getArguments()[0]));
        writeContext.flushChannel();

        verify(listener).onResponse(channel);
        assertArrayEquals(expectedBytes, consumeAllChannel.bytes);
        assertArrayEquals(expectedBytes2, consumeAllChannel.bytes2);
        assertFalse(writeContext.hasQueuedWriteOps());
    }

    public void testPartialFlush() throws IOException {
        HalfConsumeChannel halfConsumeChannel = new HalfConsumeChannel();
        byte[] expectedBytes = generateBytes(10);
        byte[] expectedBytes2 = generateBytes(10);
        CompositeBytesReference reference = new CompositeBytesReference(new BytesArray(expectedBytes), new BytesArray(expectedBytes2));

        assertFalse(writeContext.hasQueuedWriteOps());

        writeContext.queueWriteOperations(new WriteOperation(channel,  reference, listener));

        assertTrue(writeContext.hasQueuedWriteOps());

        when(channel.vectorizedWrite(any())).thenAnswer(invocationOnMock ->
            halfConsumeChannel.vectorizedWrite((ByteBuffer[]) invocationOnMock.getArguments()[0]));
        writeContext.flushChannel();

        verify(listener, times(0)).onResponse(channel);
        assertArrayEquals(expectedBytes, halfConsumeChannel.bytes);
        assertArrayEquals(new byte[expectedBytes2.length], halfConsumeChannel.bytes2);
        assertTrue(writeContext.hasQueuedWriteOps());
    }

    public void testMultiplePartialFlushes() throws IOException {
        HalfConsumeChannel halfConsumeChannel = new HalfConsumeChannel();
        byte[] expectedBytes = generateBytes(10);
        byte[] expectedBytes2 = generateBytes(10);
        CompositeBytesReference reference = new CompositeBytesReference(new BytesArray(expectedBytes), new BytesArray(expectedBytes2));

        assertFalse(writeContext.hasQueuedWriteOps());

        writeContext.queueWriteOperations(new WriteOperation(channel,  reference, listener));

        assertTrue(writeContext.hasQueuedWriteOps());

        when(channel.vectorizedWrite(any())).thenAnswer(invocationOnMock ->
            halfConsumeChannel.vectorizedWrite((ByteBuffer[]) invocationOnMock.getArguments()[0]));
        writeContext.flushChannel();

        assertArrayEquals(expectedBytes, halfConsumeChannel.bytes);
        assertArrayEquals(new byte[expectedBytes2.length], halfConsumeChannel.bytes2);

        assertTrue(writeContext.hasQueuedWriteOps());

        writeContext.flushChannel();

        verify(listener).onResponse(channel);

        assertArrayEquals(expectedBytes, halfConsumeChannel.bytes);
        assertArrayEquals(expectedBytes2, halfConsumeChannel.bytes2);
        assertFalse(writeContext.hasQueuedWriteOps());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleWritesPartialFlushes() throws IOException {
        MultiWriteChannel multiWriteChannel = new MultiWriteChannel();
        byte[] expectedBytes = generateBytes(10);
        byte[] expectedBytes2 = generateBytes(10);
        byte[] expectedBytes3 = generateBytes(10);
        byte[] expectedBytes4 = generateBytes(10);
        CompositeBytesReference reference = new CompositeBytesReference(new BytesArray(expectedBytes), new BytesArray(expectedBytes2));
        CompositeBytesReference reference2 = new CompositeBytesReference(new BytesArray(expectedBytes3), new BytesArray(expectedBytes4));

        assertFalse(writeContext.hasQueuedWriteOps());

        ActionListener listener2 = mock(ActionListener.class);
        writeContext.queueWriteOperations(new WriteOperation(channel,  reference, listener));
        writeContext.queueWriteOperations(new WriteOperation(channel,  reference2, listener2));

        assertTrue(writeContext.hasQueuedWriteOps());

        when(channel.vectorizedWrite(any())).thenAnswer(invocationOnMock ->
            multiWriteChannel.vectorizedWrite((ByteBuffer[]) invocationOnMock.getArguments()[0]));
        writeContext.flushChannel();

        verify(listener).onResponse(channel);
        verify(listener2, times(0)).onResponse(channel);

        assertArrayEquals(expectedBytes, multiWriteChannel.write1Bytes);
        assertArrayEquals(expectedBytes2, multiWriteChannel.write1Bytes2);
        assertArrayEquals(expectedBytes3, multiWriteChannel.write2Bytes1);
        assertArrayEquals(new byte[expectedBytes4.length], multiWriteChannel.write2Bytes2);

        assertTrue(writeContext.hasQueuedWriteOps());

        writeContext.flushChannel();

        verify(listener2).onResponse(channel);

        assertArrayEquals(expectedBytes, multiWriteChannel.write1Bytes);
        assertArrayEquals(expectedBytes2, multiWriteChannel.write1Bytes2);
        assertArrayEquals(expectedBytes3, multiWriteChannel.write2Bytes1);
        assertArrayEquals(expectedBytes4, multiWriteChannel.write2Bytes2);

        assertFalse(writeContext.hasQueuedWriteOps());
    }

    private class ConsumeAllChannel extends NioSocketChannel {

        private byte[] bytes;
        private byte[] bytes2;

        ConsumeAllChannel() throws IOException {
            super("", mock(SocketChannel.class));
        }

        public int write(ByteBuffer buffer) throws IOException {
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes.length;
        }

        public long vectorizedWrite(ByteBuffer[] buffer) throws IOException {
            if (buffer.length != 2) {
                throw new IOException("Only allows 2 buffers");
            }
            bytes = new byte[buffer[0].remaining()];
            buffer[0].get(bytes);

            bytes2 = new byte[buffer[1].remaining()];
            buffer[1].get(bytes2);
            return bytes.length + bytes2.length;
        }
    }

    private class HalfConsumeChannel extends NioSocketChannel {

        private byte[] bytes;
        private byte[] bytes2;

        HalfConsumeChannel() throws IOException {
            super("", mock(SocketChannel.class));
        }

        public int write(ByteBuffer buffer) throws IOException {
            bytes = new byte[buffer.limit() / 2];
            buffer.get(bytes);
            return bytes.length;
        }

        public long vectorizedWrite(ByteBuffer[] buffers) throws IOException {
            if (buffers.length != 2) {
                throw new IOException("Only allows 2 buffers");
            }
            if (bytes == null) {
                bytes = new byte[buffers[0].remaining()];
                bytes2 = new byte[buffers[1].remaining()];
            }

            if (buffers[0].remaining() != 0) {
                buffers[0].get(bytes);
                return bytes.length;
            } else {
                buffers[1].get(bytes2);
                return bytes2.length;
            }
        }
    }

    private class MultiWriteChannel extends NioSocketChannel {

        private byte[] write1Bytes;
        private byte[] write1Bytes2;
        private byte[] write2Bytes1;
        private byte[] write2Bytes2;

        MultiWriteChannel() throws IOException {
            super("", mock(SocketChannel.class));
        }

        public long vectorizedWrite(ByteBuffer[] buffers) throws IOException {
            if (buffers.length != 4 && write1Bytes == null) {
                throw new IOException("Only allows 4 buffers");
            } else if (buffers.length != 2 && write1Bytes != null) {
                throw new IOException("Only allows 2 buffers on second write");
            }
            if (write1Bytes == null) {
                write1Bytes = new byte[buffers[0].remaining()];
                write1Bytes2 = new byte[buffers[1].remaining()];
                write2Bytes1 = new byte[buffers[2].remaining()];
                write2Bytes2 = new byte[buffers[3].remaining()];
            }

            if (buffers[0].remaining() != 0) {
                buffers[0].get(write1Bytes);
                buffers[1].get(write1Bytes2);
                buffers[2].get(write2Bytes1);
                return write1Bytes.length + write1Bytes2.length + write2Bytes1.length;
            } else {
                buffers[1].get(write2Bytes2);
                return write2Bytes2.length;
            }
        }
    }

    private byte[] generateBytes(int n) {
        n += 10;
        byte[] bytes = new byte[n];
        for (int i = 0; i < n; ++i) {
            bytes[i] = randomByte();
        }
        return bytes;
    }

}
