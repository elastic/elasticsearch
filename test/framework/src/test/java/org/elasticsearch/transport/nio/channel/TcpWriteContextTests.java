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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.SocketSelector;
import org.elasticsearch.transport.nio.WriteOperation;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TcpWriteContextTests extends ESTestCase {

    private SocketSelector selector;
    private ActionListener<Void> listener;
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
        assertEquals(ByteBuffer.wrap(bytes), writeOp.getByteReferences()[0].getReadByteBuffer());
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
        assertEquals(ByteBuffer.wrap(bytes), writeOp.getByteReferences()[0].getReadByteBuffer());
    }

    public void testWriteIsQueuedInChannel() throws Exception {
        assertFalse(writeContext.hasQueuedWriteOps());

        writeContext.queueWriteOperations(new WriteOperation(channel, new BytesArray(generateBytes(10)), listener));

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
        assertFalse(writeContext.hasQueuedWriteOps());

        WriteOperation writeOperation = mock(WriteOperation.class);
        writeContext.queueWriteOperations(writeOperation);

        assertTrue(writeContext.hasQueuedWriteOps());

        when(writeOperation.isFullyFlushed()).thenReturn(true);
        when(writeOperation.getListener()).thenReturn(listener);
        writeContext.flushChannel();

        verify(writeOperation).flush();
        verify(listener).onResponse(null);
        assertFalse(writeContext.hasQueuedWriteOps());
    }

    public void testPartialFlush() throws IOException {
        assertFalse(writeContext.hasQueuedWriteOps());

        WriteOperation writeOperation = mock(WriteOperation.class);
        writeContext.queueWriteOperations(writeOperation);

        assertTrue(writeContext.hasQueuedWriteOps());

        when(writeOperation.isFullyFlushed()).thenReturn(false);
        writeContext.flushChannel();

        verify(listener, times(0)).onResponse(null);
        assertTrue(writeContext.hasQueuedWriteOps());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleWritesPartialFlushes() throws IOException {
        assertFalse(writeContext.hasQueuedWriteOps());

        ActionListener listener2 = mock(ActionListener.class);
        WriteOperation writeOperation1 = mock(WriteOperation.class);
        WriteOperation writeOperation2 = mock(WriteOperation.class);
        when(writeOperation1.getListener()).thenReturn(listener);
        when(writeOperation2.getListener()).thenReturn(listener2);
        writeContext.queueWriteOperations(writeOperation1);
        writeContext.queueWriteOperations(writeOperation2);

        assertTrue(writeContext.hasQueuedWriteOps());

        when(writeOperation1.isFullyFlushed()).thenReturn(true);
        when(writeOperation2.isFullyFlushed()).thenReturn(false);
        writeContext.flushChannel();

        verify(listener).onResponse(null);
        verify(listener2, times(0)).onResponse(channel);
        assertTrue(writeContext.hasQueuedWriteOps());

        when(writeOperation2.isFullyFlushed()).thenReturn(true);

        writeContext.flushChannel();

        verify(listener2).onResponse(null);
        assertFalse(writeContext.hasQueuedWriteOps());
    }

    public void testWhenIOExceptionThrownListenerIsCalled() throws IOException {
        assertFalse(writeContext.hasQueuedWriteOps());

        WriteOperation writeOperation = mock(WriteOperation.class);
        writeContext.queueWriteOperations(writeOperation);

        assertTrue(writeContext.hasQueuedWriteOps());

        IOException exception = new IOException();
        when(writeOperation.flush()).thenThrow(exception);
        when(writeOperation.getListener()).thenReturn(listener);
        expectThrows(IOException.class, () -> writeContext.flushChannel());

        verify(listener).onFailure(exception);
        assertFalse(writeContext.hasQueuedWriteOps());
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
