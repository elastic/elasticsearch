/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BytesChannelContextTests extends ESTestCase {

    private CheckedFunction<InboundChannelBuffer, Integer, IOException> readConsumer;
    private NioSocketChannel channel;
    private SocketChannel rawChannel;
    private BytesChannelContext context;
    private InboundChannelBuffer channelBuffer;
    private NioSelector selector;
    private BiConsumer<Void, Exception> listener;
    private int messageLength;

    @Before
    @SuppressWarnings("unchecked")
    public void init() {
        readConsumer = mock(CheckedFunction.class);

        messageLength = randomInt(96) + 20;
        selector = mock(NioSelector.class);
        listener = mock(BiConsumer.class);
        channel = mock(NioSocketChannel.class);
        rawChannel = mock(SocketChannel.class);
        channelBuffer = InboundChannelBuffer.allocatingInstance();
        TestReadWriteHandler handler = new TestReadWriteHandler(readConsumer);
        when(channel.getRawChannel()).thenReturn(rawChannel);
        context = new BytesChannelContext(channel, selector, mock(Config.Socket.class), mock(Consumer.class), handler, channelBuffer);

        when(selector.isOnCurrentThread()).thenReturn(true);
        ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
        when(selector.getIoBuffer()).thenAnswer(invocationOnMock -> {
            buffer.clear();
            return buffer;
        });
    }

    public void testSuccessfulRead() throws IOException {
        byte[] bytes = createMessage(messageLength);

        when(rawChannel.read(any(ByteBuffer.class))).thenAnswer(invocationOnMock -> {
            ByteBuffer buffer = (ByteBuffer) invocationOnMock.getArguments()[0];
            buffer.put(bytes);
            return bytes.length;
        });

        when(readConsumer.apply(channelBuffer)).thenReturn(messageLength, 0);

        assertEquals(messageLength, context.read());

        assertEquals(0, channelBuffer.getIndex());
        assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE - bytes.length, channelBuffer.getCapacity());
        verify(readConsumer, times(1)).apply(channelBuffer);
    }

    public void testMultipleReadsConsumed() throws IOException {
        byte[] bytes = createMessage(messageLength * 2);

        when(rawChannel.read(any(ByteBuffer.class))).thenAnswer(invocationOnMock -> {
            ByteBuffer buffer = (ByteBuffer) invocationOnMock.getArguments()[0];
            buffer.put(bytes);
            return bytes.length;
        });

        when(readConsumer.apply(channelBuffer)).thenReturn(messageLength, messageLength, 0);

        assertEquals(bytes.length, context.read());

        assertEquals(0, channelBuffer.getIndex());
        assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE - bytes.length, channelBuffer.getCapacity());
        verify(readConsumer, times(2)).apply(channelBuffer);
    }

    public void testPartialRead() throws IOException {
        byte[] bytes = createMessage(messageLength);

        when(rawChannel.read(any(ByteBuffer.class))).thenAnswer(invocationOnMock -> {
            ByteBuffer buffer = (ByteBuffer) invocationOnMock.getArguments()[0];
            buffer.put(bytes);
            return bytes.length;
        });


        when(readConsumer.apply(channelBuffer)).thenReturn(0);

        assertEquals(messageLength, context.read());

        assertEquals(bytes.length, channelBuffer.getIndex());
        verify(readConsumer, times(1)).apply(channelBuffer);

        when(readConsumer.apply(channelBuffer)).thenReturn(messageLength * 2, 0);

        assertEquals(messageLength, context.read());

        assertEquals(0, channelBuffer.getIndex());
        assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE - (bytes.length * 2), channelBuffer.getCapacity());
        verify(readConsumer, times(2)).apply(channelBuffer);
    }

    public void testReadThrowsIOException() throws IOException {
        IOException ioException = new IOException();
        when(rawChannel.read(any(ByteBuffer.class))).thenThrow(ioException);

        IOException ex = expectThrows(IOException.class, () -> context.read());
        assertSame(ioException, ex);
    }

    public void testReadThrowsIOExceptionMeansReadyForClose() throws IOException {
        when(rawChannel.read(any(ByteBuffer.class))).thenThrow(new IOException());

        assertFalse(context.selectorShouldClose());
        expectThrows(IOException.class, () -> context.read());
        assertTrue(context.selectorShouldClose());
    }

    public void testReadLessThanZeroMeansReadyForClose() throws IOException {
        when(rawChannel.read(any(ByteBuffer.class))).thenReturn(-1);

        assertEquals(0, context.read());

        assertTrue(context.selectorShouldClose());
    }

    @SuppressWarnings("varargs")
    public void testQueuedWriteIsFlushedInFlushCall() throws Exception {
        assertFalse(context.readyForFlush());

        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};

        FlushReadyWrite flushOperation = mock(FlushReadyWrite.class);
        context.queueWriteOperation(flushOperation);

        assertTrue(context.readyForFlush());

        when(flushOperation.getBuffersToWrite(anyInt())).thenReturn(buffers);
        when(flushOperation.isFullyFlushed()).thenReturn(false, true);
        when(flushOperation.getListener()).thenReturn(listener);
        context.flushChannel();

        ByteBuffer buffer = buffers[0].duplicate();
        buffer.flip();
        verify(rawChannel).write(eq(buffer));
        verify(selector).executeListener(listener, null);
        assertFalse(context.readyForFlush());
    }

    public void testPartialFlush() throws IOException {
        assertFalse(context.readyForFlush());
        FlushReadyWrite flushOperation = mock(FlushReadyWrite.class);
        context.queueWriteOperation(flushOperation);
        assertTrue(context.readyForFlush());

        when(flushOperation.isFullyFlushed()).thenReturn(false);
        when(flushOperation.getBuffersToWrite(anyInt())).thenReturn(new ByteBuffer[] {ByteBuffer.allocate(3)});
        context.flushChannel();

        verify(listener, times(0)).accept(null, null);
        assertTrue(context.readyForFlush());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleWritesPartialFlushes() throws IOException {
        assertFalse(context.readyForFlush());

        BiConsumer<Void, Exception> listener2 = mock(BiConsumer.class);
        FlushReadyWrite flushOperation1 = mock(FlushReadyWrite.class);
        FlushReadyWrite flushOperation2 = mock(FlushReadyWrite.class);
        when(flushOperation1.getBuffersToWrite(anyInt())).thenReturn(new ByteBuffer[] {ByteBuffer.allocate(3)});
        when(flushOperation2.getBuffersToWrite(anyInt())).thenReturn(new ByteBuffer[] {ByteBuffer.allocate(3)});
        when(flushOperation1.getListener()).thenReturn(listener);
        when(flushOperation2.getListener()).thenReturn(listener2);

        context.queueWriteOperation(flushOperation1);
        context.queueWriteOperation(flushOperation2);

        assertTrue(context.readyForFlush());

        when(flushOperation1.isFullyFlushed()).thenReturn(false, true);
        when(flushOperation2.isFullyFlushed()).thenReturn(false);
        context.flushChannel();

        verify(selector).executeListener(listener, null);
        verify(listener2, times(0)).accept(null, null);
        assertTrue(context.readyForFlush());

        when(flushOperation2.isFullyFlushed()).thenReturn(false, true);

        context.flushChannel();

        verify(selector).executeListener(listener2, null);
        assertFalse(context.readyForFlush());
    }

    public void testWhenIOExceptionThrownListenerIsCalled() throws IOException {
        assertFalse(context.readyForFlush());

        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};
        FlushReadyWrite flushOperation = mock(FlushReadyWrite.class);
        context.queueWriteOperation(flushOperation);

        assertTrue(context.readyForFlush());

        IOException exception = new IOException();
        when(flushOperation.getBuffersToWrite(anyInt())).thenReturn(buffers);
        when(rawChannel.write(any(ByteBuffer.class))).thenThrow(exception);
        when(flushOperation.getListener()).thenReturn(listener);
        expectThrows(IOException.class, () -> context.flushChannel());

        verify(selector).executeFailedListener(listener, exception);
        assertFalse(context.readyForFlush());
    }

    public void testWriteIOExceptionMeansChannelReadyToClose() throws IOException {
        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};
        FlushReadyWrite flushOperation = mock(FlushReadyWrite.class);
        context.queueWriteOperation(flushOperation);

        IOException exception = new IOException();
        when(flushOperation.getBuffersToWrite(anyInt())).thenReturn(buffers);
        when(rawChannel.write(any(ByteBuffer.class))).thenThrow(exception);

        assertFalse(context.selectorShouldClose());
        expectThrows(IOException.class, () -> context.flushChannel());
        assertTrue(context.selectorShouldClose());
    }

    public void testInitiateCloseSchedulesCloseWithSelector() {
        context.closeChannel();
        verify(selector).queueChannelClose(channel);
    }

    private static byte[] createMessage(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = randomByte();
        }
        return bytes;
    }

    private static class TestReadWriteHandler extends BytesWriteHandler {

        private final CheckedFunction<InboundChannelBuffer, Integer, IOException> fn;

        private TestReadWriteHandler(CheckedFunction<InboundChannelBuffer, Integer, IOException> fn) {
            this.fn = fn;
        }

        @Override
        public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
            return fn.apply(channelBuffer);
        }
    }
}
