/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.nio.BytesWriteHandler;
import org.elasticsearch.nio.FlushReadyWrite;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.TaskScheduler;
import org.elasticsearch.nio.WriteOperation;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SSLChannelContextTests extends ESTestCase {

    private CheckedFunction<InboundChannelBuffer, Integer, IOException> readConsumer;
    private NioSocketChannel channel;
    private SocketChannel rawChannel;
    private SSLChannelContext context;
    private InboundChannelBuffer channelBuffer;
    private NioSelector selector;
    private TaskScheduler nioTimer;
    private BiConsumer<Void, Exception> listener;
    private Consumer exceptionHandler;
    private SSLDriver sslDriver;
    private ByteBuffer readBuffer = ByteBuffer.allocate(1 << 14);
    private ByteBuffer writeBuffer = ByteBuffer.allocate(1 << 14);
    private int messageLength;

    @Before
    @SuppressWarnings("unchecked")
    public void init() {
        readConsumer = mock(CheckedFunction.class);
        TestReadWriteHandler readWriteHandler = new TestReadWriteHandler(readConsumer);

        messageLength = randomInt(96) + 20;
        selector = mock(NioSelector.class);
        nioTimer = mock(TaskScheduler.class);
        listener = mock(BiConsumer.class);
        channel = mock(NioSocketChannel.class);
        rawChannel = mock(SocketChannel.class);
        sslDriver = mock(SSLDriver.class);
        channelBuffer = InboundChannelBuffer.allocatingInstance();
        when(channel.getRawChannel()).thenReturn(rawChannel);
        exceptionHandler = mock(Consumer.class);
        context = new SSLChannelContext(channel, selector, exceptionHandler, sslDriver, readWriteHandler, channelBuffer);

        when(selector.isOnCurrentThread()).thenReturn(true);
        when(selector.getTaskScheduler()).thenReturn(nioTimer);
        when(sslDriver.getNetworkReadBuffer()).thenReturn(readBuffer);
        when(sslDriver.getNetworkWriteBuffer()).thenReturn(writeBuffer);
        ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
        when(selector.getIoBuffer()).thenAnswer(invocationOnMock -> {
            buffer.clear();
            return buffer;
        });
    }

    public void testSuccessfulRead() throws IOException {
        byte[] bytes = createMessage(messageLength);

        when(rawChannel.read(any(ByteBuffer.class))).thenReturn(bytes.length);
        doAnswer(getAnswerForBytes(bytes)).when(sslDriver).read(channelBuffer);

        when(readConsumer.apply(channelBuffer)).thenReturn(messageLength, 0);

        assertEquals(messageLength, context.read());

        assertEquals(0, channelBuffer.getIndex());
        assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE - bytes.length, channelBuffer.getCapacity());
        verify(readConsumer, times(1)).apply(channelBuffer);
    }

    public void testMultipleReadsConsumed() throws IOException {
        byte[] bytes = createMessage(messageLength * 2);

        when(rawChannel.read(any(ByteBuffer.class))).thenReturn(bytes.length);
        doAnswer(getAnswerForBytes(bytes)).when(sslDriver).read(channelBuffer);

        when(readConsumer.apply(channelBuffer)).thenReturn(messageLength, messageLength, 0);

        assertEquals(bytes.length, context.read());

        assertEquals(0, channelBuffer.getIndex());
        assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE - bytes.length, channelBuffer.getCapacity());
        verify(readConsumer, times(2)).apply(channelBuffer);
    }

    public void testPartialRead() throws IOException {
        byte[] bytes = createMessage(messageLength);

        when(rawChannel.read(any(ByteBuffer.class))).thenReturn(bytes.length);
        doAnswer(getAnswerForBytes(bytes)).when(sslDriver).read(channelBuffer);


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

    @SuppressWarnings("unchecked")
    public void testSSLDriverClosedOnClose() throws IOException {
        try (SocketChannel realChannel = SocketChannel.open()) {
            when(channel.getRawChannel()).thenReturn(realChannel);
            TestReadWriteHandler readWriteHandler = new TestReadWriteHandler(readConsumer);
            context = new SSLChannelContext(channel, selector, exceptionHandler, sslDriver, readWriteHandler, channelBuffer);
            when(channel.isOpen()).thenReturn(true);
            context.closeFromSelector();

            verify(sslDriver).close();
        }
    }

    public void testQueuedWritesAreIgnoredWhenNotReadyForAppWrites() {
        when(sslDriver.readyForApplicationWrites()).thenReturn(false);
        when(sslDriver.hasFlushPending()).thenReturn(false);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(false);

        context.queueWriteOperation(mock(FlushReadyWrite.class));

        assertFalse(context.readyForFlush());
    }

    public void testPendingFlushMeansWriteInterested() {
        when(sslDriver.readyForApplicationWrites()).thenReturn(randomBoolean());
        when(sslDriver.hasFlushPending()).thenReturn(true);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(false);

        assertTrue(context.readyForFlush());
    }

    public void testNeedsNonAppWritesMeansWriteInterested() {
        when(sslDriver.readyForApplicationWrites()).thenReturn(false);
        when(sslDriver.hasFlushPending()).thenReturn(false);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(true);

        assertTrue(context.readyForFlush());
    }

    public void testNotWritesInterestInAppMode() {
        when(sslDriver.readyForApplicationWrites()).thenReturn(true);
        when(sslDriver.hasFlushPending()).thenReturn(false);

        assertFalse(context.readyForFlush());

        verify(sslDriver, times(0)).needsNonApplicationWrite();
    }

    public void testFirstFlushMustFinishForWriteToContinue() throws Exception {
        when(sslDriver.hasFlushPending()).thenReturn(true, true);
        when(sslDriver.readyForApplicationWrites()).thenReturn(false);

        context.flushChannel();

        verify(sslDriver, times(0)).nonApplicationWrite();
    }

    public void testNonAppWrites() throws Exception {
        when(sslDriver.hasFlushPending()).thenReturn(false, false, true, false, true);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(true, true, false);
        when(sslDriver.readyForApplicationWrites()).thenReturn(false);

        context.flushChannel();

        verify(sslDriver, times(2)).nonApplicationWrite();
        verify(rawChannel, times(2)).write(same(selector.getIoBuffer()));
    }

    public void testNonAppWritesStopIfBufferNotFullyFlushed() throws Exception {
        when(sslDriver.hasFlushPending()).thenReturn(false, false, true, true);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(true, true, true, true);
        when(sslDriver.readyForApplicationWrites()).thenReturn(false);

        context.flushChannel();

        verify(sslDriver, times(1)).nonApplicationWrite();
        verify(rawChannel, times(1)).write(same(selector.getIoBuffer()));
    }

    public void testQueuedWriteIsFlushedInFlushCall() throws Exception {
        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};
        FlushReadyWrite flushOperation = mock(FlushReadyWrite.class);
        context.queueWriteOperation(flushOperation);

        when(flushOperation.getBuffersToWrite()).thenReturn(buffers);
        when(flushOperation.getListener()).thenReturn(listener);
        when(sslDriver.hasFlushPending()).thenReturn(false, false, false, false);
        when(sslDriver.readyForApplicationWrites()).thenReturn(true);
        when(sslDriver.applicationWrite(buffers)).thenReturn(10);
        when(flushOperation.isFullyFlushed()).thenReturn(false,true);
        context.flushChannel();

        verify(flushOperation).incrementIndex(10);
        verify(rawChannel, times(1)).write(same(selector.getIoBuffer()));
        verify(selector).executeListener(listener, null);
        assertFalse(context.readyForFlush());
    }

    public void testPartialFlush() throws IOException {
        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};
        FlushReadyWrite flushOperation = mock(FlushReadyWrite.class);
        context.queueWriteOperation(flushOperation);

        when(flushOperation.getBuffersToWrite()).thenReturn(buffers);
        when(flushOperation.getListener()).thenReturn(listener);
        when(sslDriver.hasFlushPending()).thenReturn(false, false, true);
        when(sslDriver.readyForApplicationWrites()).thenReturn(true);
        when(sslDriver.applicationWrite(buffers)).thenReturn(5);
        when(flushOperation.isFullyFlushed()).thenReturn(false, false);
        context.flushChannel();

        verify(rawChannel, times(1)).write(same(selector.getIoBuffer()));
        verify(selector, times(0)).executeListener(listener, null);
        assertTrue(context.readyForFlush());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleWritesPartialFlushes() throws IOException {
        BiConsumer<Void, Exception> listener2 = mock(BiConsumer.class);
        ByteBuffer[] buffers1 = {ByteBuffer.allocate(10)};
        ByteBuffer[] buffers2 = {ByteBuffer.allocate(5)};
        FlushReadyWrite flushOperation1 = mock(FlushReadyWrite.class);
        FlushReadyWrite flushOperation2 = mock(FlushReadyWrite.class);
        when(flushOperation1.getBuffersToWrite()).thenReturn(buffers1);
        when(flushOperation2.getBuffersToWrite()).thenReturn(buffers2);
        when(flushOperation1.getListener()).thenReturn(listener);
        when(flushOperation2.getListener()).thenReturn(listener2);
        context.queueWriteOperation(flushOperation1);
        context.queueWriteOperation(flushOperation2);

        when(sslDriver.hasFlushPending()).thenReturn(false, false, false, false, false, true);
        when(sslDriver.readyForApplicationWrites()).thenReturn(true);
        when(sslDriver.applicationWrite(buffers1)).thenReturn(5,  5);
        when(sslDriver.applicationWrite(buffers2)).thenReturn(3);
        when(flushOperation1.isFullyFlushed()).thenReturn(false, false, true);
        when(flushOperation2.isFullyFlushed()).thenReturn(false);
        context.flushChannel();

        verify(flushOperation1, times(2)).incrementIndex(5);
        verify(rawChannel, times(3)).write(same(selector.getIoBuffer()));
        verify(selector).executeListener(listener, null);
        verify(selector, times(0)).executeListener(listener2, null);
        assertTrue(context.readyForFlush());
    }

    public void testWhenIOExceptionThrownListenerIsCalled() throws IOException {
        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};
        FlushReadyWrite flushOperation = mock(FlushReadyWrite.class);
        context.queueWriteOperation(flushOperation);

        IOException exception = new IOException();
        when(flushOperation.getBuffersToWrite()).thenReturn(buffers);
        when(flushOperation.getListener()).thenReturn(listener);
        when(sslDriver.hasFlushPending()).thenReturn(false, false);
        when(sslDriver.readyForApplicationWrites()).thenReturn(true);
        when(sslDriver.applicationWrite(buffers)).thenReturn(5);
        when(rawChannel.write(any(ByteBuffer.class))).thenThrow(exception);
        when(flushOperation.isFullyFlushed()).thenReturn(false);
        expectThrows(IOException.class, () -> context.flushChannel());

        verify(flushOperation).incrementIndex(5);
        verify(selector).executeFailedListener(listener, exception);
        assertFalse(context.readyForFlush());
    }

    public void testWriteIOExceptionMeansChannelReadyToClose() throws Exception {
        when(sslDriver.hasFlushPending()).thenReturn(true);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(true);
        when(sslDriver.readyForApplicationWrites()).thenReturn(false);
        when(rawChannel.write(any(ByteBuffer.class))).thenThrow(new IOException());

        assertFalse(context.selectorShouldClose());
        expectThrows(IOException.class, () -> context.flushChannel());
        assertTrue(context.selectorShouldClose());
    }

    public void testReadyToCloseIfDriverIndicateClosed() {
        when(sslDriver.isClosed()).thenReturn(false, true);
        assertFalse(context.selectorShouldClose());
        assertTrue(context.selectorShouldClose());
    }

    public void testCloseTimeout() {
        context.closeChannel();

        ArgumentCaptor<WriteOperation> captor = ArgumentCaptor.forClass(WriteOperation.class);
        verify(selector).writeToChannel(captor.capture());

        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        Runnable cancellable = mock(Runnable.class);
        when(nioTimer.scheduleAtRelativeTime(taskCaptor.capture(), anyLong())).thenReturn(cancellable);
        context.queueWriteOperation(captor.getValue());
        verify(nioTimer).scheduleAtRelativeTime(taskCaptor.capture(), anyLong());
        assertFalse(context.selectorShouldClose());
        taskCaptor.getValue().run();
        assertTrue(context.selectorShouldClose());
        verify(selector).queueChannelClose(channel);
        verify(cancellable, never()).run();
    }

    @SuppressWarnings("unchecked")
    public void testCloseTimeoutIsCancelledOnClose() throws IOException {
        try (SocketChannel realChannel = SocketChannel.open()) {
            when(channel.getRawChannel()).thenReturn(realChannel);
            TestReadWriteHandler readWriteHandler = new TestReadWriteHandler(readConsumer);
            context = new SSLChannelContext(channel, selector, exceptionHandler, sslDriver, readWriteHandler, channelBuffer);
            context.closeChannel();
            ArgumentCaptor<WriteOperation> captor = ArgumentCaptor.forClass(WriteOperation.class);
            verify(selector).writeToChannel(captor.capture());
            ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
            Runnable cancellable = mock(Runnable.class);
            when(nioTimer.scheduleAtRelativeTime(taskCaptor.capture(), anyLong())).thenReturn(cancellable);
            context.queueWriteOperation(captor.getValue());

            when(channel.isOpen()).thenReturn(true);
            context.closeFromSelector();
            verify(cancellable).run();
        }
    }

    public void testInitiateCloseFromDifferentThreadSchedulesCloseNotify() {
        when(selector.isOnCurrentThread()).thenReturn(false, true);
        context.closeChannel();

        ArgumentCaptor<FlushReadyWrite> captor = ArgumentCaptor.forClass(FlushReadyWrite.class);
        verify(selector).queueWrite(captor.capture());

        context.queueWriteOperation(captor.getValue());
        verify(sslDriver).initiateClose();
    }

    public void testInitiateCloseFromSameThreadSchedulesCloseNotify() {
        context.closeChannel();

        ArgumentCaptor<WriteOperation> captor = ArgumentCaptor.forClass(WriteOperation.class);
        verify(selector).writeToChannel(captor.capture());

        context.queueWriteOperation(captor.getValue());
        verify(sslDriver).initiateClose();
    }

    @SuppressWarnings("unchecked")
    public void testRegisterInitiatesDriver() throws IOException {
        try (Selector realSelector = Selector.open();
             SocketChannel realSocket = SocketChannel.open()) {
            realSocket.configureBlocking(false);
            when(selector.rawSelector()).thenReturn(realSelector);
            when(channel.getRawChannel()).thenReturn(realSocket);
            TestReadWriteHandler readWriteHandler = new TestReadWriteHandler(readConsumer);
            context = new SSLChannelContext(channel, selector, exceptionHandler, sslDriver, readWriteHandler, channelBuffer);
            context.register();
            verify(sslDriver).init();
        }
    }

    private Answer getAnswerForBytes(byte[] bytes) {
        return invocationOnMock -> {
            InboundChannelBuffer buffer = (InboundChannelBuffer) invocationOnMock.getArguments()[0];
            buffer.ensureCapacity(buffer.getIndex() + bytes.length);
            ByteBuffer[] buffers = buffer.sliceBuffersFrom(buffer.getIndex());
            assert buffers[0].remaining() > bytes.length;
            buffers[0].put(bytes);
            buffer.incrementIndex(bytes.length);
            return bytes.length;
        };
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
