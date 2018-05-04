/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.nio.BytesWriteOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.SocketSelector;
import org.elasticsearch.nio.WriteOperation;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SSLChannelContextTests extends ESTestCase {

    private SocketChannelContext.ReadConsumer readConsumer;
    private NioSocketChannel channel;
    private SocketChannel rawChannel;
    private SSLChannelContext context;
    private InboundChannelBuffer channelBuffer;
    private SocketSelector selector;
    private BiConsumer<Void, Throwable> listener;
    private Consumer exceptionHandler;
    private SSLDriver sslDriver;
    private ByteBuffer readBuffer = ByteBuffer.allocate(1 << 14);
    private ByteBuffer writeBuffer = ByteBuffer.allocate(1 << 14);
    private int messageLength;

    @Before
    @SuppressWarnings("unchecked")
    public void init() {
        readConsumer = mock(SocketChannelContext.ReadConsumer.class);

        messageLength = randomInt(96) + 20;
        selector = mock(SocketSelector.class);
        listener = mock(BiConsumer.class);
        channel = mock(NioSocketChannel.class);
        rawChannel = mock(SocketChannel.class);
        sslDriver = mock(SSLDriver.class);
        channelBuffer = InboundChannelBuffer.allocatingInstance();
        when(channel.getRawChannel()).thenReturn(rawChannel);
        exceptionHandler = mock(Consumer.class);
        context = new SSLChannelContext(channel, selector, exceptionHandler, sslDriver, readConsumer, channelBuffer);

        when(selector.isOnCurrentThread()).thenReturn(true);
        when(sslDriver.getNetworkReadBuffer()).thenReturn(readBuffer);
        when(sslDriver.getNetworkWriteBuffer()).thenReturn(writeBuffer);
    }

    public void testSuccessfulRead() throws IOException {
        byte[] bytes = createMessage(messageLength);

        when(rawChannel.read(same(readBuffer))).thenReturn(bytes.length);
        doAnswer(getAnswerForBytes(bytes)).when(sslDriver).read(channelBuffer);

        when(readConsumer.consumeReads(channelBuffer)).thenReturn(messageLength, 0);

        assertEquals(messageLength, context.read());

        assertEquals(0, channelBuffer.getIndex());
        assertEquals(BigArrays.BYTE_PAGE_SIZE - bytes.length, channelBuffer.getCapacity());
        verify(readConsumer, times(1)).consumeReads(channelBuffer);
    }

    public void testMultipleReadsConsumed() throws IOException {
        byte[] bytes = createMessage(messageLength * 2);

        when(rawChannel.read(same(readBuffer))).thenReturn(bytes.length);
        doAnswer(getAnswerForBytes(bytes)).when(sslDriver).read(channelBuffer);

        when(readConsumer.consumeReads(channelBuffer)).thenReturn(messageLength, messageLength, 0);

        assertEquals(bytes.length, context.read());

        assertEquals(0, channelBuffer.getIndex());
        assertEquals(BigArrays.BYTE_PAGE_SIZE - bytes.length, channelBuffer.getCapacity());
        verify(readConsumer, times(2)).consumeReads(channelBuffer);
    }

    public void testPartialRead() throws IOException {
        byte[] bytes = createMessage(messageLength);

        when(rawChannel.read(same(readBuffer))).thenReturn(bytes.length);
        doAnswer(getAnswerForBytes(bytes)).when(sslDriver).read(channelBuffer);


        when(readConsumer.consumeReads(channelBuffer)).thenReturn(0);

        assertEquals(messageLength, context.read());

        assertEquals(bytes.length, channelBuffer.getIndex());
        verify(readConsumer, times(1)).consumeReads(channelBuffer);

        when(readConsumer.consumeReads(channelBuffer)).thenReturn(messageLength * 2, 0);

        assertEquals(messageLength, context.read());

        assertEquals(0, channelBuffer.getIndex());
        assertEquals(BigArrays.BYTE_PAGE_SIZE - (bytes.length * 2), channelBuffer.getCapacity());
        verify(readConsumer, times(2)).consumeReads(channelBuffer);
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
    public void testCloseClosesChannelBuffer() throws IOException {
        try (SocketChannel realChannel = SocketChannel.open()) {
            when(channel.getRawChannel()).thenReturn(realChannel);

            AtomicInteger closeCount = new AtomicInteger(0);
            Supplier<InboundChannelBuffer.Page> pageSupplier = () -> new InboundChannelBuffer.Page(ByteBuffer.allocate(1 << 14),
                    closeCount::incrementAndGet);
            InboundChannelBuffer buffer = new InboundChannelBuffer(pageSupplier);
            buffer.ensureCapacity(1);
            SSLChannelContext context = new SSLChannelContext(channel, selector, exceptionHandler, sslDriver, readConsumer, buffer);
            when(channel.isOpen()).thenReturn(true);
            context.closeFromSelector();
            assertEquals(1, closeCount.get());
        }
    }

    @SuppressWarnings("unchecked")
    public void testWriteOpsClearedOnClose() throws IOException {
        try (SocketChannel realChannel = SocketChannel.open()) {
            when(channel.getRawChannel()).thenReturn(realChannel);
            context = new SSLChannelContext(channel, selector, exceptionHandler, sslDriver, readConsumer, channelBuffer);
            assertFalse(context.hasQueuedWriteOps());

            ByteBuffer[] buffer = {ByteBuffer.allocate(10)};
            context.queueWriteOperation(new BytesWriteOperation(context,  buffer, listener));

            when(sslDriver.readyForApplicationWrites()).thenReturn(true);
            assertTrue(context.hasQueuedWriteOps());

            when(channel.isOpen()).thenReturn(true);
            context.closeFromSelector();

            verify(selector).executeFailedListener(same(listener), any(ClosedChannelException.class));

            assertFalse(context.hasQueuedWriteOps());
        }
    }

    @SuppressWarnings("unchecked")
    public void testSSLDriverClosedOnClose() throws IOException {
        try (SocketChannel realChannel = SocketChannel.open()) {
            when(channel.getRawChannel()).thenReturn(realChannel);
            context = new SSLChannelContext(channel, selector, exceptionHandler, sslDriver, readConsumer, channelBuffer);
            when(channel.isOpen()).thenReturn(true);
            context.closeFromSelector();

            verify(sslDriver).close();
        }
    }

    public void testWriteFailsIfClosing() {
        context.closeChannel();

        ByteBuffer[] buffers = {ByteBuffer.wrap(createMessage(10))};
        context.sendMessage(buffers, listener);

        verify(listener).accept(isNull(Void.class), any(ClosedChannelException.class));
    }

    public void testSendMessageFromDifferentThreadIsQueuedWithSelector() throws Exception {
        ArgumentCaptor<BytesWriteOperation> writeOpCaptor = ArgumentCaptor.forClass(BytesWriteOperation.class);

        when(selector.isOnCurrentThread()).thenReturn(false);

        ByteBuffer[] buffers = {ByteBuffer.wrap(createMessage(10))};
        context.sendMessage(buffers, listener);

        verify(selector).queueWrite(writeOpCaptor.capture());
        BytesWriteOperation writeOp = writeOpCaptor.getValue();

        assertSame(listener, writeOp.getListener());
        assertSame(context, writeOp.getChannel());
        assertEquals(buffers[0], writeOp.getBuffersToWrite()[0]);
    }

    public void testSendMessageFromSameThreadIsQueuedInChannel() {
        ArgumentCaptor<BytesWriteOperation> writeOpCaptor = ArgumentCaptor.forClass(BytesWriteOperation.class);

        ByteBuffer[] buffers = {ByteBuffer.wrap(createMessage(10))};
        context.sendMessage(buffers, listener);

        verify(selector).queueWriteInChannelBuffer(writeOpCaptor.capture());
        BytesWriteOperation writeOp = writeOpCaptor.getValue();

        assertSame(listener, writeOp.getListener());
        assertSame(context, writeOp.getChannel());
        assertEquals(buffers[0], writeOp.getBuffersToWrite()[0]);
    }

    public void testWriteIsQueuedInChannel() {
        when(sslDriver.readyForApplicationWrites()).thenReturn(true);
        when(sslDriver.hasFlushPending()).thenReturn(false);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(false);
        assertFalse(context.hasQueuedWriteOps());

        ByteBuffer[] buffer = {ByteBuffer.allocate(10)};
        context.queueWriteOperation(new BytesWriteOperation(context, buffer, listener));

        assertTrue(context.hasQueuedWriteOps());
    }

    public void testQueuedWritesAreIgnoredWhenNotReadyForAppWrites() {
        when(sslDriver.readyForApplicationWrites()).thenReturn(false);
        when(sslDriver.hasFlushPending()).thenReturn(false);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(false);

        ByteBuffer[] buffer = {ByteBuffer.allocate(10)};
        context.queueWriteOperation(new BytesWriteOperation(context, buffer, listener));

        assertFalse(context.hasQueuedWriteOps());
    }

    public void testPendingFlushMeansWriteInterested() {
        when(sslDriver.readyForApplicationWrites()).thenReturn(randomBoolean());
        when(sslDriver.hasFlushPending()).thenReturn(true);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(false);

        assertTrue(context.hasQueuedWriteOps());
    }

    public void testNeedsNonAppWritesMeansWriteInterested() {
        when(sslDriver.readyForApplicationWrites()).thenReturn(false);
        when(sslDriver.hasFlushPending()).thenReturn(false);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(true);

        assertTrue(context.hasQueuedWriteOps());
    }

    public void testNotWritesInterestInAppMode() {
        when(sslDriver.readyForApplicationWrites()).thenReturn(true);
        when(sslDriver.hasFlushPending()).thenReturn(false);

        assertFalse(context.hasQueuedWriteOps());

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
        verify(rawChannel, times(2)).write(sslDriver.getNetworkWriteBuffer());
    }

    public void testNonAppWritesStopIfBufferNotFullyFlushed() throws Exception {
        when(sslDriver.hasFlushPending()).thenReturn(false, false, true, true);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(true, true, true, true);
        when(sslDriver.readyForApplicationWrites()).thenReturn(false);

        context.flushChannel();

        verify(sslDriver, times(1)).nonApplicationWrite();
        verify(rawChannel, times(1)).write(sslDriver.getNetworkWriteBuffer());
    }

    public void testQueuedWriteIsFlushedInFlushCall() throws Exception {
        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};
        BytesWriteOperation writeOperation = mock(BytesWriteOperation.class);
        context.queueWriteOperation(writeOperation);

        when(writeOperation.getBuffersToWrite()).thenReturn(buffers);
        when(writeOperation.getListener()).thenReturn(listener);
        when(sslDriver.hasFlushPending()).thenReturn(false, false, false, false);
        when(sslDriver.readyForApplicationWrites()).thenReturn(true);
        when(sslDriver.applicationWrite(buffers)).thenReturn(10);
        when(writeOperation.isFullyFlushed()).thenReturn(false,true);
        context.flushChannel();

        verify(writeOperation).incrementIndex(10);
        verify(rawChannel, times(1)).write(sslDriver.getNetworkWriteBuffer());
        verify(selector).executeListener(listener, null);
        assertFalse(context.hasQueuedWriteOps());
    }

    public void testPartialFlush() throws IOException {
        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};
        BytesWriteOperation writeOperation = mock(BytesWriteOperation.class);
        context.queueWriteOperation(writeOperation);

        when(writeOperation.getBuffersToWrite()).thenReturn(buffers);
        when(writeOperation.getListener()).thenReturn(listener);
        when(sslDriver.hasFlushPending()).thenReturn(false, false, true);
        when(sslDriver.readyForApplicationWrites()).thenReturn(true);
        when(sslDriver.applicationWrite(buffers)).thenReturn(5);
        when(writeOperation.isFullyFlushed()).thenReturn(false, false);
        context.flushChannel();

        verify(writeOperation).incrementIndex(5);
        verify(rawChannel, times(1)).write(sslDriver.getNetworkWriteBuffer());
        verify(selector, times(0)).executeListener(listener, null);
        assertTrue(context.hasQueuedWriteOps());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleWritesPartialFlushes() throws IOException {
        BiConsumer<Void, Throwable> listener2 = mock(BiConsumer.class);
        ByteBuffer[] buffers1 = {ByteBuffer.allocate(10)};
        ByteBuffer[] buffers2 = {ByteBuffer.allocate(5)};
        BytesWriteOperation writeOperation1 = mock(BytesWriteOperation.class);
        BytesWriteOperation writeOperation2 = mock(BytesWriteOperation.class);
        when(writeOperation1.getBuffersToWrite()).thenReturn(buffers1);
        when(writeOperation2.getBuffersToWrite()).thenReturn(buffers2);
        when(writeOperation1.getListener()).thenReturn(listener);
        when(writeOperation2.getListener()).thenReturn(listener2);
        context.queueWriteOperation(writeOperation1);
        context.queueWriteOperation(writeOperation2);

        when(sslDriver.hasFlushPending()).thenReturn(false, false, false, false, false, true);
        when(sslDriver.readyForApplicationWrites()).thenReturn(true);
        when(sslDriver.applicationWrite(buffers1)).thenReturn(5,  5);
        when(sslDriver.applicationWrite(buffers2)).thenReturn(3);
        when(writeOperation1.isFullyFlushed()).thenReturn(false, false, true);
        when(writeOperation2.isFullyFlushed()).thenReturn(false);
        context.flushChannel();

        verify(writeOperation1, times(2)).incrementIndex(5);
        verify(rawChannel, times(3)).write(sslDriver.getNetworkWriteBuffer());
        verify(selector).executeListener(listener, null);
        verify(selector, times(0)).executeListener(listener2, null);
        assertTrue(context.hasQueuedWriteOps());
    }

    public void testWhenIOExceptionThrownListenerIsCalled() throws IOException {
        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};
        BytesWriteOperation writeOperation = mock(BytesWriteOperation.class);
        context.queueWriteOperation(writeOperation);

        IOException exception = new IOException();
        when(writeOperation.getBuffersToWrite()).thenReturn(buffers);
        when(writeOperation.getListener()).thenReturn(listener);
        when(sslDriver.hasFlushPending()).thenReturn(false, false);
        when(sslDriver.readyForApplicationWrites()).thenReturn(true);
        when(sslDriver.applicationWrite(buffers)).thenReturn(5);
        when(rawChannel.write(sslDriver.getNetworkWriteBuffer())).thenThrow(exception);
        when(writeOperation.isFullyFlushed()).thenReturn(false);
        expectThrows(IOException.class, () -> context.flushChannel());

        verify(writeOperation).incrementIndex(5);
        verify(selector).executeFailedListener(listener, exception);
        assertFalse(context.hasQueuedWriteOps());
    }

    public void testWriteIOExceptionMeansChannelReadyToClose() throws Exception {
        when(sslDriver.hasFlushPending()).thenReturn(true);
        when(sslDriver.needsNonApplicationWrite()).thenReturn(true);
        when(sslDriver.readyForApplicationWrites()).thenReturn(false);
        when(rawChannel.write(sslDriver.getNetworkWriteBuffer())).thenThrow(new IOException());

        assertFalse(context.selectorShouldClose());
        expectThrows(IOException.class, () -> context.flushChannel());
        assertTrue(context.selectorShouldClose());
    }

    public void testReadyToCloseIfDriverIndicateClosed() {
        when(sslDriver.isClosed()).thenReturn(false, true);
        assertFalse(context.selectorShouldClose());
        assertTrue(context.selectorShouldClose());
    }

    public void testInitiateCloseFromDifferentThreadSchedulesCloseNotify() {
        when(selector.isOnCurrentThread()).thenReturn(false, true);
        context.closeChannel();

        ArgumentCaptor<WriteOperation> captor = ArgumentCaptor.forClass(WriteOperation.class);
        verify(selector).queueWrite(captor.capture());

        context.queueWriteOperation(captor.getValue());
        verify(sslDriver).initiateClose();
    }

    public void testInitiateCloseFromSameThreadSchedulesCloseNotify() {
        context.closeChannel();

        ArgumentCaptor<WriteOperation> captor = ArgumentCaptor.forClass(WriteOperation.class);
        verify(selector).queueWriteInChannelBuffer(captor.capture());

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
            context = new SSLChannelContext(channel, selector, exceptionHandler, sslDriver, readConsumer, channelBuffer);
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
}
