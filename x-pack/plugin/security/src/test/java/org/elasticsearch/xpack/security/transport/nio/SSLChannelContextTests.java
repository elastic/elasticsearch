/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.nio.BytesWriteHandler;
import org.elasticsearch.nio.Config;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.FlushReadyWrite;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.Page;
import org.elasticsearch.nio.TaskScheduler;
import org.elasticsearch.nio.WriteOperation;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.net.ssl.SSLException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
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
    private SSLOutboundBuffer outboundBuffer;
    private NioSelector selector;
    private TaskScheduler nioTimer;
    private BiConsumer<Void, Exception> listener;
    private Consumer<Exception> exceptionHandler;
    private SSLDriver sslDriver;
    private int messageLength;
    private Config.Socket socketConfig;

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
        outboundBuffer = new SSLOutboundBuffer((n) -> new Page(ByteBuffer.allocate(n), () -> {}));
        when(channel.getRawChannel()).thenReturn(rawChannel);
        exceptionHandler = mock(Consumer.class);
        socketConfig = new Config.Socket(
            randomBoolean(),
            randomBoolean(),
            -1,
            -1,
            -1,
            randomBoolean(),
            -1,
            -1,
            mock(InetSocketAddress.class),
            false
        );
        context = new SSLChannelContext(channel, selector, socketConfig, exceptionHandler, sslDriver, readWriteHandler, channelBuffer);
        context.setSelectionKey(mock(SelectionKey.class));

        when(selector.isOnCurrentThread()).thenReturn(true);
        when(selector.getTaskScheduler()).thenReturn(nioTimer);
        when(sslDriver.getOutboundBuffer()).thenReturn(outboundBuffer);
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
        doAnswer(getReadAnswerForBytes(bytes)).when(sslDriver).read(any(InboundChannelBuffer.class), eq(channelBuffer));

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
        doAnswer(getReadAnswerForBytes(bytes)).when(sslDriver).read(any(InboundChannelBuffer.class), eq(channelBuffer));

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
        doAnswer(getReadAnswerForBytes(bytes)).when(sslDriver).read(any(InboundChannelBuffer.class), eq(channelBuffer));

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
            context = new SSLChannelContext(channel, selector, socketConfig, exceptionHandler, sslDriver, readWriteHandler, channelBuffer);
            when(channel.isOpen()).thenReturn(true);
            context.closeFromSelector();

            verify(sslDriver).close();
        }
    }

    public void testQueuedWritesAreIgnoredWhenNotReadyForAppWrites() {
        when(sslDriver.readyForApplicationData()).thenReturn(false);

        context.queueWriteOperation(mock(FlushReadyWrite.class));

        assertFalse(context.readyForFlush());
    }

    public void testPendingEncryptedFlushMeansWriteInterested() throws Exception {
        context.queueWriteOperation(mock(FlushReadyWrite.class));
        when(sslDriver.readyForApplicationData()).thenReturn(true);
        doAnswer(getWriteAnswer(1, false)).when(sslDriver).write(any(FlushOperation.class));

        // Call will put bytes in buffer to flush
        context.flushChannel();
        assertTrue(context.readyForFlush());
    }

    public void testFirstFlushMustFinishForWriteToContinue() throws Exception {
        context.queueWriteOperation(mock(FlushReadyWrite.class));
        context.queueWriteOperation(mock(FlushReadyWrite.class));
        when(sslDriver.readyForApplicationData()).thenReturn(true);
        doAnswer(getWriteAnswer(1, false)).when(sslDriver).write(any(FlushOperation.class));

        // First call will put bytes in buffer to flush
        context.flushChannel();
        assertTrue(context.readyForFlush());
        // Second call will will not continue generating non-app bytes because they still need to be flushed
        context.flushChannel();
        assertTrue(context.readyForFlush());

        verify(sslDriver, times(1)).write(any(FlushOperation.class));
    }

    public void testQueuedWriteIsFlushedInFlushCall() throws Exception {
        ByteBuffer[] buffers = { ByteBuffer.allocate(10) };
        FlushReadyWrite flushOperation = new FlushReadyWrite(context, buffers, listener);
        context.queueWriteOperation(flushOperation);

        when(sslDriver.readyForApplicationData()).thenReturn(true);
        doAnswer(getWriteAnswer(10, true)).when(sslDriver).write(eq(flushOperation));

        when(rawChannel.write(same(selector.getIoBuffer()))).thenReturn(10);
        context.flushChannel();

        verify(rawChannel, times(1)).write(same(selector.getIoBuffer()));
        verify(selector).executeListener(listener, null);
        assertFalse(context.readyForFlush());
    }

    public void testPartialFlush() throws IOException {
        ByteBuffer[] buffers = { ByteBuffer.allocate(5) };
        FlushReadyWrite flushOperation = new FlushReadyWrite(context, buffers, listener);
        context.queueWriteOperation(flushOperation);

        when(sslDriver.readyForApplicationData()).thenReturn(true);
        doAnswer(getWriteAnswer(5, true)).when(sslDriver).write(eq(flushOperation));
        when(rawChannel.write(same(selector.getIoBuffer()))).thenReturn(4);
        context.flushChannel();

        verify(rawChannel, times(1)).write(same(selector.getIoBuffer()));
        verify(selector, times(0)).executeListener(listener, null);
        assertTrue(context.readyForFlush());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleWritesPartialFlushes() throws IOException {
        BiConsumer<Void, Exception> listener2 = mock(BiConsumer.class);
        ByteBuffer[] buffers1 = { ByteBuffer.allocate(10) };
        ByteBuffer[] buffers2 = { ByteBuffer.allocate(5) };
        FlushReadyWrite flushOperation1 = new FlushReadyWrite(context, buffers1, listener);
        FlushReadyWrite flushOperation2 = new FlushReadyWrite(context, buffers2, listener2);
        context.queueWriteOperation(flushOperation1);
        context.queueWriteOperation(flushOperation2);

        when(sslDriver.readyForApplicationData()).thenReturn(true);
        doAnswer(getWriteAnswer(5, true)).when(sslDriver).write(any(FlushOperation.class));
        when(rawChannel.write(same(selector.getIoBuffer()))).thenReturn(5, 5, 2);
        context.flushChannel();

        verify(rawChannel, times(3)).write(same(selector.getIoBuffer()));
        verify(selector).executeListener(listener, null);
        verify(selector, times(0)).executeListener(listener2, null);
        assertTrue(context.readyForFlush());
    }

    public void testWhenIOExceptionThrownListenerIsCalled() throws IOException {
        ByteBuffer[] buffers = { ByteBuffer.allocate(5) };
        FlushReadyWrite flushOperation = new FlushReadyWrite(context, buffers, listener);
        context.queueWriteOperation(flushOperation);

        IOException exception = new IOException();
        when(sslDriver.readyForApplicationData()).thenReturn(true);
        doAnswer(getWriteAnswer(5, true)).when(sslDriver).write(eq(flushOperation));
        when(rawChannel.write(any(ByteBuffer.class))).thenThrow(exception);
        expectThrows(IOException.class, () -> context.flushChannel());

        verify(selector).executeFailedListener(listener, exception);
        assertFalse(context.readyForFlush());
    }

    public void testWriteIOExceptionMeansChannelReadyToClose() throws Exception {
        context.queueWriteOperation(mock(FlushReadyWrite.class));
        when(sslDriver.readyForApplicationData()).thenReturn(true);
        doAnswer(getWriteAnswer(1, false)).when(sslDriver).write(any(FlushOperation.class));

        context.flushChannel();

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
        verify(selector).queueWrite(captor.capture());

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

            context = new SSLChannelContext(channel, selector, socketConfig, exceptionHandler, sslDriver, readWriteHandler, channelBuffer);
            context.setSelectionKey(mock(SelectionKey.class));

            context.closeChannel();
            ArgumentCaptor<WriteOperation> captor = ArgumentCaptor.forClass(WriteOperation.class);
            verify(selector).queueWrite(captor.capture());
            ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
            Runnable cancellable = mock(Runnable.class);
            when(nioTimer.scheduleAtRelativeTime(taskCaptor.capture(), anyLong())).thenReturn(cancellable);
            context.queueWriteOperation(captor.getValue());

            when(channel.isOpen()).thenReturn(true);
            context.closeFromSelector();
            verify(cancellable).run();
        }
    }

    public void testInitiateCloseSchedulesCloseNotify() throws SSLException {
        context.closeChannel();

        ArgumentCaptor<WriteOperation> captor = ArgumentCaptor.forClass(WriteOperation.class);
        verify(selector).queueWrite(captor.capture());

        context.queueWriteOperation(captor.getValue());
        verify(sslDriver).initiateClose();
    }

    public void testInitiateUnregisteredScheduledDirectClose() throws SSLException {
        context.setSelectionKey(null);
        context.closeChannel();

        verify(selector).queueChannelClose(channel);
    }

    @SuppressWarnings("unchecked")
    public void testActiveInitiatesDriver() throws IOException {
        try (Selector realSelector = Selector.open(); SocketChannel realSocket = SocketChannel.open()) {
            realSocket.configureBlocking(false);
            when(selector.rawSelector()).thenReturn(realSelector);
            when(channel.getRawChannel()).thenReturn(realSocket);
            TestReadWriteHandler readWriteHandler = new TestReadWriteHandler(readConsumer);
            context = new SSLChannelContext(channel, selector, socketConfig, exceptionHandler, sslDriver, readWriteHandler, channelBuffer);
            context.channelActive();
            verify(sslDriver).init();
        }
    }

    private Answer<Integer> getWriteAnswer(int bytesToEncrypt, boolean isApp) {
        return invocationOnMock -> {
            ByteBuffer byteBuffer = outboundBuffer.nextWriteBuffer(bytesToEncrypt + 1);
            for (int i = 0; i < bytesToEncrypt; ++i) {
                byteBuffer.put((byte) i);
            }
            outboundBuffer.incrementEncryptedBytes(bytesToEncrypt);
            if (isApp) {
                ((FlushOperation) invocationOnMock.getArguments()[0]).incrementIndex(bytesToEncrypt);
            }
            return bytesToEncrypt;
        };
    }

    private Answer<Integer> getReadAnswerForBytes(byte[] bytes) {
        return invocationOnMock -> {
            InboundChannelBuffer appBuffer = (InboundChannelBuffer) invocationOnMock.getArguments()[1];
            appBuffer.ensureCapacity(appBuffer.getIndex() + bytes.length);
            ByteBuffer[] buffers = appBuffer.sliceBuffersFrom(appBuffer.getIndex());
            assert buffers[0].remaining() > bytes.length;
            buffers[0].put(bytes);
            appBuffer.incrementIndex(bytes.length);
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
