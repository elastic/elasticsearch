/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NioSelectorTests extends ESTestCase {

    private NioSelector selector;
    private EventHandler eventHandler;
    private NioSocketChannel channel;
    private NioServerSocketChannel serverChannel;
    private TestSelectionKey selectionKey;
    private SocketChannelContext channelContext;
    private ServerChannelContext serverChannelContext;
    private BiConsumer<Void, Exception> listener;
    private ByteBuffer[] buffers = {ByteBuffer.allocate(1)};
    private Selector rawSelector;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        rawSelector = mock(Selector.class);
        eventHandler = mock(EventHandler.class);
        channel = mock(NioSocketChannel.class);
        channelContext = mock(SocketChannelContext.class);
        serverChannel = mock(NioServerSocketChannel.class);
        serverChannelContext = mock(ServerChannelContext.class);
        listener = mock(BiConsumer.class);
        selectionKey = new TestSelectionKey(0);

        this.selector = new NioSelector(eventHandler, rawSelector);
        this.selector.setThread();

        when(channel.getContext()).thenReturn(channelContext);
        when(channelContext.isOpen()).thenReturn(true);
        when(channelContext.getSelector()).thenReturn(selector);
        when(channelContext.getSelectionKey()).thenReturn(selectionKey);
        when(channelContext.isConnectComplete()).thenReturn(true);

        when(serverChannel.getContext()).thenReturn(serverChannelContext);
        when(serverChannelContext.isOpen()).thenReturn(true);
        when(serverChannelContext.getSelector()).thenReturn(selector);
        when(serverChannelContext.getSelectionKey()).thenReturn(selectionKey);
        doAnswer(invocationOnMock -> {
            ((Runnable) invocationOnMock.getArguments()[0]).run();
            return null;
        }).when(eventHandler).handleTask(any());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testQueueChannelForClosed() throws IOException {
        NioChannel channel = mock(NioChannel.class);
        ChannelContext context = mock(ChannelContext.class);
        when(channel.getContext()).thenReturn(context);
        when(context.getSelector()).thenReturn(selector);

        selector.queueChannelClose(channel);

        selector.singleLoop();

        verify(eventHandler).handleClose(context);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testCloseException() throws IOException, InterruptedException {
        IOException ioException = new IOException();
        NioChannel channel = mock(NioChannel.class);
        ChannelContext context = mock(ChannelContext.class);
        when(channel.getContext()).thenReturn(context);
        when(context.getSelector()).thenReturn(selector);

        executeOnNewThread(() -> selector.queueChannelClose(channel));

        doThrow(ioException).when(eventHandler).handleClose(context);

        selector.singleLoop();

        verify(eventHandler).closeException(context, ioException);
    }

    public void testNioDelayedTasksAreExecuted() throws IOException {
        AtomicBoolean isRun = new AtomicBoolean(false);
        long nanoTime = System.nanoTime() - 1;
        selector.getTaskScheduler().scheduleAtRelativeTime(() -> isRun.set(true), nanoTime);

        assertFalse(isRun.get());
        selector.singleLoop();
        verify(rawSelector).selectNow();
        assertTrue(isRun.get());
    }

    public void testTaskExceptionsAreHandled() {
        RuntimeException taskException = new RuntimeException();
        long nanoTime = System.nanoTime() - 1;
        Runnable task = () -> {
            throw taskException;
        };
        selector.getTaskScheduler().scheduleAtRelativeTime(task, nanoTime);

        doAnswer((a) -> {
            task.run();
            return null;
        }).when(eventHandler).handleTask(same(task));

        selector.singleLoop();
        verify(eventHandler).taskException(taskException);
    }

    public void testDefaultSelectorTimeoutIsUsedIfNoTaskSooner() throws IOException {
        long delay = new TimeValue(15, TimeUnit.MINUTES).nanos();
        selector.getTaskScheduler().scheduleAtRelativeTime(() -> {
        }, System.nanoTime() + delay);

        selector.singleLoop();
        verify(rawSelector).select(300);
    }

    public void testSelectorTimeoutWillBeReducedIfTaskSooner() throws Exception {
        // As this is a timing based test, we must assertBusy in the very small chance that the loop is
        // delayed for 50 milliseconds (causing a selectNow())
        assertBusy(() -> {
            ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
            long delay = new TimeValue(50, TimeUnit.MILLISECONDS).nanos();
            selector.getTaskScheduler().scheduleAtRelativeTime(() -> {
            }, System.nanoTime() + delay);
            selector.singleLoop();
            verify(rawSelector).select(captor.capture());
            assertTrue(captor.getValue() > 0);
            assertTrue(captor.getValue() < 300);
        });
    }

    public void testSelectorClosedExceptionIsNotCaughtWhileRunning() throws IOException {
        boolean closedSelectorExceptionCaught = false;
        when(rawSelector.select(anyInt())).thenThrow(new ClosedSelectorException());
        try {
            this.selector.singleLoop();
        } catch (ClosedSelectorException e) {
            closedSelectorExceptionCaught = true;
        }

        assertTrue(closedSelectorExceptionCaught);
    }

    public void testIOExceptionWhileSelect() throws IOException {
        IOException ioException = new IOException();

        when(rawSelector.select(anyInt())).thenThrow(ioException);

        this.selector.singleLoop();

        verify(eventHandler).selectorException(ioException);
    }

    public void testSelectorClosedIfOpenAndEventLoopNotRunning() throws Exception {
        when(rawSelector.isOpen()).thenReturn(true);
        executeOnNewThread(() -> selector.close());

        verify(rawSelector).close();
    }

    public void testRegisteredChannel() throws IOException {
        selector.scheduleForRegistration(serverChannel);

        selector.preSelect();

        verify(eventHandler).handleRegistration(serverChannelContext);
        verify(eventHandler).handleActive(serverChannelContext);
    }

    public void testClosedServerChannelWillNotBeRegistered() {
        when(serverChannelContext.isOpen()).thenReturn(false);
        selector.scheduleForRegistration(serverChannel);

        selector.preSelect();

        verify(eventHandler).registrationException(same(serverChannelContext), any(ClosedChannelException.class));
    }

    public void testRegisterServerChannelFailsDueToException() throws Exception {
        executeOnNewThread(() -> selector.scheduleForRegistration(serverChannel));
        ClosedChannelException closedChannelException = new ClosedChannelException();
        doThrow(closedChannelException).when(eventHandler).handleRegistration(serverChannelContext);

        selector.preSelect();

        verify(eventHandler, times(0)).handleActive(serverChannelContext);
        verify(eventHandler).registrationException(serverChannelContext, closedChannelException);
        verify(eventHandler).handleClose(serverChannelContext);
    }

    public void testChannelActiveException() throws Exception {
        executeOnNewThread(() -> selector.scheduleForRegistration(serverChannel));
        IOException ioException = new IOException();
        doThrow(ioException).when(eventHandler).handleActive(serverChannelContext);

        selector.preSelect();

        verify(eventHandler).handleActive(serverChannelContext);
        verify(eventHandler).activeException(serverChannelContext, ioException);
    }

    public void testClosedSocketChannelWillNotBeRegistered() throws Exception {
        when(channelContext.isOpen()).thenReturn(false);
        selector.scheduleForRegistration(channel);

        selector.preSelect();

        verify(eventHandler).registrationException(same(channelContext), any(ClosedChannelException.class));
        verify(eventHandler, times(0)).handleConnect(channelContext);
        verify(eventHandler).handleClose(channelContext);
    }

    public void testRegisterSocketChannelFailsDueToException() throws InterruptedException {
        executeOnNewThread(() -> {
            selector.scheduleForRegistration(channel);

            ClosedChannelException closedChannelException = new ClosedChannelException();
            doThrow(closedChannelException).when(eventHandler).handleRegistration(channelContext);

            selector.preSelect();

            verify(eventHandler).registrationException(channelContext, closedChannelException);
            verify(eventHandler, times(0)).handleActive(serverChannelContext);
            verify(eventHandler, times(0)).handleConnect(channelContext);
            verify(eventHandler).handleClose(channelContext);
        });
    }

    public void testAcceptEvent() throws IOException {
        selectionKey.setReadyOps(SelectionKey.OP_ACCEPT);

        selectionKey.attach(serverChannelContext);
        selector.processKey(selectionKey);

        verify(eventHandler).acceptChannel(serverChannelContext);
    }

    public void testAcceptException() throws IOException {
        selectionKey.setReadyOps(SelectionKey.OP_ACCEPT);
        IOException ioException = new IOException();

        doThrow(ioException).when(eventHandler).acceptChannel(serverChannelContext);

        selectionKey.attach(serverChannelContext);
        selector.processKey(selectionKey);

        verify(eventHandler).acceptException(serverChannelContext, ioException);
    }

    public void testRegisterChannel() throws Exception {
        selector.scheduleForRegistration(channel);

        selector.preSelect();

        verify(eventHandler).handleRegistration(channelContext);
    }

    public void testSuccessfullyRegisterChannelWillAttemptConnect() throws Exception {
        selector.scheduleForRegistration(channel);

        selector.preSelect();

        verify(eventHandler).handleConnect(channelContext);
    }

    public void testQueueWriteWhenNotRunning() throws Exception {
        executeOnNewThread(() -> {
            selector.close();
            selector.queueWrite(new FlushReadyWrite(channelContext, buffers, listener));
        });
        verify(listener).accept(isNull(Void.class), any(ClosedSelectorException.class));
    }

    public void testQueueWriteChannelIsClosed() throws Exception {
        WriteOperation writeOperation = new FlushReadyWrite(channelContext, buffers, listener);

        executeOnNewThread(() -> selector.queueWrite(writeOperation));
        when(channelContext.isOpen()).thenReturn(false);
        selector.preSelect();

        verify(channelContext, times(0)).queueWriteOperation(writeOperation);
        verify(listener).accept(isNull(Void.class), any(ClosedChannelException.class));
    }

    public void testQueueWriteChannelIsUnregistered() throws Exception {
        WriteOperation writeOperation = new FlushReadyWrite(channelContext, buffers, listener);

        executeOnNewThread(() -> selector.queueWrite(writeOperation));
        when(channelContext.getSelectionKey()).thenReturn(null);
        selector.preSelect();

        verify(channelContext, times(0)).queueWriteOperation(writeOperation);
        verify(listener).accept(isNull(Void.class), any(IllegalStateException.class));
    }

    public void testQueueWriteSuccessful() throws Exception {
        WriteOperation writeOperation = new FlushReadyWrite(channelContext, buffers, listener);
        executeOnNewThread(() -> selector.queueWrite(writeOperation));

        assertTrue((selectionKey.interestOps() & SelectionKey.OP_WRITE) == 0);

        selector.preSelect();

        verify(channelContext).queueWriteOperation(writeOperation);
    }

    public void testQueueDirectlyInChannelBufferSuccessful() throws Exception {
        WriteOperation writeOperation = new FlushReadyWrite(channelContext, buffers, listener);

        when(channelContext.readyForFlush()).thenReturn(true);
        selector.queueWrite(writeOperation);

        verify(channelContext).queueWriteOperation(writeOperation);
        verify(eventHandler, times(0)).handleWrite(channelContext);
        verify(eventHandler, times(0)).postHandling(channelContext);
    }

    public void testShouldFlushIfNoPendingFlushes() throws Exception {
        WriteOperation writeOperation = new FlushReadyWrite(channelContext, buffers, listener);

        when(channelContext.readyForFlush()).thenReturn(false);
        selector.queueWrite(writeOperation);

        verify(channelContext).queueWriteOperation(writeOperation);
        verify(eventHandler).handleWrite(channelContext);
        verify(eventHandler).postHandling(channelContext);
    }

    public void testShouldNotFlushIfChannelNotConnectedPendingFlushes() throws Exception {
        WriteOperation writeOperation = new FlushReadyWrite(channelContext, buffers, listener);

        when(channelContext.readyForFlush()).thenReturn(false);
        when(channelContext.isConnectComplete()).thenReturn(false);
        selector.queueWrite(writeOperation);

        verify(channelContext).queueWriteOperation(writeOperation);
        verify(eventHandler, times(0)).handleWrite(channelContext);
        verify(eventHandler).postHandling(channelContext);
    }

    public void testConnectEvent() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_CONNECT);

        selectionKey.attach(channelContext);
        selector.processKey(selectionKey);

        verify(eventHandler).handleConnect(channelContext);
    }

    public void testConnectEventFinishThrowException() throws Exception {
        IOException ioException = new IOException();

        selectionKey.setReadyOps(SelectionKey.OP_CONNECT);

        doThrow(ioException).when(eventHandler).handleConnect(channelContext);
        selectionKey.attach(channelContext);
        selector.processKey(selectionKey);

        verify(eventHandler).connectException(channelContext, ioException);
    }

    public void testWillNotConsiderWriteOrReadUntilConnectionComplete() throws Exception {
        IOException ioException = new IOException();

        selectionKey.setReadyOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);

        doThrow(ioException).when(eventHandler).handleWrite(channelContext);

        when(channelContext.isConnectComplete()).thenReturn(false);
        selectionKey.attach(channelContext);
        selector.processKey(selectionKey);

        verify(eventHandler, times(0)).handleWrite(channelContext);
        verify(eventHandler, times(0)).handleRead(channelContext);
    }

    public void testSuccessfulWriteEvent() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_WRITE);

        selectionKey.attach(channelContext);
        selector.processKey(selectionKey);

        verify(eventHandler).handleWrite(channelContext);
    }

    public void testWriteEventWithException() throws Exception {
        IOException ioException = new IOException();

        selectionKey.attach(channelContext);
        selectionKey.setReadyOps(SelectionKey.OP_WRITE);

        doThrow(ioException).when(eventHandler).handleWrite(channelContext);

        selectionKey.attach(channelContext);
        selector.processKey(selectionKey);

        verify(eventHandler).writeException(channelContext, ioException);
    }

    public void testSuccessfulReadEvent() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_READ);

        selectionKey.attach(channelContext);
        selector.processKey(selectionKey);

        verify(eventHandler).handleRead(channelContext);
    }

    public void testReadEventWithException() throws Exception {
        IOException ioException = new IOException();

        selectionKey.setReadyOps(SelectionKey.OP_READ);

        doThrow(ioException).when(eventHandler).handleRead(channelContext);

        selectionKey.attach(channelContext);
        selector.processKey(selectionKey);

        verify(eventHandler).readException(channelContext, ioException);
    }

    public void testWillCallPostHandleAfterChannelHandling() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);

        selectionKey.attach(channelContext);
        selector.processKey(selectionKey);

        verify(eventHandler).handleWrite(channelContext);
        verify(eventHandler).handleRead(channelContext);
        verify(eventHandler).postHandling(channelContext);
    }

    public void testCleanup() throws Exception {
        NioSocketChannel unregisteredChannel = mock(NioSocketChannel.class);
        SocketChannelContext unregisteredContext = mock(SocketChannelContext.class);
        when(unregisteredContext.getSelector()).thenReturn(selector);
        when(unregisteredChannel.getContext()).thenReturn(unregisteredContext);

        executeOnNewThread(() -> selector.scheduleForRegistration(channel));

        selector.preSelect();

        executeOnNewThread(() -> {
            selector.queueWrite(new FlushReadyWrite(channelContext, buffers, listener));
            selector.scheduleForRegistration(unregisteredChannel);
        });

        TestSelectionKey testSelectionKey = new TestSelectionKey(0);
        testSelectionKey.attach(channelContext);
        when(rawSelector.keys()).thenReturn(new HashSet<>(Collections.singletonList(testSelectionKey)));

        selector.cleanupAndCloseChannels();

        verify(listener).accept(isNull(Void.class), any(ClosedSelectorException.class));
        verify(eventHandler).handleClose(channelContext);
        verify(eventHandler).handleClose(unregisteredContext);
    }

    private static void executeOnNewThread(CheckedRunnable<Exception> runnable) throws InterruptedException {
        final Thread thread = new Thread(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                runnable.run();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        thread.start();
        thread.join();
    }
}
