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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * This is a nio selector implementation. This selector wraps a raw nio {@link Selector}. When you call
 * {@link #runLoop()}, the selector will run until {@link #close()} is called. This instance handles closing
 * of channels. Users should call {@link #queueChannelClose(NioChannel)} to schedule a channel for close by
 * this selector.
 */
public class NioSelector implements Closeable {

    private final ConcurrentLinkedQueue<WriteOperation> queuedWrites = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<ChannelContext<?>> channelsToClose = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<ChannelContext<?>> channelsToRegister = new ConcurrentLinkedQueue<>();
    private final EventHandler eventHandler;
    private final Selector selector;
    private final ByteBuffer ioBuffer;

    private final TaskScheduler taskScheduler = new TaskScheduler();
    private final ReentrantLock runLock = new ReentrantLock();
    private final CountDownLatch exitedLoop = new CountDownLatch(1);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final CompletableFuture<Void> isRunningFuture = new CompletableFuture<>();
    private final AtomicReference<Thread> thread = new AtomicReference<>(null);

    public NioSelector(EventHandler eventHandler) throws IOException {
        this(eventHandler, Selector.open());
    }

    public NioSelector(EventHandler eventHandler, Selector selector) {
        this.selector = selector;
        this.eventHandler = eventHandler;
        this.ioBuffer = ByteBuffer.allocateDirect(1 << 16);
    }

    /**
     * Returns a cached direct byte buffer for network operations. It is cleared on every get call.
     *
     * @return the byte buffer
     */
    public ByteBuffer getIoBuffer() {
        assertOnSelectorThread();
        ioBuffer.clear();
        return ioBuffer;
    }

    public TaskScheduler getTaskScheduler() {
        return taskScheduler;
    }

    public Selector rawSelector() {
        return selector;
    }

    public boolean isOpen() {
        return isClosed.get() == false;
    }

    public boolean isRunning() {
        return runLock.isLocked();
    }

    Future<Void> isRunningFuture() {
        return isRunningFuture;
    }

    void setThread() {
        boolean wasSet = thread.compareAndSet(null, Thread.currentThread());
        assert wasSet : "Failed to set thread as it was already set. Should only set once.";
    }

    public boolean isOnCurrentThread() {
        return Thread.currentThread() == thread.get();
    }

    public void assertOnSelectorThread() {
        assert isOnCurrentThread() : "Must be on selector thread [" + thread.get().getName() + "} to perform this operation. " +
            "Currently on thread [" + Thread.currentThread().getName() + "].";
    }

    /**
     * Starts this selector. The selector will run until {@link #close()} is called.
     */
    public void runLoop() {
        if (runLock.tryLock()) {
            isRunningFuture.complete(null);
            try {
                setThread();
                while (isOpen()) {
                    singleLoop();
                }
            } finally {
                try {
                    cleanupAndCloseChannels();
                } finally {
                    try {
                        selector.close();
                    } catch (IOException e) {
                        eventHandler.selectorException(e);
                    } finally {
                        runLock.unlock();
                        exitedLoop.countDown();
                    }
                }
            }
        } else {
            throw new IllegalStateException("selector is already running");
        }
    }

    void singleLoop() {
        try {
            closePendingChannels();
            preSelect();
            long nanosUntilNextTask = taskScheduler.nanosUntilNextTask(System.nanoTime());
            int ready;
            if (nanosUntilNextTask == 0) {
                ready = selector.selectNow();
            } else {
                long millisUntilNextTask = TimeUnit.NANOSECONDS.toMillis(nanosUntilNextTask);
                // Only select until the next task needs to be run. Do not select with a value of 0 because
                // that blocks without a timeout.
                ready = selector.select(Math.min(300, Math.max(millisUntilNextTask, 1)));
            }
            if (ready > 0) {
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectionKeys.iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey sk = keyIterator.next();
                    keyIterator.remove();
                    if (sk.isValid()) {
                        try {
                            processKey(sk);
                        } catch (CancelledKeyException cke) {
                            eventHandler.genericChannelException((ChannelContext<?>) sk.attachment(),  cke);
                        }
                    } else {
                        eventHandler.genericChannelException((ChannelContext<?>) sk.attachment(),  new CancelledKeyException());
                    }
                }
            }

            handleScheduledTasks(System.nanoTime());
        } catch (ClosedSelectorException e) {
            if (isOpen()) {
                throw e;
            }
        } catch (IOException e) {
            eventHandler.selectorException(e);
        } catch (Exception e) {
            eventHandler.uncaughtException(e);
        }
    }

    void cleanupAndCloseChannels() {
        cleanupPendingWrites();
        channelsToClose.addAll(channelsToRegister);
        channelsToRegister.clear();
        channelsToClose.addAll(selector.keys().stream().map(sk -> (ChannelContext<?>) sk.attachment()).collect(Collectors.toList()));
        closePendingChannels();
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            wakeup();
            if (isRunning()) {
                try {
                    exitedLoop.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Thread was interrupted while waiting for selector to close", e);
                }
            } else if (selector.isOpen()) {
                selector.close();
            }
        }
    }

    void processKey(SelectionKey selectionKey) {
        ChannelContext<?> context = (ChannelContext<?>) selectionKey.attachment();
        if (selectionKey.isAcceptable()) {
            assert context instanceof ServerChannelContext : "Only server channels can receive accept events";
            ServerChannelContext serverChannelContext = (ServerChannelContext) context;
            int ops = selectionKey.readyOps();
            if ((ops & SelectionKey.OP_ACCEPT) != 0) {
                try {
                    eventHandler.acceptChannel(serverChannelContext);
                } catch (IOException e) {
                    eventHandler.acceptException(serverChannelContext, e);
                }
            }
        } else {
            assert context instanceof SocketChannelContext : "Only sockets channels can receive non-accept events";
            SocketChannelContext channelContext = (SocketChannelContext) context;
            int ops = selectionKey.readyOps();
            if ((ops & SelectionKey.OP_CONNECT) != 0) {
                attemptConnect(channelContext, true);
            }

            if (channelContext.isConnectComplete()) {
                if ((ops & SelectionKey.OP_WRITE) != 0) {
                    handleWrite(channelContext);
                }

                if ((ops & SelectionKey.OP_READ) != 0) {
                    handleRead(channelContext);
                }
            }
            eventHandler.postHandling(channelContext);
        }

    }

    /**
     * Called immediately prior to a raw {@link Selector#select()} call. Should be used to implement
     * channel registration, handling queued writes, and other work that is not specifically processing
     * a selection key.
     */
    void preSelect() {
        setUpNewChannels();
        handleQueuedWrites();
    }

    private void handleScheduledTasks(long nanoTime) {
        Runnable task;
        while ((task = taskScheduler.pollTask(nanoTime)) != null) {
            try {
                task.run();
            } catch (Exception e) {
                eventHandler.taskException(e);
            }
        }
    }

    /**
     * Queues a write operation to be handled by the event loop. This can be called by any thread and is the
     * api available for non-selector threads to schedule writes.
     *
     * @param writeOperation to be queued
     */
    public void queueWrite(WriteOperation writeOperation) {
        queuedWrites.offer(writeOperation);
        if (isOpen() == false) {
            boolean wasRemoved = queuedWrites.remove(writeOperation);
            if (wasRemoved) {
                writeOperation.getListener().accept(null, new ClosedSelectorException());
            }
        } else {
            wakeup();
        }
    }

    public void queueChannelClose(NioChannel channel) {
        ChannelContext<?> context = channel.getContext();
        assert context.getSelector() == this : "Must schedule a channel for closure with its selector";
        channelsToClose.offer(context);
        if (isOnCurrentThread() == false) {
            ensureSelectorOpenForEnqueuing(channelsToClose, context);
            wakeup();
        }
    }

    /**
     * Schedules a NioChannel to be registered with this selector. The channel will by queued and
     * eventually registered next time through the event loop.
     *
     * @param channel to register
     */
    public void scheduleForRegistration(NioChannel channel) {
        ChannelContext<?> context = channel.getContext();
        channelsToRegister.add(context);
        ensureSelectorOpenForEnqueuing(channelsToRegister, context);
        wakeup();
    }

    /**
     * Queues a write operation directly in a channel's buffer. If this channel does not have pending writes
     * already, the channel will be flushed. Channel buffers are only safe to be accessed by the selector
     * thread. As a result, this method should only be called by the selector thread. If this channel does
     * not have pending writes already, the channel will be flushed.
     *
     * @param writeOperation to be queued in a channel's buffer
     */
    public void writeToChannel(WriteOperation writeOperation) {
        assertOnSelectorThread();
        SocketChannelContext context = writeOperation.getChannel();
        // If the channel does not currently have anything that is ready to flush, we should flush after
        // the write operation is queued.
        boolean shouldFlushAfterQueuing = context.readyForFlush() == false;
        try {
            SelectionKeyUtils.setWriteInterested(context.getSelectionKey());
            context.queueWriteOperation(writeOperation);
        } catch (Exception e) {
            shouldFlushAfterQueuing = false;
            executeFailedListener(writeOperation.getListener(), e);
        }

        if (shouldFlushAfterQueuing) {
            handleWrite(context);
            eventHandler.postHandling(context);
        }
    }

    /**
     * Executes a success listener with consistent exception handling. This can only be called from current
     * selector thread.
     *
     * @param listener to be executed
     * @param value    to provide to listener
     */
    public <V> void executeListener(BiConsumer<V, Exception> listener, V value) {
        assertOnSelectorThread();
        try {
            listener.accept(value, null);
        } catch (Exception e) {
            eventHandler.taskException(e);
        }
    }

    /**
     * Executes a failed listener with consistent exception handling. This can only be called from current
     * selector thread.
     *
     * @param listener  to be executed
     * @param exception to provide to listener
     */
    public <V> void executeFailedListener(BiConsumer<V, Exception> listener, Exception exception) {
        assertOnSelectorThread();
        try {
            listener.accept(null, exception);
        } catch (Exception e) {
            eventHandler.taskException(e);
        }
    }

    private void cleanupPendingWrites() {
        WriteOperation op;
        while ((op = queuedWrites.poll()) != null) {
            executeFailedListener(op.getListener(), new ClosedSelectorException());
        }
    }

    private void wakeup() {
        // TODO: Do we need the wakeup optimizations that some other libraries use?
        selector.wakeup();
    }

    private void handleWrite(SocketChannelContext context) {
        try {
            eventHandler.handleWrite(context);
        } catch (Exception e) {
            eventHandler.writeException(context, e);
        }
    }

    private void handleRead(SocketChannelContext context) {
        try {
            eventHandler.handleRead(context);
        } catch (Exception e) {
            eventHandler.readException(context, e);
        }
    }

    private void attemptConnect(SocketChannelContext context, boolean connectEvent) {
        try {
            eventHandler.handleConnect(context);
            if (connectEvent && context.isConnectComplete() == false) {
                eventHandler.connectException(context, new IOException("Received OP_CONNECT but connect failed"));
            }
        } catch (Exception e) {
            eventHandler.connectException(context, e);
        }
    }

    private void setUpNewChannels() {
        ChannelContext<?> newChannel;
        while ((newChannel = this.channelsToRegister.poll()) != null) {
            assert newChannel.getSelector() == this : "The channel must be registered with the selector with which it was created";
            try {
                if (newChannel.isOpen()) {
                    eventHandler.handleRegistration(newChannel);
                    if (newChannel instanceof SocketChannelContext) {
                        attemptConnect((SocketChannelContext) newChannel, false);
                    }
                } else {
                    eventHandler.registrationException(newChannel, new ClosedChannelException());
                }
            } catch (Exception e) {
                eventHandler.registrationException(newChannel, e);
            }
        }
    }

    private void closePendingChannels() {
        ChannelContext<?> channelContext;
        while ((channelContext = channelsToClose.poll()) != null) {
            eventHandler.handleClose(channelContext);
        }
    }

    private void handleQueuedWrites() {
        WriteOperation writeOperation;
        while ((writeOperation = queuedWrites.poll()) != null) {
            if (writeOperation.getChannel().isOpen()) {
                writeToChannel(writeOperation);
            } else {
                executeFailedListener(writeOperation.getListener(), new ClosedChannelException());
            }
        }
    }

    /**
     * This is a convenience method to be called after some object (normally channels) are enqueued with this
     * selector. This method will check if the selector is still open. If it is open, normal operation can
     * proceed.
     *
     * If the selector is closed, then we attempt to remove the object from the queue. If the removal
     * succeeds then we throw an {@link IllegalStateException} indicating that normal operation failed. If
     * the object cannot be removed from the queue, then the object has already been handled by the selector
     * and operation can proceed normally.
     *
     * If this method is called from the selector thread, we will not allow the queuing to occur as the
     * selector thread can manipulate its queues internally even if it is no longer open.
     *
     * @param queue the queue to which the object was added
     * @param objectAdded the objected added
     * @param <O> the object type
     */
    private <O> void ensureSelectorOpenForEnqueuing(ConcurrentLinkedQueue<O> queue, O objectAdded) {
        if (isOpen() == false && isOnCurrentThread() == false) {
            if (queue.remove(objectAdded)) {
                throw new IllegalStateException("selector is already closed");
            }
        }
    }
}
