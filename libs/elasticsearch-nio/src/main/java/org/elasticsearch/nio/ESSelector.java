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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * This is a basic selector abstraction. This selector wraps a raw nio {@link Selector}. When you call
 * {@link #runLoop()}, the selector will run until {@link #close()} is called. This instance handles closing
 * of channels. Users should call {@link #queueChannelClose(NioChannel)} to schedule a channel for close by
 * this selector.
 * <p>
 * Children of this class should implement the specific {@link #processKey(SelectionKey)},
 * {@link #preSelect()}, and {@link #cleanup()} functionality.
 */
public abstract class ESSelector implements Closeable {

    final Selector selector;
    final ConcurrentLinkedQueue<ChannelContext<?>> channelsToClose = new ConcurrentLinkedQueue<>();

    private final EventHandler eventHandler;
    private final ReentrantLock runLock = new ReentrantLock();
    private final CountDownLatch exitedLoop = new CountDownLatch(1);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final CompletableFuture<Void> isRunningFuture = new CompletableFuture<>();
    private volatile Thread thread;

    ESSelector(EventHandler eventHandler) throws IOException {
        this(eventHandler, Selector.open());
    }

    ESSelector(EventHandler eventHandler, Selector selector) {
        this.eventHandler = eventHandler;
        this.selector = selector;
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

            int ready = selector.select(300);
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
        cleanup();
        channelsToClose.addAll(selector.keys().stream().map(sk -> (ChannelContext<?>) sk.attachment()).collect(Collectors.toList()));
        closePendingChannels();
    }

    /**
     * Called by the base {@link ESSelector} class when there is a {@link SelectionKey} to be handled.
     *
     * @param selectionKey the key to be handled
     * @throws CancelledKeyException thrown when the key has already been cancelled
     */
    abstract void processKey(SelectionKey selectionKey) throws CancelledKeyException;

    /**
     * Called immediately prior to a raw {@link Selector#select()} call. Should be used to implement
     * channel registration, handling queued writes, and other work that is not specifically processing
     * a selection key.
     */
    abstract void preSelect();

    /**
     * Called once as the selector is being closed.
     */
    abstract void cleanup();

    void setThread() {
        thread = Thread.currentThread();
    }

    public boolean isOnCurrentThread() {
        return Thread.currentThread() == thread;
    }

    public void assertOnSelectorThread() {
        assert isOnCurrentThread() : "Must be on selector thread to perform this operation. Currently on thread ["
            + Thread.currentThread().getName() + "].";
    }

    void wakeup() {
        // TODO: Do we need the wakeup optimizations that some other libraries use?
        selector.wakeup();
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

    public void queueChannelClose(NioChannel channel) {
        ChannelContext<?> context = channel.getContext();
        assert context.getSelector() == this : "Must schedule a channel for closure with its selector";
        channelsToClose.offer(context);
        ensureSelectorOpenForEnqueuing(channelsToClose, context);
        wakeup();
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

    public Future<Void> isRunningFuture() {
        return isRunningFuture;
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
     * If this method is called from the selector thread, we will not throw an exception as the selector
     * thread can manipulate its queues internally even if it is no longer open.
     *
     * @param queue the queue to which the object was added
     * @param objectAdded the objected added
     * @param <O> the object type
     */
    <O> void ensureSelectorOpenForEnqueuing(ConcurrentLinkedQueue<O> queue, O objectAdded) {
        if (isOpen() == false && isOnCurrentThread() == false) {
            if (queue.remove(objectAdded)) {
                throw new IllegalStateException("selector is already closed");
            }
        }
    }

    private void closePendingChannels() {
        ChannelContext<?> channelContext;
        while ((channelContext = channelsToClose.poll()) != null) {
            eventHandler.handleClose(channelContext);
        }
    }
}
