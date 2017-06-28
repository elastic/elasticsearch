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

package org.elasticsearch.transport.nio;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.transport.nio.channel.NioChannel;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.Selector;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is a basic selector abstraction used by {@link org.elasticsearch.transport.nio.NioTransport}. This
 * selector wraps a raw nio {@link Selector}. When you call {@link #runLoop()}, the selector will run until
 * {@link #close()} is called. This instance handles closing of channels. Users should call
 * {@link #queueChannelClose(NioChannel)} to schedule a channel for close by this selector.
 * <p>
 * Children of this class should implement the specific {@link #doSelect(int)} and {@link #cleanup()}
 * functionality.
 */
public abstract class ESSelector implements Closeable {

    final Selector selector;
    final ConcurrentLinkedQueue<NioChannel> channelsToClose = new ConcurrentLinkedQueue<>();
    final Set<NioChannel> registeredChannels = Collections.newSetFromMap(new ConcurrentHashMap<NioChannel, Boolean>());

    private final EventHandler eventHandler;
    private final ReentrantLock runLock = new ReentrantLock();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final PlainActionFuture<Boolean> isRunningFuture = PlainActionFuture.newFuture();
    private volatile Thread thread;

    ESSelector(EventHandler eventHandler) throws IOException {
        this(eventHandler, Selector.open());
    }

    ESSelector(EventHandler eventHandler, Selector selector) throws IOException {
        this.eventHandler = eventHandler;
        this.selector = selector;
    }

    /**
     * Starts this selector. The selector will run until {@link #close()} or {@link #close(boolean)} is
     * called.
     */
    public void runLoop() {
        if (runLock.tryLock()) {
            isRunningFuture.onResponse(true);
            try {
                setThread();
                while (isOpen()) {
                    singleLoop();
                }
            } finally {
                try {
                    cleanup();
                } finally {
                    runLock.unlock();
                }
            }
        } else {
            throw new IllegalStateException("selector is already running");
        }
    }

    void singleLoop() {
        try {
            closePendingChannels();
            doSelect(300);
        } catch (ClosedSelectorException e) {
            if (isOpen()) {
                throw e;
            }
        } catch (IOException e) {
            eventHandler.selectException(e);
        } catch (Exception e) {
            eventHandler.uncaughtException(e);
        }
    }

    /**
     * Should implement the specific select logic. This will be called once per {@link #singleLoop()}
     *
     * @param timeout to pass to the raw select operation
     * @throws IOException             thrown by the raw select operation
     * @throws ClosedSelectorException thrown if the raw selector is closed
     */
    abstract void doSelect(int timeout) throws IOException, ClosedSelectorException;

    void setThread() {
        thread = Thread.currentThread();
    }

    public boolean isOnCurrentThread() {
        return Thread.currentThread() == thread;
    }

    public void wakeup() {
        // TODO: Do I need the wakeup optimizations that some other libraries use?
        selector.wakeup();
    }

    public Set<NioChannel> getRegisteredChannels() {
        return registeredChannels;
    }

    @Override
    public void close() throws IOException {
        close(false);
    }

    public void close(boolean shouldInterrupt) throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            selector.close();
            if (shouldInterrupt && thread != null) {
                thread.interrupt();
            } else {
                wakeup();
            }
            runLock.lock(); // wait for the shutdown to complete
        }
    }

    public void queueChannelClose(NioChannel channel) {
        ensureOpen();
        channelsToClose.offer(channel);
        wakeup();
    }

    void closePendingChannels() {
        NioChannel channel;
        while ((channel = channelsToClose.poll()) != null) {
            closeChannel(channel);
        }
    }


    /**
     * Called once as the selector is being closed.
     */
    abstract void cleanup();

    public Selector rawSelector() {
        return selector;
    }

    public boolean isOpen() {
        return isClosed.get() == false;
    }

    public boolean isRunning() {
        return runLock.isLocked();
    }

    public PlainActionFuture<Boolean> isRunningFuture() {
        return isRunningFuture;
    }

    private void closeChannel(NioChannel channel) {
        try {
            eventHandler.handleClose(channel);
        } finally {
            registeredChannels.remove(channel);
        }
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("selector is already closed");
        }
    }
}
