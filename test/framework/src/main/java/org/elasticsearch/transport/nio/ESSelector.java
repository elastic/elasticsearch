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

import org.elasticsearch.common.util.BigArrays;
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

public abstract class ESSelector implements Closeable {

    protected final Selector selector;
    protected final AtomicBoolean isClosed = new AtomicBoolean(false);
    protected final ConcurrentLinkedQueue<NioChannel> channelsToClose = new ConcurrentLinkedQueue<>();
    protected final Set<NioChannel> registeredChannels = Collections.newSetFromMap(new ConcurrentHashMap<NioChannel, Boolean>());

    private final EventHandler eventHandler;
    private final BigArrays bigArrays;
    private final ReentrantLock runLock = new ReentrantLock();
    private volatile Thread thread;

    protected ESSelector(EventHandler eventHandler, BigArrays bigArrays) throws IOException {
        this(eventHandler, bigArrays, Selector.open());
    }

    protected ESSelector(EventHandler eventHandler, BigArrays bigArrays, Selector selector) throws IOException {
        this.eventHandler = eventHandler;
        this.bigArrays = bigArrays;
        this.selector = selector;
    }

    public void runLoop() {
        if (runLock.tryLock()) {
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

    public abstract void doSelect(int timeout) throws IOException, ClosedSelectorException;

    protected void setThread() {
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
        close(true);
    }

    public void close(boolean shouldInterrupt) throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            selector.close();
            if (shouldInterrupt && thread != null) {
                thread.interrupt();
            }
            runLock.lock(); // wait for the shutdown to complete
        }
    }

    public void queueChannelClose(NioChannel channel) {
        channelsToClose.offer(channel);
        wakeup();
    }

    void closePendingChannels() {
        NioChannel channel;
        while ((channel = channelsToClose.poll()) != null) {
            closeChannel(channel);
        }
    }

    protected abstract void cleanup();

    public BigArrays getBigArrays() {
        return bigArrays;
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

    private void closeChannel(NioChannel channel) {
        try {
            eventHandler.handleClose(channel);
        } catch (IOException e) {
            eventHandler.closeException(channel, e);
        } finally {
            registeredChannels.remove(channel);
        }
    }
}
