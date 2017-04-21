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

import java.io.IOException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.Selector;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

public abstract class ESSelector {

    protected static final int NOT_STARTED = 0;
    protected static final int RUNNING = 1;
    protected static final int STOPPED = 2;

    protected Selector selector;
    protected volatile int state = 0;
    protected final ConcurrentLinkedQueue<NioChannel> channelsToClose = new ConcurrentLinkedQueue<>();
    protected final Set<NioChannel> registeredChannels = Collections.newSetFromMap(new ConcurrentHashMap<NioChannel, Boolean>());

    private final EventHandler eventHandler;
    private final BigArrays bigArrays;
    private volatile Thread thread;
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    protected ESSelector(EventHandler eventHandler, BigArrays bigArrays) throws IOException {
        this(eventHandler, bigArrays, Selector.open());
    }

    protected ESSelector(EventHandler eventHandler, BigArrays bigArrays, Selector selector) throws IOException {
        this.eventHandler = eventHandler;
        this.bigArrays = bigArrays;
        this.selector = selector;
    }

    public void runLoop() {
        setThread();
        state = RUNNING;
        try {
            while (state == RUNNING) {
                singleLoop();
            }
        } finally {
            try {
                cleanup();
            } finally {
                shutdownLatch.countDown();
            }
        }
    }

    void singleLoop() {
        try {
            closePendingChannels();
            doSelect(300);
        } catch (ClosedSelectorException e) {
            if (state == RUNNING) {
                throw e;
            }
        } catch (IOException e) {
            eventHandler.selectException(e);
        } catch (Exception e) {
            eventHandler.uncaughtException(e);
        }
    }

    public abstract void doSelect(int timeout) throws IOException, ClosedSelectorException;

    public void setThread() {
        thread = Thread.currentThread();
    }

    public boolean onThread() {
        return Thread.currentThread() == thread;
    }

    public void wakeup() {
        // TODO: Do I need the wakeup optimizations that some other libraries use?
        selector.wakeup();
    }

    public Set<NioChannel> getRegisteredChannels() {
        return registeredChannels;
    }

    public CountDownLatch close() throws IOException {
        return close(true);
    }

    public CountDownLatch close(boolean shouldInterrupt) throws IOException {
        int currentState = this.state;
        state = STOPPED;
        selector.close();
        if (shouldInterrupt && thread != null) {
            thread.interrupt();
        }
        if (currentState == NOT_STARTED) {
            shutdownLatch.countDown();
        }
        return shutdownLatch;
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

    private void closeChannel(NioChannel channel) {
        try {
            eventHandler.handleClose(channel);
        } catch (IOException e) {
            eventHandler.closeException(channel, e);
        } finally {
            registeredChannels.remove(channel);
        }
    }

    protected abstract void cleanup();

    public BigArrays getBigArrays() {
        return bigArrays;
    }

    public Selector rawSelector() {
        return selector;
    }

    public boolean isRunning() {
        return state == RUNNING;
    }
}
