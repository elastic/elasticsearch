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

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;

/**
 * Selector implementation that handles {@link NioSocketChannel}. It's main piece of functionality is
 * handling connect, read, and write events.
 */
public class SocketSelector extends ESSelector {

    private final ConcurrentLinkedQueue<SocketChannelContext> newChannels = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<WriteOperation> queuedWrites = new ConcurrentLinkedQueue<>();
    private final SocketEventHandler eventHandler;

    public SocketSelector(SocketEventHandler eventHandler) throws IOException {
        super(eventHandler);
        this.eventHandler = eventHandler;
    }

    public SocketSelector(SocketEventHandler eventHandler, Selector selector) throws IOException {
        super(eventHandler, selector);
        this.eventHandler = eventHandler;
    }

    @Override
    void processKey(SelectionKey selectionKey) {
        SocketChannelContext channelContext = (SocketChannelContext) selectionKey.attachment();
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

    @Override
    void preSelect() {
        setUpNewChannels();
        handleQueuedWrites();
    }

    @Override
    void cleanup() {
        WriteOperation op;
        while ((op = queuedWrites.poll()) != null) {
            executeFailedListener(op.getListener(), new ClosedSelectorException());
        }
        channelsToClose.addAll(newChannels);
    }

    /**
     * Schedules a NioSocketChannel to be registered by this selector. The channel will by queued and eventually
     * registered next time through the event loop.
     * @param nioSocketChannel the channel to register
     */
    public void scheduleForRegistration(NioSocketChannel nioSocketChannel) {
        SocketChannelContext channelContext = nioSocketChannel.getContext();
        newChannels.offer(channelContext);
        ensureSelectorOpenForEnqueuing(newChannels, channelContext);
        wakeup();
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

    /**
     * Queues a write operation directly in a channel's buffer. Channel buffers are only safe to be accessed
     * by the selector thread. As a result, this method should only be called by the selector thread.
     *
     * @param writeOperation to be queued in a channel's buffer
     */
    public void queueWriteInChannelBuffer(WriteOperation writeOperation) {
        assertOnSelectorThread();
        SocketChannelContext context = writeOperation.getChannel();
        try {
            SelectionKeyUtils.setWriteInterested(context.getSelectionKey());
            context.queueWriteOperation(writeOperation);
        } catch (Exception e) {
            executeFailedListener(writeOperation.getListener(), e);
        }
    }

    /**
     * Executes a success listener with consistent exception handling. This can only be called from current
     * selector thread.
     *
     * @param listener to be executed
     * @param value to provide to listener
     */
    public <V> void executeListener(BiConsumer<V, Exception> listener, V value) {
        assertOnSelectorThread();
        try {
            listener.accept(value, null);
        } catch (Exception e) {
            eventHandler.listenerException(e);
        }
    }

    /**
     * Executes a failed listener with consistent exception handling. This can only be called from current
     * selector thread.
     *
     * @param listener to be executed
     * @param exception to provide to listener
     */
    public <V> void executeFailedListener(BiConsumer<V, Exception> listener, Exception exception) {
        assertOnSelectorThread();
        try {
            listener.accept(null, exception);
        } catch (Exception e) {
            eventHandler.listenerException(e);
        }
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

    private void handleQueuedWrites() {
        WriteOperation writeOperation;
        while ((writeOperation = queuedWrites.poll()) != null) {
            if (writeOperation.getChannel().isOpen()) {
                queueWriteInChannelBuffer(writeOperation);
            } else {
                executeFailedListener(writeOperation.getListener(), new ClosedChannelException());
            }
        }
    }

    private void setUpNewChannels() {
        SocketChannelContext channelContext;
        while ((channelContext = this.newChannels.poll()) != null) {
            setupChannel(channelContext);
        }
    }

    private void setupChannel(SocketChannelContext context) {
        assert context.getSelector() == this : "The channel must be registered with the selector with which it was created";
        try {
            if (context.isOpen()) {
                eventHandler.handleRegistration(context);
                attemptConnect(context, false);
            } else {
                eventHandler.registrationException(context, new ClosedChannelException());
            }
        } catch (Exception e) {
            eventHandler.registrationException(context, e);
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
}
