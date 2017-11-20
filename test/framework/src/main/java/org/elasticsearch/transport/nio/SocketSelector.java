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

import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.SelectionKeyUtils;
import org.elasticsearch.transport.nio.channel.WriteContext;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Selector implementation that handles {@link NioSocketChannel}. It's main piece of functionality is
 * handling connect, read, and write events.
 */
public class SocketSelector extends ESSelector {

    private final ConcurrentLinkedQueue<NioSocketChannel> newChannels = new ConcurrentLinkedQueue<>();
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
        NioSocketChannel nioSocketChannel = (NioSocketChannel) selectionKey.attachment();
        int ops = selectionKey.readyOps();
        if ((ops & SelectionKey.OP_CONNECT) != 0) {
            attemptConnect(nioSocketChannel, true);
        }

        if (nioSocketChannel.isConnectComplete()) {
            if ((ops & SelectionKey.OP_WRITE) != 0) {
                handleWrite(nioSocketChannel);
            }

            if ((ops & SelectionKey.OP_READ) != 0) {
                handleRead(nioSocketChannel);
            }
        }
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
            op.getListener().onFailure(new ClosedSelectorException());
        }
        channelsToClose.addAll(newChannels);
    }

    /**
     * Schedules a NioSocketChannel to be registered by this selector. The channel will by queued and eventually
     * registered next time through the event loop.
     * @param nioSocketChannel the channel to register
     */
    public void scheduleForRegistration(NioSocketChannel nioSocketChannel) {
        newChannels.offer(nioSocketChannel);
        ensureSelectorOpenForEnqueuing(newChannels, nioSocketChannel);
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
                writeOperation.getListener().onFailure(new ClosedSelectorException());
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
        assert isOnCurrentThread() : "Must be on selector thread";
        NioSocketChannel channel = writeOperation.getChannel();
        WriteContext context = channel.getWriteContext();
        try {
            SelectionKeyUtils.setWriteInterested(channel);
            context.queueWriteOperations(writeOperation);
        } catch (Exception e) {
            writeOperation.getListener().onFailure(e);
        }
    }

    private void handleWrite(NioSocketChannel nioSocketChannel) {
        try {
            eventHandler.handleWrite(nioSocketChannel);
        } catch (Exception e) {
            eventHandler.writeException(nioSocketChannel, e);
        }
    }

    private void handleRead(NioSocketChannel nioSocketChannel) {
        try {
            eventHandler.handleRead(nioSocketChannel);
        } catch (Exception e) {
            eventHandler.readException(nioSocketChannel, e);
        }
    }

    private void handleQueuedWrites() {
        WriteOperation writeOperation;
        while ((writeOperation = queuedWrites.poll()) != null) {
            if (writeOperation.getChannel().isWritable()) {
                queueWriteInChannelBuffer(writeOperation);
            } else {
                writeOperation.getListener().onFailure(new ClosedChannelException());
            }
        }
    }

    private void setUpNewChannels() {
        NioSocketChannel newChannel;
        while ((newChannel = this.newChannels.poll()) != null) {
            setupChannel(newChannel);
        }
    }

    private void setupChannel(NioSocketChannel newChannel) {
        assert newChannel.getSelector() == this : "The channel must be registered with the selector with which it was created";
        try {
            if (newChannel.isOpen()) {
                newChannel.register();
                SelectionKey key = newChannel.getSelectionKey();
                key.attach(newChannel);
                eventHandler.handleRegistration(newChannel);
                attemptConnect(newChannel, false);
            } else {
                eventHandler.registrationException(newChannel, new ClosedChannelException());
            }
        } catch (Exception e) {
            eventHandler.registrationException(newChannel, e);
        }
    }

    private void attemptConnect(NioSocketChannel newChannel, boolean connectEvent) {
        try {
            if (newChannel.finishConnect()) {
                eventHandler.handleConnect(newChannel);
            } else if (connectEvent) {
                eventHandler.connectException(newChannel, new IOException("Received OP_CONNECT but connect failed"));
            }
        } catch (Exception e) {
            eventHandler.connectException(newChannel, e);
        }
    }
}
