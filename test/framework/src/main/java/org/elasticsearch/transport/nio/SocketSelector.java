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
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.SelectionKeyUtils;
import org.elasticsearch.transport.nio.channel.WriteContext;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SocketSelector extends ESSelector {

    private static final ClosedSelectorException CLOSED_SELECTOR_EXCEPTION = new ClosedSelectorException();
    private static final ClosedChannelException CLOSED_CHANNEL_EXCEPTION = new ClosedChannelException();
    private static final CancelledKeyException CANCELLED_KEY_EXCEPTION = new CancelledKeyException();

    private final ConcurrentLinkedQueue<NioSocketChannel> newChannels = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<WriteOperation> queuedWrites = new ConcurrentLinkedQueue<>();
    private final SocketEventHandler eventHandler;

    public SocketSelector(SocketEventHandler eventHandler, BigArrays bigArrays) throws IOException {
        super(eventHandler, bigArrays);
        this.eventHandler = eventHandler;
    }

    public SocketSelector(SocketEventHandler eventHandler, BigArrays bigArrays, Selector selector) throws IOException {
        super(eventHandler, bigArrays, selector);
        this.eventHandler = eventHandler;
    }

    @Override
    public void doSelect(int timeout) throws IOException, ClosedSelectorException {
        setUpNewChannels();
        handleQueuedWrites();

        int ready = selector.select(timeout);
        if (ready > 0) {
            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            processKeys(selectionKeys);
        }

    }

    @Override
    protected void cleanup() {
        WriteOperation op;
        while ((op = queuedWrites.poll()) != null) {
            op.getListener().onFailure(CLOSED_SELECTOR_EXCEPTION);
        }
        channelsToClose.addAll(newChannels);
        channelsToClose.addAll(registeredChannels);
        closePendingChannels();
    }

    public void registerSocketChannel(NioSocketChannel nioSocketChannel) {
        newChannels.offer(nioSocketChannel);
        wakeup();
    }

    public void queueWrite(WriteOperation writeOperation) {
        queuedWrites.offer(writeOperation);
        if (isOpen() == false) {
            boolean wasRemoved = queuedWrites.remove(writeOperation);
            if (wasRemoved) {
                writeOperation.getListener().onFailure(CLOSED_SELECTOR_EXCEPTION);
            }
        } else {
            wakeup();
        }
    }

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

    private void processKeys(Set<SelectionKey> selectionKeys) {
        Iterator<SelectionKey> keyIterator = selectionKeys.iterator();
        while (keyIterator.hasNext()) {
            SelectionKey sk = keyIterator.next();
            keyIterator.remove();
            NioSocketChannel nioSocketChannel = (NioSocketChannel) sk.attachment();
            if (sk.isValid()) {
                try {
                    int ops = sk.readyOps();
                    if ((ops & SelectionKey.OP_CONNECT) != 0) {
                        attemptConnect(nioSocketChannel);
                    }

                    if (nioSocketChannel.isConnectComplete()) {
                        if ((ops & SelectionKey.OP_WRITE) != 0) {
                            handleWrite(nioSocketChannel);
                        }

                        if ((ops & SelectionKey.OP_READ) != 0) {
                            handleRead(nioSocketChannel);
                        }
                    }
                } catch (CancelledKeyException e) {
                    eventHandler.genericChannelException(nioSocketChannel, e);
                }
            } else {
                eventHandler.genericChannelException(nioSocketChannel, CANCELLED_KEY_EXCEPTION);
            }
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
                writeOperation.getListener().onFailure(CLOSED_CHANNEL_EXCEPTION);
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
        try {
            if (newChannel.register(this)) {
                registeredChannels.add(newChannel);
                SelectionKey key = newChannel.getSelectionKey();
                key.attach(newChannel);
                eventHandler.handleRegistration(newChannel);
                attemptConnect(newChannel);
            }
        } catch (Exception e) {
            eventHandler.registrationException(newChannel, e);
        }
    }

    private void attemptConnect(NioSocketChannel newChannel) {
        try {
            if (newChannel.finishConnect()) {
                eventHandler.handleConnect(newChannel);
            }
        } catch (Exception e) {
            eventHandler.connectException(newChannel, e);
        }
    }
}
