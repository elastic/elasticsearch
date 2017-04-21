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
import org.elasticsearch.transport.nio.channel.NioServerSocketChannel;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class AcceptingSelector extends ESSelector {

    private static final CancelledKeyException CANCELLED_KEY_EXCEPTION = new CancelledKeyException();

    private final AcceptorEventHandler eventHandler;
    private final ConcurrentLinkedQueue<NioServerSocketChannel> newChannels = new ConcurrentLinkedQueue<>();

    public AcceptingSelector(AcceptorEventHandler eventHandler, BigArrays bigArrays) throws IOException {
        super(eventHandler, bigArrays);
        this.eventHandler = eventHandler;
    }

    public AcceptingSelector(AcceptorEventHandler eventHandler, BigArrays bigArrays, Selector selector) throws IOException {
        super(eventHandler, bigArrays, selector);
        this.eventHandler = eventHandler;
    }

    @Override
    public void doSelect(int timeout) throws IOException, ClosedSelectorException {
        setUpNewServerChannels();

        try {
            int ready = selector.select(timeout);
            if (ready > 0) {
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectionKeys.iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey sk = keyIterator.next();
                    keyIterator.remove();
                    acceptChannel(sk);
                }
            }
        } catch (ClosedSelectorException e) {
            if (isRunning()) {
                throw e;
            }
        }
    }

    @Override
    protected void cleanup() {
        channelsToClose.addAll(registeredChannels);
        closePendingChannels();
    }

    public void registerServerChannel(NioServerSocketChannel serverSocketChannel) throws ClosedChannelException {
        newChannels.add(serverSocketChannel);
    }

    private void setUpNewServerChannels() throws ClosedChannelException {
        NioServerSocketChannel newChannel;
        while ((newChannel = this.newChannels.poll()) != null) {
            if (newChannel.register(this)) {
                SelectionKey selectionKey = newChannel.getSelectionKey();
                selectionKey.interestOps(SelectionKey.OP_ACCEPT);
                selectionKey.attach(newChannel);
                registeredChannels.add(newChannel);
                eventHandler.serverChannelRegistered(newChannel);
            }
        }
    }

    private void acceptChannel(SelectionKey sk) {
        NioServerSocketChannel serverChannel = (NioServerSocketChannel) sk.attachment();
        if (sk.isValid()) {
            try {
                if (sk.isAcceptable()) {
                    try {
                        eventHandler.acceptChannel(serverChannel);
                    } catch (IOException e) {
                        eventHandler.acceptException(serverChannel, e);
                    }
                }
            } catch (CancelledKeyException ex) {
                eventHandler.genericServerChannelException(serverChannel, ex);
            }
        } else {
            eventHandler.genericServerChannelException(serverChannel, CANCELLED_KEY_EXCEPTION);
        }
    }
}
