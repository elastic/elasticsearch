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
import java.nio.channels.SelectionKey;
import java.util.function.Consumer;

/**
 * Event handler designed to handle events from non-server sockets
 */
public class SocketEventHandler extends EventHandler {

    public SocketEventHandler(Consumer<Exception> exceptionHandler) {
        super(exceptionHandler);
    }

    /**
     * This method is called when a NioSocketChannel is successfully registered. It should only be called
     * once per channel.
     *
     * @param context that was registered
     */
    protected void handleRegistration(SocketChannelContext context) throws IOException {
        context.register();
        SelectionKey selectionKey = context.getSelectionKey();
        selectionKey.attach(context);
        if (context.readyForFlush()) {
            SelectionKeyUtils.setConnectReadAndWriteInterested(selectionKey);
        } else {
            SelectionKeyUtils.setConnectAndReadInterested(selectionKey);
        }
    }

    /**
     * This method is called when an attempt to register a channel throws an exception.
     *
     * @param context that was registered
     * @param exception that occurred
     */
    protected void registrationException(SocketChannelContext context, Exception exception) {
        context.handleException(exception);
    }

    /**
     * This method is called when a NioSocketChannel has just been accepted or if it has receive an
     * OP_CONNECT event.
     *
     * @param context that was registered
     */
    protected void handleConnect(SocketChannelContext context) throws IOException {
        if (context.connect()) {
            SelectionKeyUtils.removeConnectInterested(context.getSelectionKey());
        }
    }

    /**
     * This method is called when an attempt to connect a channel throws an exception.
     *
     * @param context that was connecting
     * @param exception that occurred
     */
    protected void connectException(SocketChannelContext context, Exception exception) {
        context.handleException(exception);
    }

    /**
     * This method is called when a channel signals it is ready for be read. All of the read logic should
     * occur in this call.
     *
     * @param context that can be read
     */
    protected void handleRead(SocketChannelContext context) throws IOException {
        context.read();
    }

    /**
     * This method is called when an attempt to read from a channel throws an exception.
     *
     * @param context that was being read
     * @param exception that occurred
     */
    protected void readException(SocketChannelContext context, Exception exception) {
        context.handleException(exception);
    }

    /**
     * This method is called when a channel signals it is ready to receive writes. All of the write logic
     * should occur in this call.
     *
     * @param context that can be written to
     */
    protected void handleWrite(SocketChannelContext context) throws IOException {
        context.flushChannel();
    }

    /**
     * This method is called when an attempt to write to a channel throws an exception.
     *
     * @param context that was being written to
     * @param exception that occurred
     */
    protected void writeException(SocketChannelContext context, Exception exception) {
        context.handleException(exception);
    }

    /**
     * This method is called when a listener attached to a channel operation throws an exception.
     *
     * @param exception that occurred
     */
    protected void listenerException(Exception exception) {
        exceptionHandler.accept(exception);
    }

    /**
     * @param context that was handled
     */
    protected void postHandling(SocketChannelContext context) {
        if (context.selectorShouldClose()) {
            handleClose(context);
        } else {
            SelectionKey selectionKey = context.getSelectionKey();
            boolean currentlyWriteInterested = SelectionKeyUtils.isWriteInterested(selectionKey);
            boolean pendingWrites = context.readyForFlush();
            if (currentlyWriteInterested == false && pendingWrites) {
                SelectionKeyUtils.setWriteInterested(selectionKey);
            } else if (currentlyWriteInterested && pendingWrites == false) {
                SelectionKeyUtils.removeWriteInterested(selectionKey);
            }
        }
    }
}
