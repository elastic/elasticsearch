/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class EventHandler {

    protected final Consumer<Exception> exceptionHandler;
    private final Supplier<NioSelector> selectorSupplier;

    public EventHandler(Consumer<Exception> exceptionHandler, Supplier<NioSelector> selectorSupplier) {
        this.exceptionHandler = exceptionHandler;
        this.selectorSupplier = selectorSupplier;
    }

    /**
     * This method is called when a server channel signals it is ready to accept a connection. All of the
     * accept logic should occur in this call.
     *
     * @param context that can accept a connection
     */
    protected void acceptChannel(ServerChannelContext context) throws IOException {
        context.acceptChannels(selectorSupplier);
    }

    /**
     * This method is called when an attempt to accept a connection throws an exception.
     *
     * @param context that accepting a connection
     * @param exception that occurred
     */
    protected void acceptException(ServerChannelContext context, Exception exception) {
        context.handleException(exception);
    }

    /**
     * This method is called when a NioChannel is being registered with the selector. It should
     * only be called once per channel.
     *
     * @param context that was registered
     */
    protected void handleRegistration(ChannelContext<?> context) throws IOException {
        context.register();
        assert context.getSelectionKey() != null : "SelectionKey should not be null after registration";
        assert context.getSelectionKey().attachment() != null : "Attachment should not be null after registration";
    }

    /**
     * This method is called when an attempt to register a channel throws an exception.
     *
     * @param context that was registered
     * @param exception that occurred
     */
    protected void registrationException(ChannelContext<?> context, Exception exception) {
        context.handleException(exception);
    }

    /**
     * This method is called after a NioChannel is active with the selector. It should only be called once
     * per channel.
     *
     * @param context that was marked active
     */
    protected void handleActive(ChannelContext<?> context) throws IOException {
        context.channelActive();
        if (context instanceof SocketChannelContext) {
            if (((SocketChannelContext) context).readyForFlush()) {
                SelectionKeyUtils.setConnectReadAndWriteInterested(context.getSelectionKey());
            } else {
                SelectionKeyUtils.setConnectAndReadInterested(context.getSelectionKey());
            }
        } else {
            assert context instanceof ServerChannelContext : "If not SocketChannelContext the context must be a ServerChannelContext";
            SelectionKeyUtils.setAcceptInterested(context.getSelectionKey());
        }
    }

    /**
     * This method is called when setting a channel to active throws an exception.
     *
     * @param context that was marked active
     * @param exception that occurred
     */
    protected void activeException(ChannelContext<?> context, Exception exception) {
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
     * This method is called when a task or listener attached to a channel is available to run.
     *
     * @param task to handle
     */
    protected void handleTask(Runnable task) {
        task.run();
    }

    /**
     * This method is called when a task or listener attached to a channel operation throws an exception.
     *
     * @param exception that occurred
     */
    protected void taskException(Exception exception) {
        exceptionHandler.accept(exception);
    }

    /**
     * This method is called after events (READ, WRITE, CONNECT) have been handled for a channel.
     *
     * @param context that was handled
     */
    protected void postHandling(SocketChannelContext context) {
        if (context.selectorShouldClose()) {
            try {
                handleClose(context);
            } catch (IOException e) {
                closeException(context, e);
            }
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

    /**
     * This method handles an IOException that was thrown during a call to {@link Selector#select(long)} or
     * {@link Selector#close()}.
     *
     * @param exception the exception
     */
    protected void selectorException(IOException exception) {
        exceptionHandler.accept(exception);
    }

    /**
     * This method handles an exception that was uncaught during a select loop.
     *
     * @param exception that was uncaught
     */
    protected void uncaughtException(Exception exception) {
        Thread thread = Thread.currentThread();
        thread.getUncaughtExceptionHandler().uncaughtException(thread, exception);
    }

    /**
     * This method handles the closing of an NioChannel
     *
     * @param context that should be closed
     */
    protected void handleClose(ChannelContext<?> context) throws IOException {
        context.closeFromSelector();
        assert context.isOpen() == false : "Should always be done as we are on the selector thread";
    }

    /**
     * This method is called when an attempt to close a channel throws an exception.
     *
     * @param context that was being closed
     * @param exception that occurred
     */
    protected void closeException(ChannelContext<?> context, Exception exception) {
        context.handleException(exception);
    }

    /**
     * This method is called when handling an event from a channel fails due to an unexpected exception.
     * An example would be if checking ready ops on a {@link java.nio.channels.SelectionKey} threw
     * {@link java.nio.channels.CancelledKeyException}.
     *
     * @param context that caused the exception
     * @param exception that was thrown
     */
    protected void genericChannelException(ChannelContext<?> context, Exception exception) {
        context.handleException(exception);
    }
}
