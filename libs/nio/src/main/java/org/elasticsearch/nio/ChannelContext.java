/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import org.elasticsearch.core.CompletableContext;

import java.io.IOException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Implements the logic related to interacting with a java.nio channel. For example: registering with a
 * selector, managing the selection key, closing, etc is implemented by this class or its subclasses.
 *
 * @param <S> the type of channel
 */
public abstract class ChannelContext<S extends SelectableChannel & NetworkChannel> {

    protected final S rawChannel;
    private final Consumer<Exception> exceptionHandler;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();
    private volatile SelectionKey selectionKey;

    ChannelContext(S rawChannel, Consumer<Exception> exceptionHandler) {
        this.rawChannel = rawChannel;
        this.exceptionHandler = exceptionHandler;
    }

    protected void register() throws IOException {
        doSelectorRegister();
    }

    protected void channelActive() throws IOException {}

    // Package private for testing
    void doSelectorRegister() throws IOException {
        setSelectionKey(rawChannel.register(getSelector().rawSelector(), 0, this));
    }

    protected SelectionKey getSelectionKey() {
        return selectionKey;
    }

    // public for tests
    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    /**
     * This method cleans up any context resources that need to be released when a channel is closed. It
     * should only be called by the selector thread.
     *
     * @throws IOException during channel / context close
     */
    public void closeFromSelector() throws IOException {
        if (isOpen()) {
            try {
                rawChannel.close();
                closeContext.complete(null);
            } catch (Exception e) {
                closeContext.completeExceptionally(e);
                throw e;
            }
        }
    }

    /**
     * Add a listener that will be called when the channel is closed.
     *
     * @param listener to be called
     */
    public void addCloseListener(BiConsumer<Void, Exception> listener) {
        closeContext.addListener(listener);
    }

    public boolean isOpen() {
        return closeContext.isDone() == false;
    }

    protected void handleException(Exception e) {
        exceptionHandler.accept(e);
    }

    /**
     * Schedules a channel to be closed by the selector event loop with which it is registered.
     *
     * If the channel is open and the state can be transitioned to closed, the close operation will
     * be scheduled with the event loop.
     *
     * Depending on the underlying protocol of the channel, a close operation might simply close the socket
     * channel or may involve reading and writing messages.
     */
    public abstract void closeChannel();

    public abstract NioSelector getSelector();

    public abstract NioChannel getChannel();

}
