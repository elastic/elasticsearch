/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface CloseableChannel extends Releasable {

    /**
     * Closes the channel. For most implementations, this will be be an asynchronous process. For this
     * reason, this method does not throw {@link java.io.IOException} There is no guarantee that the channel
     * will be closed when this method returns. Use the {@link #addCloseListener(ActionListener)} method
     * to implement logic that depends on knowing when the channel is closed.
     */
    @Override
    void close();

    /**
     * Adds a listener that will be executed when the channel is closed. If the channel is still open when
     * this listener is added, the listener will be executed by the thread that eventually closes the
     * channel. If the channel is already closed when the listener is added the listener will immediately be
     * executed by the thread that is attempting to add the listener.
     *
     * @param listener to be executed
     */
    void addCloseListener(ActionListener<Void> listener);

    /**
     * Indicates whether a channel is currently open
     *
     * @return boolean indicating if channel is open
     */
    boolean isOpen();

    /**
     * Closes the channels in a blocking manner.
     *
     * @param channels to close
     */
    static <C extends CloseableChannel> void closeChannelsBlocking(List<C> channels) {
        if (channels.isEmpty() == false) {
            Releasables.close(channels);
            final PlainActionFuture<Collection<Void>> future = PlainActionFuture.newFuture();
            final ActionListener<Void> closeListener = new GroupedActionListener<>(future, channels.size());
            for (final C channel : channels) {
                channel.addCloseListener(closeListener);
            }
            try {
                future.get();
            } catch (ExecutionException e) {
                // Ignore as we are only interested in waiting for the close process to complete. Logging
                // close exceptions happens elsewhere.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
