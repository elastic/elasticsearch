/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.core.TimeValue;

import java.net.InetSocketAddress;

/**
 * This is a tcp channel representing a single channel connection to another node. It is the base channel
 * abstraction used by the {@link TcpTransport} and {@link TransportService}. All tcp transport
 * implementations must return channels that adhere to the required method contracts.
 */
public interface TcpChannel extends CloseableChannel {

    /**
     * Indicates if the channel is an inbound server channel.
     */
    boolean isServerChannel();

    /**
     * This returns the profile for this channel.
     */
    String getProfile();

    /**
     * Returns the local address for this channel.
     *
     * @return the local address of this channel.
     */
    InetSocketAddress getLocalAddress();

    /**
     * Returns the remote address for this channel. Can be null if channel does not have a remote address.
     *
     * @return the remote address of this channel.
     */
    InetSocketAddress getRemoteAddress();

    /**
     * Sends a tcp message to the channel. The listener will be executed once the send process has been
     * completed.
     *
     * @param reference to send to channel
     * @param listener to execute upon send completion
     */
    void sendMessage(BytesReference reference, ActionListener<Void> listener);

    /**
     * Adds a listener that will be executed when the channel is connected. If the channel is still
     * unconnected when this listener is added, the listener will be executed by the thread that eventually
     * finishes the channel connection. If the channel is already connected when the listener is added the
     * listener will immediately be executed by the thread that is attempting to add the listener.
     *
     * @param listener to be executed
     */
    void addConnectListener(ActionListener<Void> listener);

    /**
     * Report a close-causing exception on this channel
     *
     * @param e the exception
     */
    void setCloseException(Exception e);

    /**
     * Returns stats about this channel
     */
    ChannelStats getChannelStats();

    class ChannelStats {

        private volatile long lastAccessedTime;

        public ChannelStats() {
            lastAccessedTime = TimeValue.nsecToMSec(System.nanoTime());
        }

        void markAccessed(long relativeMillisTime) {
            lastAccessedTime = relativeMillisTime;
        }

        long lastAccessedTime() {
            return lastAccessedTime;
        }
    }
}
