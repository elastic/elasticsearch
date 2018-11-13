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

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;


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
     * This sets the low level socket option {@link java.net.StandardSocketOptions} SO_LINGER on a channel.
     *
     * @param value to set for SO_LINGER
     * @throws IOException that can be throw by the low level socket implementation
     */
    void setSoLinger(int value) throws IOException;

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
     * Returns stats about this channel
     */
    ChannelStats getChannelStats();

    class ChannelStats {

        private volatile long lastReadTime;
        private volatile long lastWriteTime;

        public ChannelStats() {
            // We set the initial value to a day in the past to ensure that the next read or write time is
            // greater than the initial time. This is important because we mark reads and writes with our
            // ThreadPool.relativeTimeInMillis() which is incremented every 200ms.
            long oneDayAgo = TimeValue.nsecToMSec(System.nanoTime() - TimeUnit.HOURS.toNanos(24));
            lastReadTime = oneDayAgo;
            lastWriteTime = oneDayAgo;
        }

        void markRelativeReadTime(long readTime) {
            lastReadTime = readTime;
        }

        void markRelativeWriteTime(long writeTime) {
            lastWriteTime = writeTime;
        }

        long lastRelativeReadTime() {
            return lastReadTime;
        }

        long lastRelativeWriteTime() {
            return lastWriteTime;
        }
    }
}
