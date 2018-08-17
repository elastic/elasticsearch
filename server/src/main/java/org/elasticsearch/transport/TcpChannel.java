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

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * This is a tcp channel representing a single channel connection to another node. It is the base channel
 * abstraction used by the {@link TcpTransport} and {@link TransportService}. All tcp transport
 * implementations must return channels that adhere to the required method contracts.
 */
public interface TcpChannel extends CloseableChannel {

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
     * Awaits for all of the pending connections to complete. Will throw an exception if at least one of the
     * connections fails.
     *
     * @param discoveryNode the node for the pending connections
     * @param connectionFutures representing the pending connections
     * @param connectTimeout to wait for a connection
     * @throws ConnectTransportException if one of the connections fails
     */
    static void awaitConnected(DiscoveryNode discoveryNode, List<ActionFuture<Void>> connectionFutures, TimeValue connectTimeout)
        throws ConnectTransportException {
        Exception connectionException = null;
        boolean allConnected = true;

        for (ActionFuture<Void> connectionFuture : connectionFutures) {
            try {
                connectionFuture.get(connectTimeout.getMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                allConnected = false;
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } catch (ExecutionException e) {
                allConnected = false;
                connectionException = (Exception) e.getCause();
                break;
            }
        }

        if (allConnected == false) {
            if (connectionException == null) {
                throw new ConnectTransportException(discoveryNode, "connect_timeout[" + connectTimeout + "]");
            } else {
                throw new ConnectTransportException(discoveryNode, "connect_exception", connectionException);
            }
        }
    }

}
