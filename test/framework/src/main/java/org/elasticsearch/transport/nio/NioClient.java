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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.nio.channel.ChannelFactory;
import org.elasticsearch.transport.nio.channel.ConnectFuture;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class NioClient {

    private final Logger logger;
    private final OpenChannels openChannels;
    private final Supplier<SocketSelector> selectorSupplier;
    private final TimeValue defaultConnectTimeout;
    private final ChannelFactory channelFactory;
    private final Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);

    public NioClient(Logger logger, OpenChannels openChannels, Supplier<SocketSelector> selectorSupplier, TimeValue connectTimeout,
                     ChannelFactory channelFactory) {
        this.logger = logger;
        this.openChannels = openChannels;
        this.selectorSupplier = selectorSupplier;
        this.defaultConnectTimeout = connectTimeout;
        this.channelFactory = channelFactory;
    }

    public boolean connectToChannels(DiscoveryNode node,
                                     NioSocketChannel[] channels,
                                     TimeValue connectTimeout,
                                     Consumer<NioChannel> closeListener) throws IOException {
        boolean allowedToConnect = semaphore.tryAcquire();
        if (allowedToConnect == false) {
            return false;
        }

        final ArrayList<NioSocketChannel> connections = new ArrayList<>(channels.length);
        connectTimeout = getConnectTimeout(connectTimeout);
        final InetSocketAddress address = node.getAddress().address();
        try {
            for (int i = 0; i < channels.length; i++) {
                SocketSelector selector = selectorSupplier.get();
                NioSocketChannel nioSocketChannel = channelFactory.openNioChannel(address, selector, closeListener);
                openChannels.clientChannelOpened(nioSocketChannel);
                connections.add(nioSocketChannel);
            }

            Exception ex = null;
            boolean allConnected = true;
            for (NioSocketChannel socketChannel : connections) {
                ConnectFuture connectFuture = socketChannel.getConnectFuture();
                boolean success = connectFuture.awaitConnectionComplete(connectTimeout.getMillis(), TimeUnit.MILLISECONDS);
                if (success == false) {
                    allConnected = false;
                    Exception exception = connectFuture.getException();
                    if (exception != null) {
                        ex = exception;
                        break;
                    }
                }
            }

            if (allConnected == false) {
                if (ex == null) {
                    throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]");
                } else {
                    throw new ConnectTransportException(node, "connect_exception", ex);
                }
            }
            addConnectionsToList(channels, connections);
            return true;

        } catch (IOException | RuntimeException e) {
            closeChannels(connections, e);
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closeChannels(connections, e);
            throw new ElasticsearchException(e);
        } finally {
            semaphore.release();
        }
    }

    public void close() {
        semaphore.acquireUninterruptibly(Integer.MAX_VALUE);
    }

    private TimeValue getConnectTimeout(TimeValue connectTimeout) {
        if (connectTimeout != null && connectTimeout.equals(defaultConnectTimeout) == false) {
            return connectTimeout;
        } else {
            return defaultConnectTimeout;
        }
    }

    private static void addConnectionsToList(NioSocketChannel[] channels, ArrayList<NioSocketChannel> connections) {
        final Iterator<NioSocketChannel> iterator = connections.iterator();
        for (int i = 0; i < channels.length; i++) {
            assert iterator.hasNext();
            channels[i] = iterator.next();
        }
        assert iterator.hasNext() == false : "not all created connection have been consumed";
    }

    private void closeChannels(ArrayList<NioSocketChannel> connections, Exception e) {
        for (final NioSocketChannel socketChannel : connections) {
            try {
                socketChannel.closeAsync().awaitClose();
            } catch (Exception inner) {
                logger.trace("exception while closing channel", e);
                e.addSuppressed(inner);
            }
        }
    }
}
