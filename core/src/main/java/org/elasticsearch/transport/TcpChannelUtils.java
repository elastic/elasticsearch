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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TcpChannelUtils {

    public static <C extends TcpChannel<C>> void closeChannel(C channel, boolean blocking, Logger logger) {
        if (channel.isOpen()) {
            ListenableActionFuture<C> f = channel.closeAsync();
            f.addListener(ActionListener.wrap(c -> {
                },
                e -> logger.debug(() -> new ParameterizedMessage("exception while closing channel: {}", channel), e)));
            if (blocking) {
                blockOnFutures(Collections.singletonList(f));
            }
        }
    }

    public static <C extends TcpChannel<C>> void closeChannels(List<C> channels, boolean blocking, Logger logger) {
        ArrayList<ListenableActionFuture<C>> futures = new ArrayList<>(channels.size());
        for (final C channel : channels) {
            if (channel.isOpen()) {
                ListenableActionFuture<C> f = channel.closeAsync();
                f.addListener(ActionListener.wrap(c -> {
                    },
                    e -> logger.debug(() -> new ParameterizedMessage("exception while closing channel: {}", channel), e)));
                futures.add(f);
            }
        }

        if (blocking) {
            blockOnFutures(futures);
        }
    }

    public static <C extends TcpChannel<C>> void closeServerChannels(String profile, List<C> channels, Logger logger) {
        ArrayList<ListenableActionFuture<C>> futures = new ArrayList<>(channels.size());
        for (final C channel : channels) {
            if (channel.isOpen()) {
                ListenableActionFuture<C> f = channel.closeAsync();
                f.addListener(ActionListener.wrap(c -> {
                    },
                    e -> logger.warn(() -> new ParameterizedMessage("Error closing serverChannel for profile [{}]", profile), e)));
                futures.add(f);
            }
        }

        blockOnFutures(futures);
    }

    public static <C extends TcpChannel<C>> void finishConnection(DiscoveryNode discoveryNode, List<PlainChannelFuture<C>> pendingChannels,
                                                                  TimeValue connectTimeout) throws ConnectTransportException {
        Exception connectionException = null;
        boolean allConnected = true;

        for (PlainChannelFuture<C> pendingChannel : pendingChannels) {
            try {
                pendingChannel.get(connectTimeout.getMillis(), TimeUnit.MILLISECONDS);
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


    private static <C extends TcpChannel<C>> void blockOnFutures(List<ListenableActionFuture<C>> futures) {
        for (ListenableActionFuture<C> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                // Ignore as we already attached a listener to log
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Future got interrupted", e);
            }
        }
    }

    public static <Channel extends TcpChannel<Channel>> void addCloseExceptionListener(Channel channel, Logger logger) {
        channel.getCloseFuture().addListener(ActionListener.wrap(c -> {},
            e -> logger.debug(() -> new ParameterizedMessage("exception while closing channel: {}", channel), e)));
    }
}
