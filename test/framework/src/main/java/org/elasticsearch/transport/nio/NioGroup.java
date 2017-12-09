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
import org.elasticsearch.transport.nio.channel.ChannelFactory;
import org.elasticsearch.transport.nio.channel.NioServerSocketChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class NioGroup {

    private final Logger logger;

    private final int acceptorCount;
    private final ThreadFactory acceptorThreadFactory;
    private final ArrayList<AcceptingSelector> acceptors;
    private final BiFunction<Logger, Supplier<SocketSelector>, AcceptorEventHandler> acceptorEventHandlerFunction;
    private RoundRobinSupplier<AcceptingSelector> acceptorSupplier;

    private final int socketSelectorCount;
    private final ThreadFactory socketSelectorThreadFactory;
    private final ArrayList<SocketSelector> socketSelectors;
    private final Function<Logger, SocketEventHandler> socketEventHandlerFunction;
    private RoundRobinSupplier<SocketSelector> socketSelectorSupplier;

    public NioGroup(Logger logger, ThreadFactory acceptorThreadFactory, int acceptorCount,
                    BiFunction<Logger, Supplier<SocketSelector>, AcceptorEventHandler> acceptorEventHandlerFunction,
                    ThreadFactory socketSelectorThreadFactory, int socketSelectorCount,
                    Function<Logger, SocketEventHandler> socketEventHandlerFunction) {
        this.logger = logger;
        this.acceptorThreadFactory = acceptorThreadFactory;
        this.acceptorEventHandlerFunction = acceptorEventHandlerFunction;
        this.socketSelectorThreadFactory = socketSelectorThreadFactory;
        this.acceptorCount = acceptorCount;
        this.socketSelectorCount = socketSelectorCount;
        this.socketEventHandlerFunction = socketEventHandlerFunction;
        acceptors = new ArrayList<>(this.acceptorCount);
        socketSelectors = new ArrayList<>(this.socketSelectorCount);
    }

    public void start() throws IOException {
        for (int i = 0; i < socketSelectorCount; ++i) {
            SocketSelector selector = new SocketSelector(socketEventHandlerFunction.apply(logger));
            socketSelectors.add(selector);
        }

        startSelectors(socketSelectors, socketSelectorThreadFactory);


        for (int i = 0; i < acceptorCount; ++i) {
            Supplier<SocketSelector> selectorSupplier = new RoundRobinSupplier<>(socketSelectors);
            AcceptingSelector acceptor = new AcceptingSelector(acceptorEventHandlerFunction.apply(logger, selectorSupplier));
            acceptors.add(acceptor);
        }

        startSelectors(acceptors, acceptorThreadFactory);

        socketSelectorSupplier = new RoundRobinSupplier<>(socketSelectors);
        acceptorSupplier = new RoundRobinSupplier<>(acceptors);
    }

    public <S extends NioServerSocketChannel> S bindServerChannel(InetSocketAddress address, ChannelFactory<S, ?> factory)
        throws IOException {
        if (acceptorCount == 0) {
            throw new IllegalArgumentException("There are no acceptors configured. Without acceptors, server channels are not supported.");
        }
        return factory.openNioServerSocketChannel(address, acceptorSupplier.get());
    }

    public <S extends NioSocketChannel> S openChannel(InetSocketAddress address, ChannelFactory<?, S> factory) throws IOException {
        return factory.openNioChannel(address, socketSelectorSupplier.get());
    }

    public void stop() {
        for (AcceptingSelector acceptor : acceptors) {
            shutdownSelector(acceptor);
        }
        acceptors.clear();

        for (SocketSelector selector : socketSelectors) {
            shutdownSelector(selector);
        }
        socketSelectors.clear();
    }

    private static <S extends ESSelector> void startSelectors(Iterable<S> selectors, ThreadFactory threadFactory) {
        for (ESSelector acceptor : selectors) {
            if (acceptor.isRunning() == false) {
                threadFactory.newThread(acceptor::runLoop).start();
                acceptor.isRunningFuture().actionGet();
            }
        }
    }

    private void shutdownSelector(ESSelector selector) {
        try {
            selector.close();
        } catch (Exception e) {
            logger.warn("unexpected exception while stopping selector", e);
        }
    }
}
