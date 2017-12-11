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
import org.apache.lucene.util.IOUtils;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The NioGroup is a group of selectors for interfacing with java nio. When it is started it will create the
 * configured number of socket and acceptor selectors. Each selector will be running in a dedicated thread.
 * Server connections can be bound using the {@link #bindServerChannel(InetSocketAddress, ChannelFactory)}
 * method. Client connections can be opened using the {@link #openChannel(InetSocketAddress, ChannelFactory)}
 * method.
 *
 * The logic specific to a particular channel is provided by the {@link ChannelFactory} passed to the method
 * when the channel is created. This is what allows an NioGroup to support different channel types.
 */
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
        try {
            for (int i = 0; i < socketSelectorCount; ++i) {
                SocketSelector selector = new SocketSelector(socketEventHandlerFunction.apply(logger));
                socketSelectors.add(selector);
            }
            startSelectors(socketSelectors, socketSelectorThreadFactory);

            for (int i = 0; i < acceptorCount; ++i) {
                SocketSelector[] childSelectors = this.socketSelectors.toArray(new SocketSelector[this.socketSelectors.size()]);
                Supplier<SocketSelector> selectorSupplier = new RoundRobinSupplier<>(childSelectors);
                AcceptingSelector acceptor = new AcceptingSelector(acceptorEventHandlerFunction.apply(logger, selectorSupplier));
                acceptors.add(acceptor);
            }
            startSelectors(acceptors, acceptorThreadFactory);
        } catch (Exception e) {
            try {
                stop();
            } catch (Exception e1) {
                e.addSuppressed(e1);
            }
            throw e;
        }

        socketSelectorSupplier = new RoundRobinSupplier<>(socketSelectors.toArray(new SocketSelector[socketSelectors.size()]));
        acceptorSupplier = new RoundRobinSupplier<>(acceptors.toArray(new AcceptingSelector[acceptors.size()]));
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

    public void stop() throws IOException {
        IOUtils.close(Stream.concat(acceptors.stream(), socketSelectors.stream()).collect(Collectors.toList()));
    }

    private static <S extends ESSelector> void startSelectors(Iterable<S> selectors, ThreadFactory threadFactory) {
        for (ESSelector acceptor : selectors) {
            if (acceptor.isRunning() == false) {
                threadFactory.newThread(acceptor::runLoop).start();
                acceptor.isRunningFuture().actionGet();
            }
        }
    }
}
