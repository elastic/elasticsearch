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

package org.elasticsearch.nio;

import org.elasticsearch.nio.utils.ExceptionsHelper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * <p>
 * The logic specific to a particular channel is provided by the {@link ChannelFactory} passed to the method
 * when the channel is created. This is what allows an NioGroup to support different channel types.
 */
public class NioGroup implements AutoCloseable {


    private final ArrayList<AcceptingSelector> acceptors;
    private final RoundRobinSupplier<AcceptingSelector> acceptorSupplier;

    private final ArrayList<SocketSelector> socketSelectors;
    private final RoundRobinSupplier<SocketSelector> socketSelectorSupplier;

    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    public NioGroup(ThreadFactory acceptorThreadFactory, int acceptorCount,
                    Function<Supplier<SocketSelector>, AcceptorEventHandler> acceptorEventHandlerFunction,
                    ThreadFactory socketSelectorThreadFactory, int socketSelectorCount,
                    Supplier<SocketEventHandler> socketEventHandlerFunction) throws IOException {
        acceptors = new ArrayList<>(acceptorCount);
        socketSelectors = new ArrayList<>(socketSelectorCount);

        try {
            for (int i = 0; i < socketSelectorCount; ++i) {
                SocketSelector selector = new SocketSelector(socketEventHandlerFunction.get());
                socketSelectors.add(selector);
            }
            startSelectors(socketSelectors, socketSelectorThreadFactory);

            for (int i = 0; i < acceptorCount; ++i) {
                SocketSelector[] childSelectors = this.socketSelectors.toArray(new SocketSelector[this.socketSelectors.size()]);
                Supplier<SocketSelector> selectorSupplier = new RoundRobinSupplier<>(childSelectors);
                AcceptingSelector acceptor = new AcceptingSelector(acceptorEventHandlerFunction.apply(selectorSupplier));
                acceptors.add(acceptor);
            }
            startSelectors(acceptors, acceptorThreadFactory);
        } catch (Exception e) {
            try {
                close();
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
        ensureOpen();
        if (acceptors.isEmpty()) {
            throw new IllegalArgumentException("There are no acceptors configured. Without acceptors, server channels are not supported.");
        }
        return factory.openNioServerSocketChannel(address, acceptorSupplier);
    }

    public <S extends NioSocketChannel> S openChannel(InetSocketAddress address, ChannelFactory<?, S> factory) throws IOException {
        ensureOpen();
        return factory.openNioChannel(address, socketSelectorSupplier);
    }

    @Override
    public void close() throws IOException {
        if (isOpen.compareAndSet(true, false)) {
            List<ESSelector> toClose = Stream.concat(acceptors.stream(), socketSelectors.stream()).collect(Collectors.toList());
            List<IOException> closingExceptions = new ArrayList<>();
            for (ESSelector selector : toClose) {
                try {
                    selector.close();
                } catch (IOException e) {
                    closingExceptions.add(e);
                }
            }
            ExceptionsHelper.rethrowAndSuppress(closingExceptions);
        }
    }

    private static <S extends ESSelector> void startSelectors(Iterable<S> selectors, ThreadFactory threadFactory) {
        for (ESSelector acceptor : selectors) {
            if (acceptor.isRunning() == false) {
                threadFactory.newThread(acceptor::runLoop).start();
                try {
                    acceptor.isRunningFuture().get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for selector to start.", e);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                    } else {
                        throw new RuntimeException("Exception during selector start.", e);
                    }
                }
            }
        }
    }

    private void ensureOpen() {
        if (isOpen.get() == false) {
            throw new IllegalStateException("NioGroup is closed.");
        }
    }
}
