/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
 * The NioSelectorGroup is a group of selectors for interfacing with java nio. When it is started it will create the
 * configured number of selectors. Each selector will be running in a dedicated thread. Server connections
 * can be bound using the {@link #bindServerChannel(InetSocketAddress, ChannelFactory)} method. Client
 * connections can be opened using the {@link #openChannel(InetSocketAddress, ChannelFactory)} method.
 * <p>
 * The logic specific to a particular channel is provided by the {@link ChannelFactory} passed to the method
 * when the channel is created. This is what allows an NioSelectorGroup to support different channel types.
 */
public class NioSelectorGroup implements NioGroup {


    private final List<NioSelector> dedicatedAcceptors;
    private final RoundRobinSupplier<NioSelector> acceptorSupplier;

    private final List<NioSelector> selectors;
    private final RoundRobinSupplier<NioSelector> selectorSupplier;

    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    /**
     * This will create an NioSelectorGroup with no dedicated acceptors. All server channels will be handled by the
     * same selectors that are handling child channels.
     *
     * @param threadFactory factory to create selector threads
     * @param selectorCount the number of selectors to be created
     * @param eventHandlerFunction function for creating event handlers
     * @throws IOException occurs if there is a problem while opening a java.nio.Selector
     */
    public NioSelectorGroup(ThreadFactory threadFactory, int selectorCount,
                            Function<Supplier<NioSelector>, EventHandler> eventHandlerFunction) throws IOException {
        this(null, 0, threadFactory, selectorCount, eventHandlerFunction);
    }

    /**
     * This will create an NioSelectorGroup with dedicated acceptors. All server channels will be handled by a group
     * of selectors dedicated to accepting channels. These accepted channels will be handed off the
     * non-server selectors.
     *
     * @param acceptorThreadFactory factory to create acceptor selector threads
     * @param dedicatedAcceptorCount the number of dedicated acceptor selectors to be created
     * @param selectorThreadFactory factory to create non-acceptor selector threads
     * @param selectorCount the number of non-acceptor selectors to be created
     * @param eventHandlerFunction function for creating event handlers
     * @throws IOException occurs if there is a problem while opening a java.nio.Selector
     */
    public NioSelectorGroup(ThreadFactory acceptorThreadFactory, int dedicatedAcceptorCount, ThreadFactory selectorThreadFactory,
                            int selectorCount, Function<Supplier<NioSelector>, EventHandler> eventHandlerFunction) throws IOException {
        dedicatedAcceptors = new ArrayList<>(dedicatedAcceptorCount);
        selectors = new ArrayList<>(selectorCount);

        try {
            List<RoundRobinSupplier<NioSelector>> suppliersToSet = new ArrayList<>(selectorCount);
            for (int i = 0; i < selectorCount; ++i) {
                RoundRobinSupplier<NioSelector> supplier = new RoundRobinSupplier<>();
                suppliersToSet.add(supplier);
                NioSelector selector = new NioSelector(eventHandlerFunction.apply(supplier));
                selectors.add(selector);
            }
            for (RoundRobinSupplier<NioSelector> supplierToSet : suppliersToSet) {
                supplierToSet.setSelectors(selectors.toArray(new NioSelector[0]));
                assert supplierToSet.count() == selectors.size() : "Supplier should have same count as selector list.";
            }

            for (int i = 0; i < dedicatedAcceptorCount; ++i) {
                RoundRobinSupplier<NioSelector> supplier = new RoundRobinSupplier<>(selectors.toArray(new NioSelector[0]));
                NioSelector acceptor = new NioSelector(eventHandlerFunction.apply(supplier));
                dedicatedAcceptors.add(acceptor);
            }

            if (dedicatedAcceptorCount != 0) {
                acceptorSupplier = new RoundRobinSupplier<>(dedicatedAcceptors.toArray(new NioSelector[0]));
            } else {
                acceptorSupplier = new RoundRobinSupplier<>(selectors.toArray(new NioSelector[0]));
            }
            selectorSupplier = new RoundRobinSupplier<>(selectors.toArray(new NioSelector[0]));
            assert selectorCount == selectors.size() : "We need to have created all the selectors at this point.";
            assert dedicatedAcceptorCount == dedicatedAcceptors.size() : "We need to have created all the acceptors at this point.";

            startSelectors(selectors, selectorThreadFactory);
            startSelectors(dedicatedAcceptors, acceptorThreadFactory);
        } catch (Exception e) {
            try {
                close();
            } catch (Exception e1) {
                e.addSuppressed(e1);
            }
            throw e;
        }
    }

    @Override
    public <S extends NioServerSocketChannel> S bindServerChannel(InetSocketAddress address, ChannelFactory<S, ?> factory)
        throws IOException {
        ensureOpen();
        return factory.openNioServerSocketChannel(address, acceptorSupplier);
    }

    @Override
    public <S extends NioSocketChannel> S openChannel(InetSocketAddress address, ChannelFactory<?, S> factory) throws IOException {
        ensureOpen();
        return factory.openNioChannel(address, selectorSupplier);
    }

    @Override
    public void close() throws IOException {
        if (isOpen.compareAndSet(true, false)) {
            List<NioSelector> toClose = Stream.concat(dedicatedAcceptors.stream(), selectors.stream()).collect(Collectors.toList());
            List<IOException> closingExceptions = new ArrayList<>();
            for (NioSelector selector : toClose) {
                try {
                    selector.close();
                } catch (IOException e) {
                    closingExceptions.add(e);
                }
            }
            ExceptionsHelper.rethrowAndSuppress(closingExceptions);
        }
    }

    private static void startSelectors(Iterable<NioSelector> selectors, ThreadFactory threadFactory) {
        for (NioSelector selector : selectors) {
            if (selector.isRunning() == false) {
                threadFactory.newThread(selector::runLoop).start();
                try {
                    selector.isRunningFuture().get();
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
