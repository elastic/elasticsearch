/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator.exchange;

import org.elasticsearch.xpack.sql.action.compute.planner.PlanNode.ExchangeNode.Partitioning;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Helper class to set up local exchanges. Avoids having to manually create sources, sinks and the respective operators.
 */
public class Exchange {
    private boolean allSourcesFinished;

    private final ExchangeMemoryManager memoryManager;
    private final Supplier<Exchanger> exchangerSupplier;

    private final List<ExchangeSource> sources = new ArrayList<>();
    private final Set<ExchangeSink> sinks = new HashSet<>();

    private int nextSourceIndex;

    public Exchange(int defaultConcurrency, Partitioning partitioning, int bufferMaxPages) {
        int bufferCount = partitioning == Partitioning.SINGLE_DISTRIBUTION ? 1 : defaultConcurrency;
        for (int i = 0; i < bufferCount; i++) {
            sources.add(new ExchangeSource(source -> checkAllSourcesFinished()));
        }
        List<Consumer<ExchangeSource.PageReference>> buffers = this.sources.stream()
            .map(buffer -> (Consumer<ExchangeSource.PageReference>) buffer::addPage)
            .collect(Collectors.toList());

        memoryManager = new ExchangeMemoryManager(bufferMaxPages);

        if (partitioning == Partitioning.SINGLE_DISTRIBUTION || partitioning == Partitioning.FIXED_BROADCAST_DISTRIBUTION) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager);
        } else if (partitioning == Partitioning.FIXED_PASSTHROUGH_DISTRIBUTION) {
            Iterator<ExchangeSource> sourceIterator = this.sources.iterator();
            // TODO: fairly partition memory usage over sources
            exchangerSupplier = () -> new PassthroughExchanger(sourceIterator.next(), memoryManager);
        } else if (partitioning == Partitioning.FIXED_ARBITRARY_DISTRIBUTION) {
            exchangerSupplier = () -> new RandomExchanger(buffers, memoryManager);
        } else {
            throw new UnsupportedOperationException(partitioning.toString());
        }
    }

    private void checkAllSourcesFinished() {
        if (sources.stream().allMatch(ExchangeSource::isFinished) == false) {
            return;
        }

        List<ExchangeSink> openSinks;
        synchronized (this) {
            allSourcesFinished = true;

            openSinks = new ArrayList<>(sinks);
            sinks.clear();
        }

        openSinks.forEach(ExchangeSink::finish);
        checkAllSinksComplete();
    }

    public ExchangeSink createSink() {
        synchronized (this) {
            if (allSourcesFinished) {
                return ExchangeSink.finishedExchangeSink();
            }
            Exchanger exchanger = exchangerSupplier.get();
            ExchangeSink exchangeSink = new ExchangeSink(exchanger, this::sinkFinished);
            sinks.add(exchangeSink);
            return exchangeSink;
        }
    }

    private void sinkFinished(ExchangeSink exchangeSink) {
        synchronized (this) {
            sinks.remove(exchangeSink);
        }
        checkAllSinksComplete();
    }

    private void checkAllSinksComplete() {
        synchronized (this) {
            if (sinks.isEmpty() == false) {
                return;
            }
        }

        sources.forEach(ExchangeSource::finish);
    }

    public ExchangeSource getNextSource() {
        ExchangeSource result = sources.get(nextSourceIndex);
        nextSourceIndex++;
        return result;
    }
}
