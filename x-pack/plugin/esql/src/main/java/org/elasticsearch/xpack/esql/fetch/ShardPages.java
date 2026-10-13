/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.core.Releasable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * The pages that each shard of one fetch request loaded. The response returns the rows of each shard in the order the
 * request listed them, so the pages of a shard stay together and in order. The driver of each shard fills the slot of its
 * shard through an {@link OutputOperator}, and the request takes the slots once every driver is done.
 */
final class ShardPages implements Releasable {
    private final List<List<Page>> pagesByShard;
    /**
     * Set before {@link #close} empties the slots, and read under the lock of a slot. A driver that adds a page after that
     * releases it, because nobody will take it.
     */
    private volatile boolean closed;

    ShardPages(int shardCount) {
        pagesByShard = new ArrayList<>(shardCount);
        for (int s = 0; s < shardCount; s++) {
            pagesByShard.add(new ArrayList<>());
        }
    }

    /**
     * The sink of each driver: an {@link OutputOperator} that fills the slot of the shard the driver loads.
     *
     * @param shardOf the shard the driver of a {@link DriverContext} loads. The source of the driver claims it before the
     *                sink is built.
     */
    SinkOperator.SinkOperatorFactory sinks(ToIntFunction<DriverContext> shardOf) {
        return new SinkOperator.SinkOperatorFactory() {
            @Override
            public SinkOperator get(DriverContext driverContext) {
                int shard = shardOf.applyAsInt(driverContext);
                // the sink already let the page go from its driver, so the request can send it from any thread
                return new OutputOperator(List.of(), Function.identity(), page -> add(shard, page));
            }

            @Override
            public String describe() {
                return "OutputOperator[pages of its shard]";
            }
        };
    }

    private void add(int shard, Page page) {
        List<Page> pages = pagesByShard.get(shard);
        synchronized (pages) {
            if (closed == false) {
                pages.add(page);
                return;
            }
        }
        page.releaseBlocks();
    }

    /**
     * Hands the pages of {@code shard}, in order, to the caller, which releases them.
     */
    List<Page> take(int shard) {
        List<Page> pages = pagesByShard.get(shard);
        synchronized (pages) {
            List<Page> taken = new ArrayList<>(pages);
            pages.clear();
            return taken;
        }
    }

    /**
     * Releases the pages nobody took, for example after a failure, and every page a driver adds later.
     */
    @Override
    public void close() {
        closed = true;
        for (int s = 0; s < pagesByShard.size(); s++) {
            for (Page page : take(s)) {
                page.releaseBlocks();
            }
        }
    }
}
