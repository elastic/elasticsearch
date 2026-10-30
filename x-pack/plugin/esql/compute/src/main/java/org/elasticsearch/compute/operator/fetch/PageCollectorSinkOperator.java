/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fetch;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.core.Releasable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;

/**
 * The sink of a fetch driver, the driver that loads one shard. The response of a fetch returns the rows of each shard in
 * the order the request listed them, so the sink keeps its shard's pages together and in order.
 */
public final class PageCollectorSinkOperator extends SinkOperator {
    /**
     * The pages of every shard of one fetch request. Each shard's sink fills its own slot, and the request reads the
     * slots once every driver is done.
     */
    public static final class PageCollector implements Releasable {
        private final List<List<Page>> pagesByShard;
        /**
         * Set before {@link #close} empties the slots, and read under the lock of a slot. A sink that adds after it
         * releases its page, because nobody will take it.
         */
        private volatile boolean closed;

        public PageCollector(int shardCount) {
            pagesByShard = new ArrayList<>(shardCount);
            for (int s = 0; s < shardCount; s++) {
                pagesByShard.add(new ArrayList<>());
            }
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
         * The rows the pages of {@code shard} hold.
         */
        public int rows(int shard) {
            List<Page> pages = pagesByShard.get(shard);
            synchronized (pages) {
                int rows = 0;
                for (Page page : pages) {
                    rows += page.getPositionCount();
                }
                return rows;
            }
        }

        /**
         * Hands the pages of {@code shard}, in order, to the caller, which releases them.
         */
        public List<Page> take(int shard) {
            List<Page> pages = pagesByShard.get(shard);
            synchronized (pages) {
                List<Page> taken = new ArrayList<>(pages);
                pages.clear();
                return taken;
            }
        }

        /**
         * Releases the pages nobody took, for example after a failure, and every page a sink adds later.
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

    /**
     * @param shardOf the shard the driver of a {@link DriverContext} loads. The source of the driver claims it before
     *                the sink is built.
     */
    public record Factory(PageCollector collector, ToIntFunction<DriverContext> shardOf) implements SinkOperatorFactory {
        @Override
        public SinkOperator get(DriverContext driverContext) {
            return new PageCollectorSinkOperator(collector, shardOf.applyAsInt(driverContext));
        }

        @Override
        public String describe() {
            return "PageCollectorSinkOperator";
        }
    }

    private final PageCollector collector;
    private final int shard;
    private boolean finished;

    public PageCollectorSinkOperator(PageCollector collector, int shard) {
        this.collector = collector;
        this.shard = shard;
    }

    @Override
    protected void doAddInput(Page page) {
        // the page already belongs to no driver, so the request can send it from any thread
        collector.add(shard, page);
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "PageCollectorSinkOperator[shard=" + shard + "]";
    }
}
