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
 * The pages that the shards of one fetch request loaded, one slot for each piece a shard loads in. The response returns the
 * rows of each shard in the order the request listed them, so the pages of a piece stay together and in order, and the
 * request takes the pieces of a shard one after the other. The driver of each piece fills the slot of its piece through
 * an {@link OutputOperator}, and the request takes the slots once every driver is done.
 */
final class ShardPages implements Releasable {
    private final List<List<Page>> pagesByPiece;
    /**
     * Set before {@link #close} empties the slots, and read under the lock of a slot. A driver that adds a page after that
     * releases it, because nobody will take it.
     */
    private volatile boolean closed;

    ShardPages(int pieceCount) {
        pagesByPiece = new ArrayList<>(pieceCount);
        for (int p = 0; p < pieceCount; p++) {
            pagesByPiece.add(new ArrayList<>());
        }
    }

    /**
     * The sink of each driver: an {@link OutputOperator} that fills the slot of the piece the driver loads.
     *
     * @param pieceOf the piece the driver of a {@link DriverContext} loads. The source of the driver claims it before the
     *                sink is built.
     */
    SinkOperator.SinkOperatorFactory sinks(ToIntFunction<DriverContext> pieceOf) {
        return new SinkOperator.SinkOperatorFactory() {
            @Override
            public SinkOperator get(DriverContext driverContext) {
                int piece = pieceOf.applyAsInt(driverContext);
                // the sink already let the page go from its driver, so the request can send it from any thread
                return new OutputOperator(List.of(), Function.identity(), page -> add(piece, page));
            }

            @Override
            public String describe() {
                return "OutputOperator[pages of its piece]";
            }
        };
    }

    private void add(int piece, Page page) {
        List<Page> pages = pagesByPiece.get(piece);
        synchronized (pages) {
            if (closed == false) {
                pages.add(page);
                return;
            }
        }
        page.releaseBlocks();
    }

    /**
     * Hands the pages of {@code piece}, in order, to the caller, which releases them.
     */
    List<Page> take(int piece) {
        List<Page> pages = pagesByPiece.get(piece);
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
        for (int p = 0; p < pagesByPiece.size(); p++) {
            for (Page page : take(p)) {
                page.releaseBlocks();
            }
        }
    }
}
