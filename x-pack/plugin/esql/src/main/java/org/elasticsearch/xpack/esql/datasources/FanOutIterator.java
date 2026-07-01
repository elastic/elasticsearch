/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;

import java.util.NoSuchElementException;

/**
 * Materializes {@code copy_to} copies immediately after the reader, at the {@code applyRowPositionStrategy} seam that
 * every read path shares. The {@code delegate} produces pages of the DISTINCT base columns (one block per physical
 * column — see {@link PhysicalNames#fanOut}); this iterator restores the full projected shape by pointing each output
 * column at its base block through the {@link PhysicalNames.FanOut} {@code index}, sharing the block zero-copy with
 * {@code incRef} (the same idiom as {@link ColumnMapping#mapPage}). A copy target and its source column therefore share
 * one physical read and one {@link Block}.
 *
 * <p>Running here — before {@code adaptSchema}/{@link ColumnMapping} and the synthetic-column handling — restores the
 * page to {@code perFileCols} shape, so all of that downstream machinery is untouched by copy. The factory only wraps
 * the stream when {@code index} is non-identity (a copy is present); a pure move/rename passes through unwrapped, so the
 * shipped path is bit-identical.
 *
 * <p>Refcount contract (mirrors {@link ColumnMapping#mapPage}): each output slot takes its own {@code incRef} on the
 * shared base block, and {@link Page#releaseBlocks} closes per slot (it does not dedup by identity), so a block fanned
 * to N slots is incremented N times and released N times — balanced, no double-free, no leak.
 */
final class FanOutIterator implements CloseableIterator<Page> {

    private final CloseableIterator<Page> delegate;
    private final int[] index; // output column -> base-page block position

    FanOutIterator(CloseableIterator<Page> delegate, int[] index) {
        this.delegate = delegate;
        this.index = index;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Page next() {
        if (delegate.hasNext() == false) {
            throw new NoSuchElementException();
        }
        Page base = delegate.next();
        int positions = base.getPositionCount();
        Block[] blocks = new Block[index.length];
        try {
            for (int i = 0; i < index.length; i++) {
                Block source = base.getBlock(index[i]);
                source.incRef();
                blocks[i] = source;
            }
            return new Page(positions, blocks);
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw new RuntimeException("Failed to fan out copy columns", e);
        } finally {
            base.releaseBlocks();
        }
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close delegate iterator", e);
        }
    }
}
