/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Strategy for readers that have no native row-position channel and must surface {@code _rowPosition}
 * as a NULL column (parquet-rs today; the Rust bridge does not yet expose row positions). The
 * reader strips {@code _rowPosition} from its native projection so the inner iterator's pages do
 * not carry the column at all; {@link #apply} wraps the iterator and splices a constant-null
 * {@link org.elasticsearch.compute.data.LongBlock} at the slot the user's projection requested.
 * <p>
 * The downstream {@code VirtualColumnIterator} composes {@code _id} from the row-position channel;
 * a null splice yields null {@code _file.record_ref} and null {@code _id}, matching the documented
 * "row-position unsupported on this reader" semantics. The {@link #reason()} string carries the
 * explanation the wrapping iterator embeds in its {@code describe()} output.
 */
public final class NullSpliceRowPositionStrategy implements RowPositionStrategy {

    private final BlockFactory blockFactory;
    private final String reason;

    /**
     * @param blockFactory used to allocate the per-page null block; carry the same factory the
     *                     reader uses for its inner iterator so block ownership and breaker
     *                     accounting stay coherent.
     * @param reason       user-facing reason this reader cannot supply a real row position
     *                     (surfaced in {@link #reason()} and in iterator {@code describe()}).
     */
    public NullSpliceRowPositionStrategy(BlockFactory blockFactory, String reason) {
        this.blockFactory = Objects.requireNonNull(blockFactory, "blockFactory");
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    @Override
    public String reason() {
        return reason;
    }

    @Override
    public CloseableIterator<Page> apply(CloseableIterator<Page> inner, int rowPositionSlot) {
        if (rowPositionSlot < 0) {
            // The optimizer did not inject _rowPosition for this read. The reader's inner iterator
            // is already in the correct shape; no wrapping needed.
            return inner;
        }
        return new NullSplicingIterator(inner, rowPositionSlot, blockFactory, reason);
    }

    /**
     * Splices an all-null {@link org.elasticsearch.compute.data.LongBlock} into every page at the
     * {@code _rowPosition} output slot.
     */
    private static final class NullSplicingIterator implements CloseableIterator<Page>, Describable {
        private final CloseableIterator<Page> inner;
        private final int rowPosSlot;
        private final BlockFactory blockFactory;
        private final String reason;

        NullSplicingIterator(CloseableIterator<Page> inner, int rowPosSlot, BlockFactory blockFactory, String reason) {
            this.inner = inner;
            this.rowPosSlot = rowPosSlot;
            this.blockFactory = blockFactory;
            this.reason = reason;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        /**
         * Forward the async-ready signal so the producer-loop drain parks (rather than blocking in a
         * parser-backed {@code hasNext()}) across the inter-chunk gap. Without this the default
         * immediately-done {@link CloseableIterator#waitForReady()} would swallow the inner signal —
         * the multi-file text-read deadlock.
         */
        @Override
        public SubscribableListener<Void> waitForReady() {
            return inner.waitForReady();
        }

        @Override
        public Page tryAdvance() {
            Page innerPage = inner.tryAdvance();
            return innerPage != null ? splicePage(innerPage) : null;
        }

        @Override
        public Page next() {
            if (inner.hasNext() == false) {
                throw new NoSuchElementException();
            }
            return splicePage(inner.next());
        }

        private Page splicePage(Page innerPage) {
            int positions = innerPage.getPositionCount();
            int innerBlockCount = innerPage.getBlockCount();
            assert rowPosSlot <= innerBlockCount : "rowPosSlot " + rowPosSlot + " past inner block count " + innerBlockCount;
            Block[] blocks = new Block[innerBlockCount + 1];
            boolean success = false;
            Block nullBlock = null;
            try {
                nullBlock = blockFactory.newConstantNullBlock(positions);
                for (int i = 0; i < rowPosSlot; i++) {
                    blocks[i] = innerPage.getBlock(i);
                }
                blocks[rowPosSlot] = nullBlock;
                for (int i = rowPosSlot; i < innerBlockCount; i++) {
                    blocks[i + 1] = innerPage.getBlock(i);
                }
                Page out = new Page(positions, blocks);
                success = true;
                return out;
            } finally {
                if (success == false) {
                    if (nullBlock != null) {
                        nullBlock.close();
                    }
                    innerPage.releaseBlocks();
                }
            }
        }

        @Override
        public void close() throws IOException {
            inner.close();
        }

        @Override
        public String describe() {
            return inner instanceof Describable d
                ? d.describe() + "(+nullSpliceRowPosition: " + reason + ")"
                : "nullSpliceRowPosition: " + reason;
        }
    }
}
