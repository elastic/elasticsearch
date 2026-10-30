/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.ReleasableIterator;

/**
 * {@link Block#lookup Looks up} values from a provided {@link Block} and
 * merged them into each {@link Page}.
 */
public class ColumnLoadOperator extends AbstractPageMappingToIteratorOperator {
    public record Values(String name, Block block) {
        @Override
        public String toString() {
            return name + ":" + block.elementType();
        }
    }

    /**
     * Factory for {@link ColumnLoadOperator}. The {@link Block} inside {@link Values}
     * is never closed by this factory. All operators created by one factory read from
     * a single shared copy of it: the first {@link #get} makes the copy and the last
     * operator to close releases it. A copy per driver would multiply the copy time
     * and the memory by the number of drivers, and drivers are created one at a time,
     * so the copies would also delay every driver's start.
     */
    public static final class Factory implements OperatorFactory {
        private final Values values;
        private final int positionsOrd;
        /**
         * The copy shared by the operators this factory created, or {@code null} before the first {@link #get}.
         * Guarded by {@code this}.
         */
        private SharedValues shared;

        public Factory(Values values, int positionsOrd) {
            this.values = values;
            this.positionsOrd = positionsOrd;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            /*
             * Copy with the query-wide parent factory rather than this driver's: the copy outlives any single
             * driver and is released from whichever driver closes last, so it must not be charged to a
             * driver-local breaker.
             */
            return new ColumnLoadOperator(acquire(driverContext.blockFactory().parent()), positionsOrd);
        }

        /**
         * Returns the shared copy with a reference taken for the caller, making it if no live one exists.
         */
        private synchronized SharedValues acquire(BlockFactory blockFactory) {
            if (shared == null || shared.tryIncRef() == false) {
                shared = new SharedValues(values.name, values.block.deepCopy(blockFactory));
            }
            return shared;
        }

        @Override
        public String describe() {
            return "ColumnLoad[values=" + values + ", positions=" + positionsOrd + "]";
        }
    }

    /**
     * A copy of the values and the count of operators using it. The copy is closed when the last operator releases it.
     * <p>
     * {@link Block}s are immutable, so reading the copy from many drivers at once is safe, but their reference counts are
     * not thread-safe. {@link Block#lookup} takes a reference to the block it reads from and the returned iterator
     * releases it on close, so both happen while holding this object's monitor.
     * </p>
     */
    private static final class SharedValues extends AbstractRefCounted {
        private final Values values;

        private SharedValues(String name, Block block) {
            this.values = new Values(name, block);
        }

        synchronized ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
            ReleasableIterator<? extends Block> delegate = values.block.lookup(positions, targetBlockSize);
            return new ReleasableIterator<>() {
                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public Block next() {
                    return delegate.next();
                }

                @Override
                public void close() {
                    synchronized (SharedValues.this) {
                        delegate.close();
                    }
                }
            };
        }

        @Override
        protected void closeInternal() {
            values.block.close();
        }
    }

    private final SharedValues shared;
    private final int positionsOrd;

    private ColumnLoadOperator(SharedValues shared, int positionsOrd) {
        this.shared = shared;
        this.positionsOrd = positionsOrd;
    }

    /**
     * The target size of each loaded block.
     * TODO target the size more intelligently
     */
    static final ByteSizeValue TARGET_BLOCK_SIZE = ByteSizeValue.ofKb(10);

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        return appendBlocks(page, shared.lookup(page.getBlock(positionsOrd), TARGET_BLOCK_SIZE));
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            shared.decRef();
        }
    }

    @Override
    public String toString() {
        return "ColumnLoad[values=" + shared.values + ", positions=" + positionsOrd + "]";
    }
}
