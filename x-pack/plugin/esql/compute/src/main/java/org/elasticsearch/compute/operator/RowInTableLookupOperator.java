/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Looks up each row of an input {@link Page} in a pre-built table and appends
 * the matching row index as a new {@link IntBlock}.
 * <p>
 *     The table is supplied at construction time as an array of {@link Key}s,
 *     each holding one key column. Input pages may have more columns than just
 *     the key columns; {@code blockMapping} selects which blocks in the input
 *     correspond to each key, in order.
 * </p>
 * <p>
 *     For example, given a lookup table keyed on {@code class} with a
 *     non-key column {@code meaning}:
 * </p>
 * {@snippet lang="txt" :
 * ┌──────────┬───────────────────────────┐
 * │ class    │ meaning                   │
 * ├──────────┼───────────────────────────┤
 * │ Euclid   │ unpredictable behavior    │
 * │ Keter    │ extremely hard to contain │
 * │ Safe     │ safely contained          │
 * └──────────┴───────────────────────────┘
 * }
 * <p>
 *     The {@link Key} passed to this operator contains only the {@code class}
 *     column — {@code meaning} is stored separately and fetched by a downstream
 *     operator using the row index this operator emits. Given an input page:
 * </p>
 * {@snippet lang="txt" :
 * ┌─────┬─────┬──────────┐
 * │ doc │ ref │ class    │
 * ├─────┼─────┼──────────┤
 * │   0 │ 049 │ Euclid   │
 * │   1 │ 682 │ Keter    │
 * │   2 │ 179 │ Thaumiel │
 * └─────┴─────┴──────────┘
 * }
 * <p>
 *     With {@code blockMapping = [2]} ({@code class} at block 2 is the only
 *     key), the operator produces:
 * </p>
 * {@snippet lang="txt" :
 * ┌─────┬─────┬──────────┬───────────┐
 * │ doc │ ref │ class    │ row index │
 * ├─────┼─────┼──────────┼───────────┤
 * │   0 │ 049 │ Euclid   │         0 │  ← meaning: unpredictable behavior
 * │   1 │ 682 │ Keter    │         1 │  ← meaning: extremely hard to contain
 * │   2 │ 179 │ Thaumiel │      null │  ← no match: Thaumiel not in table
 * └─────┴─────┴──────────┴───────────┘
 * }
 * <p>
 *     The {@link Factory} holds the key {@link Block}s and is shared across
 *     all drivers. Every operator it creates probes one shared table, so the
 *     factory's blocks are never released by this operator. The first
 *     {@link Factory#get} builds that table.
 * </p>
 */
public class RowInTableLookupOperator extends AbstractPageMappingToIteratorOperator {
    /**
     * A single key column of the lookup table: its field name and the block
     * of values (one value per table row).
     */
    public record Key(String name, Block block) {
        @Override
        public String toString() {
            return "{name="
                + name
                + ", type="
                + block.elementType()
                + ", positions="
                + block.getPositionCount()
                + ", size="
                + ByteSizeValue.ofBytes(block.ramBytesUsed())
                + "}";
        }
    }

    /**
     * Factory for {@link RowInTableLookupOperator}. The {@link Block}s inside
     * the {@link Key}s are never closed by this factory. Every operator it creates
     * shares one table: the first {@link #get} builds it and the last operator to
     * close releases it. A table per driver would multiply the build time and the
     * memory by the number of drivers, which matters once the table holds hundreds
     * of thousands of keys.
     */
    public static final class Factory implements Operator.OperatorFactory {
        private final Key[] keys;
        private final int[] blockMapping;
        /**
         * The table shared by the operators this factory created, or {@code null} before the first {@link #get}.
         * Guarded by {@code this}.
         */
        private SharedLookup shared;

        public Factory(Key[] keys, int[] blockMapping) {
            if (keys.length < 1) {
                throw new IllegalArgumentException("expected [keys] to be non-empty");
            }
            this.keys = keys;
            this.blockMapping = blockMapping;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            /*
             * Build with the node-wide parent factory, which charges the request breaker, rather than this driver's:
             * the table outlives any single driver and is released from whichever driver closes last, so it must not
             * be charged to a driver-local breaker.
             */
            SharedLookup shared = acquire(driverContext.blockFactory().parent());
            return new RowInTableLookupOperator(driverContext.blockFactory(), keyNames(keys), shared, blockMapping);
        }

        /**
         * Returns the shared table with a reference taken for the caller, building it if no live one exists.
         */
        private synchronized SharedLookup acquire(BlockFactory blockFactory) {
            if (shared == null || shared.tryIncRef() == false) {
                shared = new SharedLookup(buildLookup(blockFactory, keys));
            }
            return shared;
        }

        @Override
        public String describe() {
            return "RowInTableLookup[keys=" + Arrays.toString(keys) + ", mapping=" + Arrays.toString(blockMapping) + "]";
        }
    }

    /**
     * A table and the count of operators using it. The last of them to release it closes the table.
     */
    private static final class SharedLookup extends AbstractRefCounted {
        private final RowInTableLookup table;

        private SharedLookup(RowInTableLookup table) {
            this.table = table;
        }

        @Override
        protected void closeInternal() {
            table.close();
        }
    }

    static RowInTableLookup buildLookup(BlockFactory blockFactory, Key[] keys) {
        if (keys.length < 1) {
            throw new IllegalArgumentException("expected [keys] to be non-empty");
        }
        /*
         * The key blocks belong to the plan, which other threads may be using too, and building may incRef and decRef
         * its input (see BatchEncoder.DirectEncoder). Those ref counts are not thread-safe, so build from a view that
         * owns its own ref count instead of the plan's block. A vector-backed key gets a fresh wrapper over the same
         * values, which is free. Any other key is copied; the copy is only needed while building.
         */
        Block[] views = new Block[keys.length];
        Block[] copies = new Block[keys.length];
        try {
            for (int k = 0; k < keys.length; k++) {
                Vector vector = keys[k].block.asVector();
                if (vector != null) {
                    views[k] = vector.asBlock();
                } else {
                    copies[k] = keys[k].block.deepCopy(blockFactory);
                    views[k] = copies[k];
                }
            }
            return RowInTableLookup.build(blockFactory, views);
        } finally {
            // Only the copies are released. Closing a wrapper would release the shared vector it wraps.
            Releasables.closeExpectNoException(copies);
        }
    }

    private static List<String> keyNames(Key[] keys) {
        List<String> names = new ArrayList<>(keys.length);
        for (Key key : keys) {
            names.add(key.name);
        }
        return names;
    }

    /**
     * The driver's factory, for the results of each lookup.
     */
    private final BlockFactory blockFactory;
    private final List<String> keys;
    private final SharedLookup shared;
    private final Releasable releaseShared;
    private final int[] blockMapping;

    /**
     * Builds a table used by this operator alone.
     */
    public RowInTableLookupOperator(BlockFactory blockFactory, Key[] keys, int[] blockMapping) {
        this(blockFactory, keyNames(keys), new SharedLookup(buildLookup(blockFactory, keys)), blockMapping);
    }

    /**
     * Takes ownership of one reference to {@code shared}.
     */
    private RowInTableLookupOperator(BlockFactory blockFactory, List<String> keys, SharedLookup shared, int[] blockMapping) {
        this.blockFactory = blockFactory;
        this.keys = keys;
        this.shared = shared;
        this.releaseShared = Releasables.releaseOnce(shared::decRef);
        this.blockMapping = blockMapping;
    }

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        Page mapped = page.projectBlocks(blockMapping);
        try {
            // lookup increments any references we need to keep for the iterator
            return appendBlocks(page, shared.table.lookup(mapped, blockFactory, BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE));
        } finally {
            mapped.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return "RowInTableLookup[" + shared.table + ", keys=" + keys + ", mapping=" + Arrays.toString(blockMapping) + "]";
    }

    @Override
    public void close() {
        Releasables.close(super::close, releaseShared);
    }
}
