/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.support.SubscribableListener;
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
 *     factory's blocks are never released by this operator. The first operator
 *     to receive input builds that table on its own driver thread. Until it is
 *     built the other operators hold up to {@link #MAX_PENDING_BYTES} of input,
 *     so their drivers keep scanning and loading keys during the build instead
 *     of idling, and then block until the table is ready.
 * </p>
 */
public class RowInTableLookupOperator implements Operator {
    /**
     * Bytes of input one operator holds while another driver builds the shared table. A few pages are enough
     * to keep the upstream operators busy for the length of a build without holding much of the input. The
     * held pages are already charged to the breaker, so this bounds how long they stay alive, not their cost.
     */
    static final long MAX_PENDING_BYTES = ByteSizeValue.ofMb(1).getBytes();

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
     * shares one table: the first of them to receive input builds it and the last
     * to close releases it. A table per driver would multiply the build time and the
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
             * Build with the query-wide parent factory rather than this driver's: the table outlives any single
             * driver and is released from whichever driver closes last, so it must not be charged to a
             * driver-local breaker.
             */
            SharedLookup shared = acquire(driverContext.blockFactory().parent());
            return new RowInTableLookupOperator(keyNames(keys), shared, blockMapping);
        }

        /**
         * Returns the shared table with a reference taken for the caller, creating an unbuilt one if no live one exists.
         */
        private synchronized SharedLookup acquire(BlockFactory blockFactory) {
            if (shared == null || shared.tryIncRef() == false) {
                shared = new SharedLookup(blockFactory, keys);
            }
            return shared;
        }

        @Override
        public String describe() {
            return "RowInTableLookup[keys=" + Arrays.toString(keys) + ", mapping=" + Arrays.toString(blockMapping) + "]";
        }
    }

    /**
     * A table and the count of operators using it. The first operator to receive input builds it and the others
     * wait on {@link #built}. Building on a driver thread rather than a separate task ties the build to the
     * query's lifetime, so the key blocks it reads cannot be released under it. The table is closed when the last
     * operator releases it, which cannot happen mid-build because the building operator holds a reference.
     */
    private static final class SharedLookup extends AbstractRefCounted {
        private final BlockFactory blockFactory;
        private final Key[] keys;
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final SubscribableListener<Void> built = new SubscribableListener<>();
        /**
         * Written before {@link #built} completes and read only after, so completing the listener publishes them.
         */
        private RowInTableLookup table;
        private Exception failure;

        private SharedLookup(BlockFactory blockFactory, Key[] keys) {
            this.blockFactory = blockFactory;
            this.keys = keys;
        }

        /**
         * Builds the table unless another caller has claimed the build. Rethrows a failure of the build this
         * thread did.
         */
        void buildIfUnclaimed() {
            if (built.isDone() || claimed.compareAndSet(false, true) == false) {
                return;
            }
            RowInTableLookup table;
            try {
                table = buildLookup(blockFactory, keys);
            } catch (Exception e) {
                onBuildFailure(e);
                throw e;
            } catch (Error e) {
                // Wake the waiting operators rather than leave them blocked forever.
                onBuildFailure(new IllegalStateException("failed to build the lookup table", e));
                throw e;
            }
            this.table = table;
            built.onResponse(null);
        }

        private void onBuildFailure(Exception e) {
            failure = e;
            built.onFailure(e);
        }

        boolean isBuilt() {
            return built.isDone();
        }

        /**
         * The built table. Rethrows the build failure, if any, so every operator fails the way the builder did.
         */
        RowInTableLookup table() {
            assert isBuilt() : "table is not built yet";
            if (failure != null) {
                throw failure instanceof RuntimeException re ? re : new IllegalStateException(failure);
            }
            return table;
        }

        @Override
        public String toString() {
            if (isBuilt() == false) {
                return "unbuilt";
            }
            return failure == null ? table.toString() : "failed";
        }

        @Override
        protected void closeInternal() {
            Releasables.closeExpectNoException(table);
        }
    }

    static RowInTableLookup buildLookup(BlockFactory blockFactory, Key[] keys) {
        if (keys.length < 1) {
            throw new IllegalArgumentException("expected [keys] to be non-empty");
        }
        /*
         * The key blocks can be shared with other plans running on other threads, and building may incRef and decRef
         * its input (see BatchEncoder.DirectEncoder). Those ref counts are not thread-safe, so build from a view that
         * owns its own ref count instead of the shared block. A vector-backed key gets a fresh wrapper over the same
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

    private final List<String> keys;
    private final SharedLookup shared;
    private final Releasable releaseShared;
    private final int[] blockMapping;
    private final Mapping mapping = new Mapping();
    /**
     * Input received before the table was built, in arrival order.
     */
    private final Deque<Page> pending = new ArrayDeque<>();
    private long pendingBytes;
    private boolean finished;

    /**
     * Builds a table used by this operator alone.
     */
    public RowInTableLookupOperator(BlockFactory blockFactory, Key[] keys, int[] blockMapping) {
        this(keyNames(keys), buildNow(blockFactory, keys), blockMapping);
    }

    private static SharedLookup buildNow(BlockFactory blockFactory, Key[] keys) {
        SharedLookup shared = new SharedLookup(blockFactory, keys);
        shared.buildIfUnclaimed();
        return shared;
    }

    /**
     * Takes ownership of one reference to {@code shared}.
     */
    private RowInTableLookupOperator(List<String> keys, SharedLookup shared, int[] blockMapping) {
        this.keys = keys;
        this.shared = shared;
        this.releaseShared = Releasables.releaseOnce(shared::decRef);
        this.blockMapping = blockMapping;
    }

    @Override
    public boolean needsInput() {
        if (finished) {
            return false;
        }
        if (shared.isBuilt() == false) {
            return pendingBytes < MAX_PENDING_BYTES;
        }
        return pending.isEmpty() && mapping.needsInput();
    }

    @Override
    public void addInput(Page page) {
        if (shared.isBuilt() == false) {
            try {
                shared.buildIfUnclaimed();
            } catch (Exception e) {
                page.releaseBlocks();
                throw e;
            }
        }
        if (shared.isBuilt() && pending.isEmpty()) {
            mapping.addInput(page);
            return;
        }
        pending.add(page);
        pendingBytes += page.ramBytesUsedByBlocks();
    }

    /**
     * Moves held input into the mapping once the table is built, and finishes the mapping once nothing is held.
     */
    private void drainPending() {
        if (shared.isBuilt() == false) {
            shared.buildIfUnclaimed();
            if (shared.isBuilt() == false) {
                return;
            }
        }
        while (pending.isEmpty() == false && mapping.needsInput()) {
            Page page = pending.poll();
            pendingBytes -= page.ramBytesUsedByBlocks();
            mapping.addInput(page);
        }
        if (finished && pending.isEmpty()) {
            mapping.finish();
        }
    }

    @Override
    public Page getOutput() {
        drainPending();
        return mapping.getOutput();
    }

    @Override
    public void finish() {
        finished = true;
        if (pending.isEmpty()) {
            mapping.finish();
        }
    }

    @Override
    public boolean isFinished() {
        return finished && pending.isEmpty() && mapping.isFinished();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return shared.isBuilt() && (pending.isEmpty() == false || mapping.canProduceMoreDataWithoutExtraInput());
    }

    /**
     * Blocked while another driver builds the table and this one can't hold more input: either the held input
     * reached {@link #MAX_PENDING_BYTES} or no more input is coming. Below the limit it stays unblocked, so the
     * driver keeps moving pages through the upstream operators.
     */
    @Override
    public IsBlockedResult isBlocked() {
        if (shared.isBuilt() || pending.isEmpty()) {
            return NOT_BLOCKED;
        }
        if (finished || pendingBytes >= MAX_PENDING_BYTES) {
            return new IsBlockedResult(shared.built, "row in table lookup build");
        }
        return NOT_BLOCKED;
    }

    @Override
    public Status status() {
        return mapping.status();
    }

    @Override
    public String toString() {
        return "RowInTableLookup[" + shared + ", keys=" + keys + ", mapping=" + Arrays.toString(blockMapping) + "]";
    }

    @Override
    public void close() {
        Releasables.close(mapping, this::releasePending, releaseShared);
    }

    private void releasePending() {
        Page page;
        while ((page = pending.poll()) != null) {
            page.releaseBlocks();
        }
        pendingBytes = 0;
    }

    /**
     * Appends the matching row positions to each page once the table is built.
     */
    private class Mapping extends AbstractPageMappingToIteratorOperator {
        @Override
        protected ReleasableIterator<Page> receive(Page page) {
            Page mapped = page.projectBlocks(blockMapping);
            try {
                // lookup increments any references we need to keep for the iterator
                return appendBlocks(page, shared.table().lookup(mapped, BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE));
            } finally {
                mapped.releaseBlocks();
            }
        }

        @Override
        public String toString() {
            return RowInTableLookupOperator.this.toString();
        }
    }
}
