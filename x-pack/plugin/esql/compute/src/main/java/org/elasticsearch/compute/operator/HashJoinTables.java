/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Keeps the table {@link RowInTableLookupOperator.Factory} builds and the copies {@link ColumnLoadOperator.Factory}
 * makes for every plan of one request that joins against the same {@link Block}s. A data node runs one plan per batch
 * of shards, each with its own factories, so without this every batch would rebuild the table and copy the values
 * again.
 * <p>
 *     Entries are found by the identity of the blocks they were made from, so plans share them only if they read the
 *     same block instances. Each entry holds a reference to what it keeps, released by {@link #close}, so the memory
 *     stays charged to the breaker until the request finishes rather than until the batch that made it does.
 * </p>
 */
public final class HashJoinTables implements Releasable {
    /**
     * Guarded by {@code this}.
     */
    private final Map<Entry, RefCounted> entries = new HashMap<>();
    /**
     * Guarded by {@code this}.
     */
    private boolean closed;

    /**
     * Returns the {@code type} entry made from {@code blocks} with a reference taken for the caller. If there is none, or
     * {@code reusable} rejects it, returns a new one from {@code make}, which must hold a single reference for the
     * caller. Once closed, new entries are returned but not kept.
     */
    synchronized <T extends RefCounted> T acquire(Class<T> type, Block[] blocks, Predicate<T> reusable, Supplier<T> make) {
        Entry entry = new Entry(type, blocks.clone());
        RefCounted kept = entries.get(entry);
        if (kept != null) {
            T existing = type.cast(kept);
            if (reusable.test(existing)) {
                existing.mustIncRef();
                return existing;
            }
            entries.remove(entry);
            existing.decRef();
        }
        T made = make.get();
        if (closed == false) {
            made.mustIncRef();
            entries.put(entry, made);
        }
        return made;
    }

    @Override
    public synchronized void close() {
        closed = true;
        for (RefCounted kept : entries.values()) {
            kept.decRef();
        }
        entries.clear();
    }

    /**
     * Compares blocks by identity: equal values in different blocks are different tables, and comparing by value would
     * read every position.
     */
    private record Entry(Class<?> type, Block[] blocks) {
        @Override
        public boolean equals(Object o) {
            if (o instanceof Entry == false) {
                return false;
            }
            Entry other = (Entry) o;
            if (type != other.type || blocks.length != other.blocks.length) {
                return false;
            }
            for (int i = 0; i < blocks.length; i++) {
                if (blocks[i] != other.blocks[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = type.hashCode();
            for (Block block : blocks) {
                hash = 31 * hash + System.identityHashCode(block);
            }
            return hash;
        }
    }
}
