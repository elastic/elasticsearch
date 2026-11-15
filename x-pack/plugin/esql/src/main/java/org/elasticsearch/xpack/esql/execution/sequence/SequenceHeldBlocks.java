/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Counts a block's reported RAM once while this source has any logical holder of that Block object. The ledger
 * neither takes a block reference nor releases a block. Distinct wrappers sharing storage can conservatively
 * count that storage more than once; this is a Source-held estimate, not a physical-allocation high-water mark.
 */
final class SequenceHeldBlocks {
    private final SequenceHeldMemory.Scope scope;
    private final Map<Block, Entry> held = new IdentityHashMap<>();

    SequenceHeldBlocks(SequenceHeldMemory memory) {
        scope = Objects.requireNonNull(memory).newScope();
    }

    /** The caller fills this private array sequentially and must not replace a block after claiming it. */
    Owner newOwner(Block[] blocks) {
        return new Owner(Objects.requireNonNull(blocks));
    }

    /** Claims an accepted raw, bound, collected or result page until its Source ownership ends. */
    Owner hold(Page page) {
        Block[] blocks = new Block[page.getBlockCount()];
        for (int channel = 0; channel < blocks.length; channel++) {
            blocks[channel] = page.getBlock(channel);
        }
        return hold(blocks);
    }

    /** A separately produced result cell can be held until its batch closes or its outgoing page takes ownership. */
    Owner hold(Block block) {
        return hold(new Block[] { block });
    }

    private Owner hold(Block[] blocks) {
        Owner owner = newOwner(blocks);
        try {
            for (int i = 0; i < blocks.length; i++) {
                owner.claimNext();
            }
            return owner;
        } catch (RuntimeException | Error e) {
            try {
                owner.close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != e) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        }
    }

    synchronized int distinctBlocks() {
        return held.size();
    }

    /** Logical ownership ends independently of actual block refcounts, including when a result is handed downstream. */
    final class Owner implements Releasable {
        private Block[] blocks;
        private int claimed;

        private Owner(Block[] blocks) {
            this.blocks = blocks;
        }

        /** Claims each freshly built cell immediately, so partial-copy failure preserves its held-memory peak. */
        void claimNext() {
            synchronized (SequenceHeldBlocks.this) {
                if (blocks == null || claimed == blocks.length) {
                    throw new IllegalStateException("held block owner has no unclaimed slot");
                }
                Block block = Objects.requireNonNull(blocks[claimed]);
                Entry entry = held.get(block);
                if (entry == null) {
                    long bytes = block.ramBytesUsed();
                    if (bytes < 0) {
                        throw new IllegalArgumentException("negative block RAM estimate");
                    }
                    try {
                        held.put(block, new Entry(bytes));
                        scope.recordHeldBytes(bytes);
                    } catch (RuntimeException | Error e) {
                        held.remove(block);
                        throw e;
                    }
                } else {
                    entry.owners = Math.addExact(entry.owners, 1);
                }
                claimed++;
            }
        }

        @Override
        public void close() {
            synchronized (SequenceHeldBlocks.this) {
                if (blocks == null) {
                    return;
                }
                for (int index = 0; index < claimed; index++) {
                    Block block = blocks[index];
                    Entry entry = held.get(block);
                    if (entry == null || entry.owners < 1) {
                        throw new IllegalStateException("held block ownership is unbalanced");
                    }
                    if (entry.owners == 1) {
                        scope.recordHeldBytes(-entry.bytes);
                        held.remove(block);
                    } else {
                        entry.owners--;
                    }
                }
                blocks = null;
                claimed = 0;
            }
        }
    }

    private static final class Entry {
        private final long bytes;
        private int owners = 1;

        private Entry(long bytes) {
            this.bytes = bytes;
        }
    }
}
