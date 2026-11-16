/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;

/** Single-owner condition cells, independent of retained output handles. */
final class SequenceCandidateValues implements Releasable {
    private final CircuitBreaker breaker;
    private final long bytes;
    private Block[] blocks;

    private SequenceCandidateValues(CircuitBreaker breaker, long bytes, Block[] blocks) {
        this.breaker = breaker;
        this.bytes = bytes;
        this.blocks = blocks;
    }

    /** Copies cells while their page is alive; builders/blocks account their data, this owner only its container. */
    static SequenceCandidateValues copyRow(
        Page page,
        int position,
        List<SequenceLatestValidConfig.ValueDemand> demands,
        int[] channels,
        BlockFactory blockFactory,
        CircuitBreaker breaker
    ) {
        if (position < 0 || position >= page.getPositionCount() || demands.size() != channels.length) {
            throw new IllegalArgumentException("invalid mandatory sequence row layout");
        }
        long bytes = RamUsageEstimator.shallowSizeOfInstance(SequenceCandidateValues.class) + RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) channels.length * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
        breaker.addEstimateBytesAndMaybeBreak(bytes, "esql_sequence_values");
        Block[] blocks = null;
        try {
            blocks = new Block[channels.length];
            for (int slot = 0; slot < channels.length; slot++) {
                if (channels[slot] < 0 || channels[slot] >= page.getBlockCount()) {
                    throw new IllegalArgumentException("missing mandatory sequence column channel");
                }
                try (var builder = PlannerUtils.toElementType(demands.get(slot).attribute().dataType()).newBlockBuilder(1, blockFactory)) {
                    builder.copyFrom(page.getBlock(channels[slot]), position, position + 1);
                    blocks[slot] = builder.build();
                }
            }
            return new SequenceCandidateValues(breaker, bytes, blocks);
        } catch (RuntimeException | Error e) {
            try {
                Releasables.close(blocks);
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != e) {
                    e.addSuppressed(cleanup);
                }
            }
            try {
                breaker.addWithoutBreaking(-bytes, "esql_sequence_values");
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != e) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        }
    }

    int size() {
        ensureOpen();
        return blocks.length;
    }

    Block block(int demandSlot) {
        ensureOpen();
        return blocks[demandSlot];
    }

    long containerBytes() {
        return bytes;
    }

    private void ensureOpen() {
        if (blocks == null) {
            throw new IllegalStateException("mandatory sequence values are closed");
        }
    }

    @Override
    public void close() {
        if (blocks != null) {
            Block[] owned = blocks;
            blocks = null;
            try {
                Releasables.close(owned);
            } finally {
                breaker.addWithoutBreaking(-bytes, "esql_sequence_values");
            }
        }
    }

}
