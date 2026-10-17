/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.store.IndexedDISIPrefetcher;
import org.elasticsearch.lucene.store.PrefetchRanges;
import org.elasticsearch.lucene.store.PrefetchSteps;
import org.elasticsearch.lucene.store.PrefetchableDocValues;

import java.io.IOException;

/**
 * Prefetches a batch of documents from a column, in the stages {@link PrefetchableDocValues} describes. Every column goes
 * from a document to its values the same way:
 * <ol>
 *   <li><b>presence</b>: the rank of a document of a sparse column, see {@link IndexedDISIPrefetcher};</li>
 *   <li><b>addresses</b>: the value addresses of a rank, for a column holding several values a document;</li>
 *   <li><b>values</b>: the bytes at those addresses, in as many stages as the layout needs.</li>
 * </ol>
 * The navigation file locates the rest, and is read whole when the segment opens. The {@link Column} is a reader of its
 * own, so the one the consumer reads does not move.
 */
public final class ColumnPrefetcher implements PrefetchableDocValues {

    /** The most stages a layout takes to ask for its values. */
    private static final int MAX_VALUE_STAGES = 3;

    /** Where a column's bytes are. Each method only reads what an earlier step asked for. */
    public interface Column {

        /** The presence of a sparse column; {@code null} for a dense one, whose documents are their ranks. */
        @Nullable
        IndexedDISIPrefetcher presence(PrefetchRanges prefetches);

        /** Whether a rank maps to its value addresses through a table rather than being one. */
        boolean hasValueAddresses();

        /**
         * Asks for all of what maps ranks to value addresses if it lies within a single read unit.
         *
         * @return whether it asked for anything
         */
        boolean prefetchValueAddressesIfWithinOneUnit(PrefetchRanges prefetches) throws IOException;

        /**
         * Asks for what mapping {@code ranks[0, count)}, in ascending order, to value addresses reads.
         *
         * @return whether it asked for anything
         */
        boolean prefetchValueAddresses(PrefetchRanges prefetches, long[] ranks, int count) throws IOException;

        /** The value address of the first value of the document at {@code rank}. */
        long firstValueAddress(int rank) throws IOException;

        /** How many values the document at {@code rank} holds. */
        long valueCount(int rank) throws IOException;

        /**
         * Runs value stage {@code stage} for the values at {@code [firsts[i], firsts[i] + counts[i])}, ascending.
         *
         * @return whether stage {@code stage + 1} is needed
         */
        boolean prefetchValues(int stage, long[] firsts, long[] counts, int count) throws IOException;
    }

    private final Column column;
    @Nullable
    private final IndexedDISIPrefetcher presence;
    private final PrefetchSteps steps;

    private long[] ranks = new long[0];
    private int rankCount;
    private long[] firsts = new long[0];
    private long[] counts = new long[0];
    private boolean valuesDone;

    public ColumnPrefetcher(PrefetchRanges prefetches, Column column) {
        this.column = column;
        this.presence = column.presence(prefetches);
        PrefetchSteps.Step[] steps = new PrefetchSteps.Step[4 + MAX_VALUE_STAGES];
        steps[0] = PrefetchSteps.all(
            (docs, from, to) -> presence != null && presence.askJumpEntries(docs, from, to),
            (docs, from, to) -> column.hasValueAddresses() && column.prefetchValueAddressesIfWithinOneUnit(prefetches)
        );
        steps[1] = (docs, from, to) -> presence != null && presence.askBlocks(docs, from, to);
        steps[2] = this::resolveRanks;
        steps[3] = (docs, from, to) -> column.hasValueAddresses() && column.prefetchValueAddresses(prefetches, ranks, rankCount);
        for (int stage = 0; stage < MAX_VALUE_STAGES; stage++) {
            steps[4 + stage] = valueStage(stage);
        }
        this.steps = new PrefetchSteps(prefetches, steps);
    }

    @Override
    public boolean prefetch(int stage, int[] docs, int from, int to, int readUnit) throws IOException {
        return steps.prefetch(stage, docs, from, to, readUnit);
    }

    /** The ranks of the documents of the batch that have a value, in ascending order. */
    private boolean resolveRanks(int[] docs, int from, int to) throws IOException {
        ranks = ArrayUtil.growNoCopy(ranks, to - from);
        if (presence == null) {
            for (int i = from; i < to; i++) {
                ranks[i - from] = docs[i];
            }
        } else {
            presence.ranks(docs, from, to, ranks);
        }
        rankCount = 0;
        for (int i = 0; i < to - from; i++) {
            if (ranks[i] >= 0) {
                ranks[rankCount++] = ranks[i];
            }
        }
        valuesDone = false;
        return false;
    }

    private PrefetchSteps.Step valueStage(int stage) {
        return (docs, from, to) -> {
            if (valuesDone) {
                return false;
            }
            if (stage == 0) {
                firsts = ArrayUtil.growNoCopy(firsts, rankCount);
                counts = ArrayUtil.growNoCopy(counts, rankCount);
                for (int i = 0; i < rankCount; i++) {
                    firsts[i] = column.firstValueAddress((int) ranks[i]);
                    counts[i] = column.valueCount((int) ranks[i]);
                }
            }
            boolean more = column.prefetchValues(stage, firsts, counts, rankCount);
            assert more == false || stage + 1 < MAX_VALUE_STAGES : "a layout asks for more value stages than " + MAX_VALUE_STAGES;
            valuesDone = more == false;
            return more;
        };
    }
}
