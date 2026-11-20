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
import org.elasticsearch.lucene.store.DocRanks;
import org.elasticsearch.lucene.store.PrefetchSteps;
import org.elasticsearch.lucene.store.PrefetchableDocValues;

import java.io.IOException;

/**
 * Prefetches a batch of documents from a column. A document's <em>rank</em> is its position among the documents that
 * have a value; its values sit at consecutive <em>value addresses</em>, positions among all the values of the column.
 * The prefetch goes from documents to ranks, from ranks to value addresses, then to the bytes of the values. The
 * navigation file, warmed when the segment opens, locates most of it without a read.
 *
 * <p>The {@link Column} is a reader of its own, so the one the consumer reads does not move.
 */
public final class ColumnPrefetcher implements PrefetchableDocValues {

    /** The most stages a layout takes to prefetch its values. */
    private static final int MAX_VALUE_STAGES = 3;

    /** Where a column's bytes are. Each method reads only what an earlier one prefetched. */
    public interface Column {

        /** The ranks of a batch, through the column's presence. */
        DocRanks ranks();

        /**
         * Prefetches, whole, the tables that locate the values of any document.
         *
         * @return whether it prefetched anything
         */
        boolean prefetchTables() throws IOException;

        /**
         * Prefetches what mapping {@code ranks[0, count)}, in ascending order, to value addresses reads.
         *
         * @return whether it prefetched anything
         */
        boolean prefetchValueAddresses(long[] ranks, int count) throws IOException;

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
    private final DocRanks ranks;
    private final PrefetchSteps steps;

    private long[] firsts = new long[0];
    private long[] counts = new long[0];
    private boolean valuesDone;

    public ColumnPrefetcher(Column column) {
        this.column = column;
        this.ranks = column.ranks();
        PrefetchSteps.Step[] steps = new PrefetchSteps.Step[2 + MAX_VALUE_STAGES];
        steps[0] = PrefetchSteps.all((docs, from, to) -> ranks.prefetchPresence(), (docs, from, to) -> column.prefetchTables());
        steps[1] = (docs, from, to) -> {
            ranks.resolve(docs, from, to);
            valuesDone = false;
            return column.prefetchValueAddresses(ranks.ranks(), ranks.count());
        };
        for (int stage = 0; stage < MAX_VALUE_STAGES; stage++) {
            steps[2 + stage] = valueStage(stage);
        }
        this.steps = new PrefetchSteps(steps);
    }

    @Override
    public boolean prefetch(int stage, int[] docs, int from, int to) throws IOException {
        return steps.prefetch(stage, docs, from, to);
    }

    private PrefetchSteps.Step valueStage(int stage) {
        return (docs, from, to) -> {
            if (valuesDone) {
                return false;
            }
            int count = ranks.count();
            if (stage == 0) {
                firsts = ArrayUtil.growNoCopy(firsts, count);
                counts = ArrayUtil.growNoCopy(counts, count);
                for (int i = 0; i < count; i++) {
                    firsts[i] = column.firstValueAddress((int) ranks.ranks()[i]);
                    counts[i] = column.valueCount((int) ranks.ranks()[i]);
                }
            }
            boolean more = column.prefetchValues(stage, firsts, counts, count);
            assert more == false || stage + 1 < MAX_VALUE_STAGES : "a layout takes more value stages than " + MAX_VALUE_STAGES;
            valuesDone = more == false;
            return more;
        };
    }
}
