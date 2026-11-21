/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;

import java.util.Arrays;

/**
 * The group ids of one page, as <strong>runs</strong>: maximal stretches of adjacent positions that share one group id.
 * <p>
 *     A hash that knows its input is sorted by the grouping key (the shard's primary {@code index.sort.field}) finds
 *     the run boundaries while it hashes. It hands them over once, through {@link GroupingAggregatorFunction.AddInput#addRuns},
 *     so no aggregator has to rescan the ids to find the same boundaries. An aggregator can then combine a whole run
 *     and touch its group's state once.
 * </p>
 * <p>
 *     Run {@code r} covers the positions {@code [start(r), end(r))} of the page, and the runs together cover every
 *     position of the page exactly once, in order. Adjacent runs normally have different group ids, but an
 *     implementation must not rely on that, and a group id may appear in many, non-adjacent, runs.
 * </p>
 * <p>
 *     An instance is a scratch buffer owned by the hash that fills it and reused for every page, so it is only valid
 *     for the duration of the {@code addRuns} call it is passed to. Its memory is bounded by the number of positions
 *     in a page and is not tracked by the circuit breaker, like the other per-page scratch arrays of the hashes.
 * </p>
 */
public final class GroupRuns {
    private final BlockFactory blockFactory;
    private int[] groupIds = new int[16];
    private int[] ends = new int[16];
    private int runCount;

    public GroupRuns(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    /**
     * Forget the runs of the previous page. Called by the hash before it fills the next page.
     */
    public void reset() {
        runCount = 0;
    }

    /**
     * Append a run that ends (exclusively) at {@code end}. It starts where the previous run ended, or at 0 for the
     * first run. Its group id is not known yet, see {@link #setGroupId}.
     */
    public void appendRunEnd(int end) {
        assert runCount == 0 ? end > 0 : end > ends[runCount - 1] : "runs must be non-empty and ordered";
        if (runCount == ends.length) {
            int newLength = ends.length << 1;
            ends = Arrays.copyOf(ends, newLength);
            groupIds = Arrays.copyOf(groupIds, newLength);
        }
        ends[runCount++] = end;
    }

    /**
     * Set the group id of run {@code run}. The hash does this once it has hashed the run's key.
     */
    public void setGroupId(int run, int groupId) {
        assert run < runCount;
        groupIds[run] = groupId;
    }

    /**
     * The number of runs.
     */
    public int runCount() {
        return runCount;
    }

    /**
     * Is the whole page a single run, so that every position belongs to the same group?
     */
    public boolean isSingleRun() {
        return runCount == 1;
    }

    /**
     * The number of positions covered by all the runs together.
     */
    public int positionCount() {
        return runCount == 0 ? 0 : ends[runCount - 1];
    }

    /**
     * The group id of run {@code run}.
     */
    public int groupId(int run) {
        assert run < runCount;
        return groupIds[run];
    }

    /**
     * The first position of run {@code run}.
     */
    public int start(int run) {
        assert run < runCount;
        return run == 0 ? 0 : ends[run - 1];
    }

    /**
     * One past the last position of run {@code run}.
     */
    public int end(int run) {
        assert run < runCount;
        return ends[run];
    }

    /**
     * The number of positions in run {@code run}.
     */
    public int length(int run) {
        return end(run) - start(run);
    }

    /**
     * Expand the runs into one group id per position. This is what an {@link GroupingAggregatorFunction.AddInput}
     * that cannot use runs falls back to. The caller owns, and must close, the result.
     */
    public IntVector toVector() {
        if (runCount == 1) {
            return blockFactory.newConstantIntVector(groupIds[0], ends[0]);
        }
        try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(positionCount())) {
            int position = 0;
            for (int run = 0; run < runCount; run++) {
                int groupId = groupIds[run];
                for (int end = ends[run]; position < end; position++) {
                    builder.appendInt(groupId);
                }
            }
            return builder.build();
        }
    }
}
