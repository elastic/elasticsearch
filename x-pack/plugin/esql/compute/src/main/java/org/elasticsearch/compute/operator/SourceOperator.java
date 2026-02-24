/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.ShardContext;

import java.util.List;

/**
 * A source operator - produces output, accepts no input.
 */
public abstract class SourceOperator implements Operator {
    /**
     * A source operator needs no input - unconditionally returns false.
     * @return false
     */
    public final boolean needsInput() {
        return false;
    }

    /**
     * A source operator does not accept input - unconditionally throws UnsupportedOperationException.
     * @param page a page
     */
    @Override
    public final void addInput(Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        // Source operators should return false by default.
        // Their data production is already gated by nextOp.needsInput() in the driver loop.
        // If a source has data but downstream doesn't need it (e.g., blocked on async),
        // we should wait, not busy-spin.
        // Subclasses that buffer data internally may override this if needed.
        return false;
    }

    /**
     * A factory for creating source operators.
     */
    public interface SourceOperatorFactory extends OperatorFactory, Describable {
        /** Creates a new source operator. */
        @Override
        SourceOperator get(DriverContext driverContext);
    }

    public record ShardLoad(ShardContext context, long processTimeNanos, long rowsEmitted) {}

    /**
     * Returns a snapshot of shard load accumulated since the previous invocation.
     *
     * <p>This method is meant to be overridden by {@link SourceOperator}
     * implementations that can attribute produced pages to shards or indices.
     * <p>The snapshot represents a <em>delta</em>: implementations must reset their
     * internal counters after the snapshot is taken so that each invocation reports
     * only the load observed since the last call.</p>
     *
     * <p>Source operators that do not operate on shards or indices can ignore this
     * mechanism and rely on the default implementation, which returns an empty list.</p>
     *
     * @return list of per-shard load since the last call
     */
    protected List<ShardLoad> shardLoadDelta(long now) {
        return List.of();
    }

    /**
     * Attributes the CPU time delta since the previous call to the shards that
     * produced pages during the same interval.
     *
     * <p>This method is invoked internally on {@link SourceOperator}. Sources are
     * responsible for shard-level attribution because they are the point in the
     * execution pipeline where output pages can still be associated with the shards
     * or indices that produced them.</p>
     *
     * <p>The {@code extraCpuNanos} parameter represents the elapsed CPU time since
     * the previous invocation. That time is attributed to shards according to the
     * load snapshot returned by {@link #shardLoadDelta(long)} for the same interval.</p>
     *
     * <p>Attribution rules:
     * <ul>
     *   <li>If exactly one shard contributed load, the full {@code extraCpuNanos} is
     *       attributed to that shard.</li>
     *   <li>If multiple shards contributed, each shard is first attributed the
     *       process time it directly reported.</li>
     *   <li>Any remaining CPU time is then distributed proportionally based on the
     *       number of rows emitted by each shard.</li>
     * </ul>
     *
     * <p>This method assumes that {@link #shardLoadDelta(long)} returns a delta and
     * resets its internal counters on each invocation.</p>
     *
     * @param extraCpuNanos CPU time delta, in nanoseconds, since the previous call
     */
    final void reportSearchLoad(long extraCpuNanos, long now) {
        final List<ShardLoad> delta = shardLoadDelta(now);
        final int size = delta.size();
        if (size == 0) {
            return;
        }

        if (size == 1) {
            // Single shard: attribute all processing to it
            delta.getFirst().context().stats().accumulateSearchLoad(extraCpuNanos, now);
            return;
        }

        long totalProcess = 0L;
        long totalRows = 0L;
        for (var load : delta) {
            totalProcess += load.processTimeNanos();
            totalRows += load.rowsEmitted();
        }
        if (totalRows == 0L && totalProcess == 0L && extraCpuNanos == 0L) {
            return;
        }

        final long rest = Math.max(0L, extraCpuNanos - totalProcess);

        for (var load : delta) {
            long weightedExtra = load.processTimeNanos();
            if (rest > 0L && load.rowsEmitted() > 0L) {
                // Distribute remaining CPU proportionally by rows emitted
                weightedExtra += Math.round((double) rest * ((double) load.rowsEmitted() / (double) totalRows));
            }
            if (weightedExtra > 0L) {
                load.context().stats().accumulateSearchLoad(weightedExtra, now);
            }
        }
    }
}
