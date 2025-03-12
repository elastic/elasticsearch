/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.ExponentiallyWeightedMovingRate;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Internal class that maintains relevant indexing statistics / metrics.
 * @see IndexShard
 */
final class InternalIndexingStats implements IndexingOperationListener {

    private final LongSupplier relativeTimeInNanosSupplier;
    private final StatsHolder totalStats;

    InternalIndexingStats(LongSupplier relativeTimeInNanosSupplier) {
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.totalStats = new StatsHolder(relativeTimeInNanosSupplier.getAsLong());
    }

    /**
     * Returns the stats, including type specific stats. If the types are null/0 length, then nothing
     * is returned for them. If they are set, then only types provided will be returned, or
     * {@code _all} for all types.
     */
    IndexingStats stats(
        boolean isThrottled,
        long currentThrottleInMillis,
        long indexingTimeBeforeShardStartedInNanos,
        long timeSinceShardStartedInNanos,
        long currentTimeInNanos,
        double recentIndexingLoadAtShardStarted
    ) {
        IndexingStats.Stats total = totalStats.stats(
            isThrottled,
            currentThrottleInMillis,
            indexingTimeBeforeShardStartedInNanos,
            timeSinceShardStartedInNanos,
            currentTimeInNanos,
            recentIndexingLoadAtShardStarted
        );
        return new IndexingStats(total);
    }

    long totalIndexingTimeInNanos() {
        return totalStats.indexMetric.sum();
    }

    /**
     * Returns an exponentially-weighted moving rate which measures the indexing load, favoring more recent load.
     */
    double recentIndexingLoad(long timeInNanos) {
        return totalStats.recentIndexMetric.getRate(timeInNanos);
    }

    @Override
    public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
        if (operation.origin().isRecovery() == false) {
            totalStats.indexCurrent.inc();
        }
        return operation;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        switch (result.getResultType()) {
            case SUCCESS:
                if (index.origin().isRecovery() == false) {
                    long took = result.getTook();
                    totalStats.indexMetric.inc(took);
                    long asLong = relativeTimeInNanosSupplier.getAsLong();
                    totalStats.recentIndexMetric.addIncrement(took, asLong);
                    // TODO(pete): Remove printf logging
                    System.out.printf(
                        "***** Increment of %g ms at %g ms - unweighted rate %g - weighted rate %g%n",
                        took * 1.0e-6,
                        (asLong - totalStats.startTimeInNanosForDebugLogging) * 1.0e-6,
                        1.0 * totalStats.indexMetric.sum() / (asLong - totalStats.startTimeInNanosForDebugLogging),
                        totalStats.recentIndexMetric.getRate(asLong)
                    );
                    totalStats.indexCurrent.dec();
                }
                break;
            case FAILURE:
                postIndex(shardId, index, result.getFailure());
                break;
            default:
                throw new IllegalArgumentException("unknown result type: " + result.getResultType());
        }
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
        if (index.origin().isRecovery() == false) {
            totalStats.indexCurrent.dec();
            totalStats.indexFailed.inc();
            if (ExceptionsHelper.unwrapCause(ex) instanceof VersionConflictEngineException) {
                totalStats.indexFailedDueToVersionConflicts.inc();
            }
        }
    }

    @Override
    public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
        if (delete.origin().isRecovery() == false) {
            totalStats.deleteCurrent.inc();
        }
        return delete;

    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        switch (result.getResultType()) {
            case SUCCESS:
                if (delete.origin().isRecovery() == false) {
                    long took = result.getTook();
                    totalStats.deleteMetric.inc(took);
                    totalStats.deleteCurrent.dec();
                }
                break;
            case FAILURE:
                postDelete(shardId, delete, result.getFailure());
                break;
            default:
                throw new IllegalArgumentException("unknown result type: " + result.getResultType());
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
        if (delete.origin().isRecovery() == false) {
            totalStats.deleteCurrent.dec();
        }
    }

    void noopUpdate() {
        totalStats.noopUpdates.inc();
    }

    static class StatsHolder {
        private final MeanMetric indexMetric = new MeanMetric(); // Used for the count and total 'took' time (in ns) of index operations
        private final ExponentiallyWeightedMovingRate recentIndexMetric; // An EWMR of the total 'took' time of index operations (in ns)
        private final MeanMetric deleteMetric = new MeanMetric();
        private final CounterMetric indexCurrent = new CounterMetric();
        private final CounterMetric indexFailed = new CounterMetric();
        private final CounterMetric indexFailedDueToVersionConflicts = new CounterMetric();
        private final CounterMetric deleteCurrent = new CounterMetric();
        private final CounterMetric noopUpdates = new CounterMetric();
        private final long startTimeInNanosForDebugLogging;

        StatsHolder(long startTimeInNanos) {
            this.startTimeInNanosForDebugLogging = startTimeInNanos; // TODO(pete): Remove this
            // TODO(pete): Make lambda configurable
            /// double lambda = Math.log(2) / 5.0e9;
            double lambda = 0.0;
            this.recentIndexMetric = new ExponentiallyWeightedMovingRate(lambda, startTimeInNanos);
        }

        IndexingStats.Stats stats(
            boolean isThrottled,
            long currentThrottleMillis,
            long indexingTimeBeforeShardStartedInNanos,
            long timeSinceShardStartedInNanos,
            long currentTimeInNanos,
            double recentIndexingLoadAtShardStarted
        ) {
            final long totalIndexingTimeInNanos = indexMetric.sum();
            final long totalIndexingTimeSinceShardStartedInNanos = totalIndexingTimeInNanos - indexingTimeBeforeShardStartedInNanos;
            final double recentIndexingLoadSinceShardStarted = recentIndexMetric.calculateRateSince(
                currentTimeInNanos,
                recentIndexMetric.getRate(currentTimeInNanos),
                // The recentIndexingLoadAtShardStarted passed in should have been calculated at this time:
                currentTimeInNanos - timeSinceShardStartedInNanos,
                recentIndexingLoadAtShardStarted
            );
            // TODO(pete): Remove printf logging
            if (timeSinceShardStartedInNanos > 0) {
                System.out.printf(
                    "***** WRITE LOADS: OLD = %g / %g = %g NEW = %g%n",
                    totalIndexingTimeInNanos * 1.0e-6,
                    timeSinceShardStartedInNanos * 1.0e-6,
                    1.0 * totalIndexingTimeSinceShardStartedInNanos / timeSinceShardStartedInNanos,
                    recentIndexingLoadSinceShardStarted
                );
            } else {
                System.out.printf("***** zero time!!!%n");
            }
            return new IndexingStats.Stats(
                indexMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(totalIndexingTimeInNanos),
                indexCurrent.count(),
                indexFailed.count(),
                indexFailedDueToVersionConflicts.count(),
                deleteMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(deleteMetric.sum()),
                deleteCurrent.count(),
                noopUpdates.count(),
                isThrottled,
                TimeUnit.MILLISECONDS.toMillis(currentThrottleMillis),
                totalIndexingTimeSinceShardStartedInNanos,
                timeSinceShardStartedInNanos,
                recentIndexingLoadSinceShardStarted
            );
        }
    }
}
