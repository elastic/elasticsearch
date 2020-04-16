/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;

import java.util.Objects;

/**
 * {@link TimingStatsReporter} class handles the logic of persisting {@link TimingStats} if they changed significantly since the last time
 * they were persisted.
 *
 * This class is not thread-safe.
 */
public class TimingStatsReporter {

    /** Persisted timing stats. May be stale. */
    private TimingStats persistedTimingStats;
    /** Current timing stats. */
    private volatile TimingStats currentTimingStats;
    /** Object used to persist current timing stats. */
    private final JobResultsPersister.Builder bulkResultsPersister;

    public TimingStatsReporter(TimingStats timingStats, JobResultsPersister.Builder jobResultsPersister) {
        Objects.requireNonNull(timingStats);
        this.persistedTimingStats = new TimingStats(timingStats);
        this.currentTimingStats = new TimingStats(timingStats);
        this.bulkResultsPersister = Objects.requireNonNull(jobResultsPersister);
    }

    public TimingStats getCurrentTimingStats() {
        return new TimingStats(currentTimingStats);
    }

    public void reportBucket(Bucket bucket) {
        currentTimingStats.updateStats(bucket.getProcessingTimeMs());
        currentTimingStats.setLatestRecordTimestamp(bucket.getTimestamp().toInstant().plusSeconds(bucket.getBucketSpan()));
        if (differSignificantly(currentTimingStats, persistedTimingStats)) {
            flush();
        }
    }

    public void finishReporting() {
        // Don't flush if current timing stats are identical to the persisted ones
        if (currentTimingStats.equals(persistedTimingStats)) {
            return;
        }
        flush();
    }

    private void flush() {
        persistedTimingStats = new TimingStats(currentTimingStats);
        bulkResultsPersister.persistTimingStats(persistedTimingStats);
    }

    /**
     * Returns true if given stats objects differ from each other by more than 10% for at least one of the statistics.
     */
    public static boolean differSignificantly(TimingStats stats1, TimingStats stats2) {
        return differSignificantly(stats1.getMinBucketProcessingTimeMs(), stats2.getMinBucketProcessingTimeMs())
            || differSignificantly(stats1.getMaxBucketProcessingTimeMs(), stats2.getMaxBucketProcessingTimeMs())
            || differSignificantly(stats1.getAvgBucketProcessingTimeMs(), stats2.getAvgBucketProcessingTimeMs())
            || differSignificantly(stats1.getExponentialAvgBucketProcessingTimeMs(), stats2.getExponentialAvgBucketProcessingTimeMs());
    }

    /**
     * Returns {@code true} if one of the ratios { value1 / value2, value2 / value1 } is smaller than MIN_VALID_RATIO.
     * This can be interpreted as values { value1, value2 } differing significantly from each other.
     * This method also returns:
     *   - {@code true} in case one value is {@code null} while the other is not.
     *   - {@code false} in case both values are {@code null}.
     */
    static boolean differSignificantly(Double value1, Double value2) {
        if (value1 != null && value2 != null) {
            return (value2 / value1 < MIN_VALID_RATIO) || (value1 / value2 < MIN_VALID_RATIO);
        }
        return (value1 != null) || (value2 != null);
    }

    /**
     * Minimum ratio of values that is interpreted as values being similar.
     * If the values ratio is less than MIN_VALID_RATIO, the values are interpreted as significantly different.
     */
    private static final double MIN_VALID_RATIO = 0.9;
}
