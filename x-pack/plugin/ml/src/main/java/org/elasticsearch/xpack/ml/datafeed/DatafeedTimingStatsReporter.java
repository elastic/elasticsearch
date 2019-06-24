/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Function;

public class DatafeedTimingStatsReporter {

    private final Clock clock;
    private final JobResultsPersister jobResultsPersister;
    private DatafeedTimingStats persistedTimingStats;
    private volatile DatafeedTimingStats currentTimingStats;

    public DatafeedTimingStatsReporter(DatafeedTimingStats timingStats, Clock clock, JobResultsPersister jobResultsPersister) {
        Objects.requireNonNull(timingStats);
        this.persistedTimingStats = new DatafeedTimingStats(timingStats);
        this.currentTimingStats = new DatafeedTimingStats(timingStats);
        this.clock = Objects.requireNonNull(clock);
        this.jobResultsPersister = Objects.requireNonNull(jobResultsPersister);
    }

    public DatafeedTimingStats getCurrentTimingStats() {
        return new DatafeedTimingStats(currentTimingStats);
    }

    /**
     * Executes the given function and reports how much time did the execution take.
     */
    public <T, R> R executeWithReporting(Function<T, R> function, T argument) {
        Instant before = clock.instant();
        R result = function.apply(argument);
        Instant after = clock.instant();
        Duration duration = Duration.between(before, after);
        reportSearchDuration(duration);
        return result;
    }

    private void reportSearchDuration(Duration searchDuration) {
        double searchDurationMs = searchDuration.toMillis();
        currentTimingStats.incrementTotalSearchTimeMs(searchDurationMs);
        if (differSignificantly(currentTimingStats, persistedTimingStats)) {
            jobResultsPersister.persistDatafeedTimingStats(currentTimingStats);
            persistedTimingStats = currentTimingStats;
            currentTimingStats = new DatafeedTimingStats(persistedTimingStats);
        }
    }

    /**
     * Returns true if given stats objects differ from each other by more than 10% for at least one of the statistics.
     */
    public static boolean differSignificantly(DatafeedTimingStats stats1, DatafeedTimingStats stats2) {
        return differSignificantly(stats1.getTotalSearchTimeMs(), stats2.getTotalSearchTimeMs());
    }

    /**
     * Returns {@code true} if one of the ratios { value1 / value2, value2 / value1 } is smaller than MIN_VALID_RATIO.
     * This can be interpreted as values { value1, value2 } differing significantly from each other.
     */
    private static boolean differSignificantly(double value1, double value2) {
        return (value2 / value1 < MIN_VALID_RATIO) || (value1 / value2 < MIN_VALID_RATIO);
    }

    /**
     * Minimum ratio of values that is interpreted as values being similar.
     * If the values ratio is less than MIN_VALID_RATIO, the values are interpreted as significantly different.
     */
    private static final double MIN_VALID_RATIO = 0.9;
}
