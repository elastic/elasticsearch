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
        if (DatafeedTimingStats.differSignificantly(currentTimingStats, persistedTimingStats)) {
            jobResultsPersister.persistDatafeedTimingStats(currentTimingStats);
            persistedTimingStats = currentTimingStats;
            currentTimingStats = new DatafeedTimingStats(persistedTimingStats);
        }
    }
}
