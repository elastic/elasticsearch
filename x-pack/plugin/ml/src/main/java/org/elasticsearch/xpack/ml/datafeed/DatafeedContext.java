/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.RestartTimeInfo;

import java.util.Objects;

class DatafeedContext {

    private final DatafeedConfig datafeedConfig;
    private final Job job;
    private final RestartTimeInfo restartTimeInfo;
    private final DatafeedTimingStats timingStats;

    DatafeedContext(DatafeedConfig datafeedConfig, Job job, RestartTimeInfo restartTimeInfo,
                           DatafeedTimingStats timingStats) {
        this.datafeedConfig = Objects.requireNonNull(datafeedConfig);
        this.job = Objects.requireNonNull(job);
        this.restartTimeInfo = Objects.requireNonNull(restartTimeInfo);
        this.timingStats = Objects.requireNonNull(timingStats);
    }


    DatafeedConfig getDatafeedConfig() {
        return datafeedConfig;
    }

    Job getJob() {
        return job;
    }

    RestartTimeInfo getRestartTimeInfo() {
        return restartTimeInfo;
    }

    DatafeedTimingStats getTimingStats() {
        return timingStats;
    }

    static class Builder {
        private volatile DatafeedConfig datafeedConfig;
        private volatile Job job;
        private volatile RestartTimeInfo restartTimeInfo;
        private volatile DatafeedTimingStats timingStats;

        Builder setDatafeedConfig(DatafeedConfig datafeedConfig) {
            this.datafeedConfig = datafeedConfig;
            return this;
        }

        Builder setJob(Job job) {
            this.job = job;
            return this;
        }

        Job getJob() {
            return job;
        }

        Builder setRestartTimeInfo(RestartTimeInfo restartTimeInfo) {
            this.restartTimeInfo = restartTimeInfo;
            return this;
        }

        Builder setTimingStats(DatafeedTimingStats timingStats) {
            this.timingStats = timingStats;
            return this;
        }

        DatafeedContext build() {
            return new DatafeedContext(datafeedConfig, job, restartTimeInfo, timingStats);
        }
    }
}
