/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.persistence.RestartTimeInfo;

import java.util.Objects;

public class DatafeedContext {

    private static final Logger logger = LogManager.getLogger(DatafeedContext.class);

    private final long datafeedStartTimeMs;
    private final DatafeedConfig datafeedConfig;
    private final Job job;
    private final RestartTimeInfo restartTimeInfo;
    private final DatafeedTimingStats timingStats;
    @Nullable
    private final ModelSnapshot modelSnapshot;

    private DatafeedContext(long datafeedStartTimeMs, DatafeedConfig datafeedConfig, Job job, RestartTimeInfo restartTimeInfo,
                           DatafeedTimingStats timingStats, ModelSnapshot modelSnapshot) {
        this.datafeedStartTimeMs = datafeedStartTimeMs;
        this.datafeedConfig = Objects.requireNonNull(datafeedConfig);
        this.job = Objects.requireNonNull(job);
        this.restartTimeInfo = Objects.requireNonNull(restartTimeInfo);
        this.timingStats = Objects.requireNonNull(timingStats);
        this.modelSnapshot = modelSnapshot;
    }

    public DatafeedConfig getDatafeedConfig() {
        return datafeedConfig;
    }

    public Job getJob() {
        return job;
    }

    public RestartTimeInfo getRestartTimeInfo() {
        return restartTimeInfo;
    }

    public DatafeedTimingStats getTimingStats() {
        return timingStats;
    }

    @Nullable
    public ModelSnapshot getModelSnapshot() {
        return modelSnapshot;
    }

    public boolean shouldRecoverFromCurrentSnapshot() {
        if (modelSnapshot == null) {
            logger.debug("[{}] checking whether recovery is required; job latest result timestamp [{}]; " +
                    "job latest record timestamp [{}]; snapshot is [null]; datafeed start time [{}]", datafeedConfig.getJobId(),
                restartTimeInfo.getLatestFinalBucketTimeMs(), restartTimeInfo.getLatestRecordTimeMs(), datafeedStartTimeMs);
        } else {
            logger.debug("[{}] checking whether recovery is required; job latest result timestamp [{}]; " +
                    "job latest record timestamp [{}]; snapshot latest result timestamp [{}]; snapshot latest record timestamp [{}]; " +
                    "datafeed start time [{}]",
                datafeedConfig.getJobId(),
                restartTimeInfo.getLatestFinalBucketTimeMs(),
                restartTimeInfo.getLatestRecordTimeMs(),
                modelSnapshot.getLatestResultTimeStamp() == null ? null : modelSnapshot.getLatestResultTimeStamp().getTime(),
                modelSnapshot.getLatestRecordTimeStamp() == null ? null : modelSnapshot.getLatestRecordTimeStamp().getTime(),
                datafeedStartTimeMs);
        }

        if (restartTimeInfo.isAfter(datafeedStartTimeMs)) {
            return restartTimeInfo.haveSeenDataPreviously() &&
                (modelSnapshot == null || restartTimeInfo.isAfterModelSnapshot(modelSnapshot));
        }
        // If the datafeed start time is past the job checkpoint we should not attempt to recover
        return false;
    }

    static Builder builder(long datafeedStartTimeMs) {
        return new Builder(datafeedStartTimeMs);
    }

    static class Builder {
        private final long datafeedStartTimeMs;
        private volatile DatafeedConfig datafeedConfig;
        private volatile Job job;
        private volatile RestartTimeInfo restartTimeInfo;
        private volatile DatafeedTimingStats timingStats;
        private volatile ModelSnapshot modelSnapshot;

        Builder(long datafeedStartTimeMs) {
            this.datafeedStartTimeMs = datafeedStartTimeMs;
        }

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

        Builder setModelSnapshot(ModelSnapshot modelSnapshot) {
            this.modelSnapshot = modelSnapshot;
            return this;
        }

        DatafeedContext build() {
            return new DatafeedContext(datafeedStartTimeMs, datafeedConfig, job, restartTimeInfo, timingStats, modelSnapshot);
        }
    }
}
