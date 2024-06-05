/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.RestartTimeInfo;

import java.util.Objects;

public record DatafeedContext(DatafeedConfig datafeedConfig, Job job, RestartTimeInfo restartTimeInfo, DatafeedTimingStats timingStats) {

    public DatafeedContext {
        Objects.requireNonNull(datafeedConfig);
        Objects.requireNonNull(job);
        Objects.requireNonNull(restartTimeInfo);
        Objects.requireNonNull(timingStats);
    }
}
