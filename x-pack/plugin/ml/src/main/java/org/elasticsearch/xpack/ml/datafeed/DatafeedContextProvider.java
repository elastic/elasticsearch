/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.persistence.RestartTimeInfo;

import java.util.Objects;
import java.util.function.Consumer;

public class DatafeedContextProvider {

    private final JobConfigProvider jobConfigProvider;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final JobResultsProvider resultsProvider;

    public DatafeedContextProvider(JobConfigProvider jobConfigProvider, DatafeedConfigProvider datafeedConfigProvider,
                                   JobResultsProvider jobResultsProvider) {
        this.jobConfigProvider = Objects.requireNonNull(jobConfigProvider);
        this.datafeedConfigProvider = Objects.requireNonNull(datafeedConfigProvider);
        this.resultsProvider = Objects.requireNonNull(jobResultsProvider);
    }

    public void buildDatafeedContext(String datafeedId, ActionListener<DatafeedContext> listener) {
        DatafeedContext.Builder context = DatafeedContext.builder();

        Consumer<DatafeedTimingStats> timingStatsListener = timingStats -> {
            context.setTimingStats(timingStats);
            listener.onResponse(context.build());
        };

        ActionListener<RestartTimeInfo> restartTimeInfoListener = ActionListener.wrap(
            restartTimeInfo -> {
                context.setRestartTimeInfo(restartTimeInfo);
                resultsProvider.datafeedTimingStats(context.getJob().getId(), timingStatsListener, listener::onFailure);
            },
            listener::onFailure
        );

        ActionListener<Job.Builder> jobConfigListener = ActionListener.wrap(
            jobBuilder -> {
                context.setJob(jobBuilder.build());
                resultsProvider.getRestartTimeInfo(jobBuilder.getId(), restartTimeInfoListener);
            },
            listener::onFailure
        );

        ActionListener<DatafeedConfig.Builder> datafeedListener = ActionListener.wrap(
            datafeedConfigBuilder -> {
                DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
                context.setDatafeedConfig(datafeedConfig);
                jobConfigProvider.getJob(datafeedConfig.getJobId(), jobConfigListener);
            },
            listener::onFailure
        );

        datafeedConfigProvider.getDatafeedConfig(datafeedId, datafeedListener);
    }
}
