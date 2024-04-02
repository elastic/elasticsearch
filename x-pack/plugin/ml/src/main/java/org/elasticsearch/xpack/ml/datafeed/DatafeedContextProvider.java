/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.Objects;

public class DatafeedContextProvider {

    private final JobConfigProvider jobConfigProvider;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final JobResultsProvider resultsProvider;

    public DatafeedContextProvider(
        JobConfigProvider jobConfigProvider,
        DatafeedConfigProvider datafeedConfigProvider,
        JobResultsProvider jobResultsProvider
    ) {
        this.jobConfigProvider = Objects.requireNonNull(jobConfigProvider);
        this.datafeedConfigProvider = Objects.requireNonNull(datafeedConfigProvider);
        this.resultsProvider = Objects.requireNonNull(jobResultsProvider);
    }

    public void buildDatafeedContext(String datafeedId, ActionListener<DatafeedContext> listener) {
        DatafeedContext.Builder context = DatafeedContext.builder();

        datafeedConfigProvider.getDatafeedConfig(datafeedId, null, listener.delegateFailureAndWrap((delegate1, datafeedConfigBuilder) -> {
            DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
            context.setDatafeedConfig(datafeedConfig);
            jobConfigProvider.getJob(datafeedConfig.getJobId(), null, delegate1.delegateFailureAndWrap((delegate2, jobBuilder) -> {
                context.setJob(jobBuilder.build());
                resultsProvider.getRestartTimeInfo(jobBuilder.getId(), delegate2.delegateFailureAndWrap((delegate3, restartTimeInfo) -> {
                    context.setRestartTimeInfo(restartTimeInfo);
                    resultsProvider.datafeedTimingStats(context.getJob().getId(), timingStats -> {
                        context.setTimingStats(timingStats);
                        delegate3.onResponse(context.build());
                    }, delegate3::onFailure);
                }));
            }));
        }));
    }
}
