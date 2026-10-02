/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.EsqlDatafeedSourceCheckpoint;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.Objects;

public class DatafeedContextProvider {

    private static final Logger LOGGER = LogManager.getLogger(DatafeedContextProvider.class);

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
        datafeedConfigProvider.getDatafeedConfig(datafeedId, null, listener.delegateFailureAndWrap((delegate1, datafeedConfigBuilder) -> {
            DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
            jobConfigProvider.getJob(datafeedConfig.getJobId(), null, delegate1.delegateFailureAndWrap((delegate2, jobBuilder) -> {
                resultsProvider.getRestartTimeInfo(jobBuilder.getId(), delegate2.delegateFailureAndWrap((delegate3, restartTimeInfo) -> {
                    resultsProvider.datafeedTimingStats(jobBuilder.getId(), timingStats -> {
                        if (datafeedConfig.getEsqlQuery() == null) {
                            delegate3.onResponse(new DatafeedContext(datafeedConfig, jobBuilder.build(), restartTimeInfo, timingStats));
                            return;
                        }
                        resultsProvider.esqlDatafeedSourceCheckpoint(
                            jobBuilder.getId(),
                            checkpoint -> delegate3.onResponse(
                                new DatafeedContext(
                                    datafeedConfig,
                                    jobBuilder.build(),
                                    restartTimeInfo,
                                    timingStats,
                                    validateLoadedCheckpoint(datafeedConfig, jobBuilder.build(), checkpoint)
                                )
                            ),
                            delegate3::onFailure
                        );
                    }, delegate3::onFailure);
                }));
            }));
        }));
    }

    @Nullable
    public static EsqlDatafeedSourceCheckpoint validateLoadedCheckpoint(
        DatafeedConfig datafeedConfig,
        org.elasticsearch.xpack.core.ml.job.config.Job job,
        @Nullable EsqlDatafeedSourceCheckpoint checkpoint
    ) {
        if (checkpoint == null) {
            return null;
        }
        String emittedTimeField = job.getDataDescription() == null ? null : job.getDataDescription().getTimeField();
        String expectedFingerprint = EsqlDatafeedSourceCheckpoint.computeFingerprint(datafeedConfig, emittedTimeField);
        EsqlDatafeedSourceCheckpoint validated = EsqlDatafeedSourceCheckpoint.validateFingerprint(checkpoint, expectedFingerprint);
        if (validated == null) {
            LOGGER.warn(
                "[{}] ES|QL source checkpoint fingerprint mismatch for datafeed [{}]; discarding persisted checkpoint. "
                    + "Recreate the datafeed and reset the job when source/time-domain semantics change.",
                job.getId(),
                datafeedConfig.getId()
            );
        }
        return validated;
    }
}
