/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.persistence.RestartTimeInfo;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
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

    public void buildDatafeedContext(String datafeedId, long datafeedStartTimeMs, ActionListener<DatafeedContext> listener) {
        DatafeedContext.Builder context = DatafeedContext.builder(datafeedStartTimeMs);

        Consumer<Result<ModelSnapshot>> modelSnapshotListener = resultSnapshot -> {
            context.setModelSnapshot(resultSnapshot == null ? null : resultSnapshot.result);
            listener.onResponse(context.build());
        };

        Consumer<DatafeedTimingStats> timingStatsListener = timingStats -> {
            context.setTimingStats(timingStats);
            resultsProvider.getModelSnapshot(
                context.getJob().getId(),
                context.getJob().getModelSnapshotId(),
                modelSnapshotListener,
                listener::onFailure);
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

    public void buildDatafeedContextForJob(String jobId, ClusterState clusterState,  ActionListener<DatafeedContext> listener) {
        ActionListener<Set<String>> datafeedListener = ActionListener.wrap(
            datafeeds -> {
                assert datafeeds.size() <= 1;
                if (datafeeds.isEmpty()) {
                    // This job has no datafeed attached to it
                    listener.onResponse(null);
                    return;
                }

                String datafeedId = datafeeds.iterator().next();
                PersistentTasksCustomMetadata tasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
                PersistentTasksCustomMetadata.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(datafeedId, tasks);
                if (datafeedTask == null) {
                    // This datafeed is not started
                    listener.onResponse(null);
                    return;
                }

                @SuppressWarnings("unchecked")
                StartDatafeedAction.DatafeedParams taskParams = (StartDatafeedAction.DatafeedParams) datafeedTask.getParams();
                buildDatafeedContext(datafeedId, taskParams.getStartTime(), listener);
            },
            listener::onFailure
        );

        datafeedConfigProvider.findDatafeedsForJobIds(Collections.singleton(jobId), datafeedListener);
    }
}
