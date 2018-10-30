/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class MlMigrator implements ClusterStateListener, LocalNodeMasterListener {

    private final ClusterService clusterService;
    private final JobConfigProvider jobConfigProvider;
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    public MlMigrator(ClusterService clusterService, JobConfigProvider jobConfigProvider) {
        this.clusterService = clusterService;
        this.jobConfigProvider = jobConfigProvider;
    }

    @Override
    public void onMaster() {
        if (enabled.compareAndSet(false, true)) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void offMaster() {
        if (enabled.compareAndSet(true, false)) {
            clusterService.removeListener(this);
        }
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        MlMetadata mlMetadata = MlMetadata.getMlMetadata(event.state());
        Map<String, Job> jobs = mlMetadata.getJobs();
        if (jobs.isEmpty()) {
            return;
        }

        PersistentTasksCustomMetaData pTasks = event.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        Set<String> openJobIds = MlTasks.openJobIds(pTasks);

    }

    private void migrate(String jobId, ActionListener<Boolean> listener) {
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterService.state());

        Job job = mlMetadata.getJobs().get(jobId);
        if (job == null) {
            listener.onResponse(Boolean.TRUE);
            return;
        }

        Optional<DatafeedConfig> datafeedConfig = mlMetadata.getDatafeedByJobId(jobId);

        jobConfigProvider.putJob(job, ActionListener.wrap(
                indexResponse -> {
                    if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                        clusterService.submitStateUpdateTask("migrate-job-" + job.getId(),
                                new AckedClusterStateUpdateTask<Boolean>(new DeleteJobAction.Request(jobId), listener) {
                            @Override
                            protected Boolean newResponse(boolean acknowledged) {
                                return acknowledged;
                            }

                            @Override
                            public ClusterState execute(ClusterState currentState) {
                                MlMetadata currentMlMetadata = MlMetadata.getMlMetadata(currentState);
                                if (currentMlMetadata.getJobs().containsKey(jobId) == false) {
                                    // Where has the job gone?
                                    return currentState;
                                }

                                MlMetadata.Builder builder = new MlMetadata.Builder(currentMlMetadata);
                                builder.deleteJob(jobId, currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE));

                                ClusterState.Builder newState = ClusterState.builder(currentState);
                                newState.metaData(MetaData.builder(currentState.getMetaData())
                                        .putCustom(MlMetadata.TYPE, builder.build())
                                        .build());
                                return newState.build();
                            }
                        });
                    }
                },
                listener::onFailure
        ));

    }
}
