/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.job.config.JobStatus;
import org.elasticsearch.xpack.ml.job.metadata.Allocation;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;

import java.util.function.Consumer;
import java.util.function.Predicate;

public class JobStatusObserver {

    private static final Logger LOGGER = Loggers.getLogger(JobStatusObserver.class);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    public JobStatusObserver(ThreadPool threadPool, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    public void waitForStatus(String jobId, TimeValue waitTimeout, JobStatus expectedStatus, Consumer<Exception> handler) {
        ClusterStateObserver observer =
                new ClusterStateObserver(clusterService, LOGGER, threadPool.getThreadContext());
        JobStatusPredicate jobStatusPredicate = new JobStatusPredicate(jobId, expectedStatus);
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                handler.accept(null);
            }

            @Override
            public void onClusterServiceClose() {
                Exception e = new IllegalArgumentException("Cluster service closed while waiting for job status to change to ["
                        + expectedStatus + "]");
                handler.accept(new IllegalStateException(e));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                if (jobStatusPredicate.test(clusterService.state())) {
                    handler.accept(null);
                } else {
                    Exception e = new IllegalArgumentException("Timeout expired while waiting for job status to change to ["
                            + expectedStatus + "]");
                    handler.accept(e);
                }
            }
        }, jobStatusPredicate, waitTimeout);
    }

    private static class JobStatusPredicate implements Predicate<ClusterState> {

        private final String jobId;
        private final JobStatus expectedStatus;

        JobStatusPredicate(String jobId, JobStatus expectedStatus) {
            this.jobId = jobId;
            this.expectedStatus = expectedStatus;
        }

        @Override
        public boolean test(ClusterState newState) {
            MlMetadata metadata = newState.getMetaData().custom(MlMetadata.TYPE);
            if (metadata != null) {
                Allocation allocation = metadata.getAllocations().get(jobId);
                if (allocation != null) {
                    return allocation.getStatus() == expectedStatus;
                }
            }
            return false;
        }

    }

}
