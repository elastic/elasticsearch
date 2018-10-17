/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.List;
import java.util.Objects;

public class DatafeedNodeSelector {

    private static final Logger LOGGER = Loggers.getLogger(DatafeedNodeSelector.class);

    private final DatafeedConfig datafeed;
    private final PersistentTasksCustomMetaData.PersistentTask<?> jobTask;
    private final ClusterState clusterState;
    private final IndexNameExpressionResolver resolver;

    public DatafeedNodeSelector(ClusterState clusterState, IndexNameExpressionResolver resolver, String datafeedId) {
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
        PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        this.datafeed = mlMetadata.getDatafeed(datafeedId);
        this.jobTask = MlTasks.getJobTask(datafeed.getJobId(), tasks);
        this.clusterState = Objects.requireNonNull(clusterState);
        this.resolver = Objects.requireNonNull(resolver);
    }

    public void checkDatafeedTaskCanBeCreated() {
        AssignmentFailure assignmentFailure = checkAssignment();
        if (assignmentFailure != null && assignmentFailure.isCriticalForTaskCreation) {
            String msg = "No node found to start datafeed [" + datafeed.getId() + "], allocation explanation [" + assignmentFailure.reason
                    + "]";
            LOGGER.debug(msg);
            throw ExceptionsHelper.conflictStatusException(msg);
        }
    }

    public PersistentTasksCustomMetaData.Assignment selectNode() {
        AssignmentFailure assignmentFailure = checkAssignment();
        if (assignmentFailure == null) {
            return new PersistentTasksCustomMetaData.Assignment(jobTask.getExecutorNode(), "");
        }
        LOGGER.debug(assignmentFailure.reason);
        return new PersistentTasksCustomMetaData.Assignment(null, assignmentFailure.reason);
    }

    @Nullable
    private AssignmentFailure checkAssignment() {
        PriorityFailureCollector priorityFailureCollector = new PriorityFailureCollector();
        priorityFailureCollector.add(verifyIndicesActive(datafeed));

        JobTaskState jobTaskState = null;
        JobState jobState = JobState.CLOSED;
        if (jobTask != null) {
            jobTaskState = (JobTaskState) jobTask.getState();
            jobState = jobTaskState == null ? JobState.OPENING : jobTaskState.getState();
        }

        if (jobState.isAnyOf(JobState.OPENING, JobState.OPENED) == false) {
            // lets try again later when the job has been opened:
            String reason = "cannot start datafeed [" + datafeed.getId() + "], because job's [" + datafeed.getJobId() +
                    "] state is [" + jobState +  "] while state [" + JobState.OPENED + "] is required";
            priorityFailureCollector.add(new AssignmentFailure(reason, true));
        }

        if (jobTaskState != null && jobTaskState.isStatusStale(jobTask)) {
            String reason = "cannot start datafeed [" + datafeed.getId() + "], job [" + datafeed.getJobId() + "] state is stale";
            priorityFailureCollector.add(new AssignmentFailure(reason, true));
        }

        return priorityFailureCollector.get();
    }

    @Nullable
    private AssignmentFailure verifyIndicesActive(DatafeedConfig datafeed) {
        List<String> indices = datafeed.getIndices();
        for (String index : indices) {

            if (RemoteClusterLicenseChecker.isRemoteIndex(index)) {
                // We cannot verify remote indices
                continue;
            }

            String[] concreteIndices;
            String reason = "cannot start datafeed [" + datafeed.getId() + "] because index ["
                    + index + "] does not exist, is closed, or is still initializing.";

            try {
                concreteIndices = resolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), index);
                if (concreteIndices.length == 0) {
                    return new AssignmentFailure(reason, true);
                }
            } catch (Exception e) {
                LOGGER.debug(reason, e);
                return new AssignmentFailure(reason, true);
            }

            for (String concreteIndex : concreteIndices) {
                IndexRoutingTable routingTable = clusterState.getRoutingTable().index(concreteIndex);
                if (routingTable == null || !routingTable.allPrimaryShardsActive()) {
                    reason = "cannot start datafeed [" + datafeed.getId() + "] because index ["
                            + concreteIndex + "] does not have all primary shards active yet.";
                    return new AssignmentFailure(reason, false);
                }
            }
        }
        return null;
    }

    private static class AssignmentFailure {
        private final String reason;
        private final boolean isCriticalForTaskCreation;

        private AssignmentFailure(String reason, boolean isCriticalForTaskCreation) {
            this.reason = reason;
            this.isCriticalForTaskCreation = isCriticalForTaskCreation;
        }
    }

    /**
     * Collects the first critical failure if any critical failure is added
     * or the first failure otherwise
     */
    private static class PriorityFailureCollector {
        private AssignmentFailure failure;

        private void add(@Nullable AssignmentFailure newFailure) {
            if (newFailure == null) {
                return;
            }
            if (failure == null || (failure.isCriticalForTaskCreation == false && newFailure.isCriticalForTaskCreation)) {
                failure = newFailure;
            }
        }

        @Nullable
        private AssignmentFailure get() {
            return failure;
        }
    }
}
