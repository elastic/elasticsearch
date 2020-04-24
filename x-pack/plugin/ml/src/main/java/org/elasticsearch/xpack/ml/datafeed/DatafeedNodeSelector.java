/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.MlTasks.AWAITING_UPGRADE;

public class DatafeedNodeSelector {

    private static final Logger LOGGER = LogManager.getLogger(DatafeedNodeSelector.class);

    public static final PersistentTasksCustomMetadata.Assignment AWAITING_JOB_ASSIGNMENT =
        new PersistentTasksCustomMetadata.Assignment(null, "datafeed awaiting job assignment.");

    private final String datafeedId;
    private final String jobId;
    private final List<String> datafeedIndices;
    private final PersistentTasksCustomMetadata.PersistentTask<?> jobTask;
    private final ClusterState clusterState;
    private final IndexNameExpressionResolver resolver;
    private final IndicesOptions indicesOptions;

    public DatafeedNodeSelector(ClusterState clusterState, IndexNameExpressionResolver resolver, String datafeedId,
                                String jobId, List<String> datafeedIndices, IndicesOptions indicesOptions) {
        PersistentTasksCustomMetadata tasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        this.datafeedId = datafeedId;
        this.jobId = jobId;
        this.datafeedIndices = datafeedIndices;
        this.jobTask = MlTasks.getJobTask(jobId, tasks);
        this.clusterState = Objects.requireNonNull(clusterState);
        this.resolver = Objects.requireNonNull(resolver);
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
    }

    public void checkDatafeedTaskCanBeCreated() {
        if (MlMetadata.getMlMetadata(clusterState).isUpgradeMode()) {
            String msg = "Unable to start datafeed [" + datafeedId +"] explanation [" + AWAITING_UPGRADE.getExplanation() + "]";
            LOGGER.debug(msg);
            Exception detail = new IllegalStateException(msg);
            throw new ElasticsearchStatusException("Could not start datafeed [" + datafeedId +"] as indices are being upgraded",
                RestStatus.TOO_MANY_REQUESTS, detail);
        }
        AssignmentFailure assignmentFailure = checkAssignment();
        if (assignmentFailure != null && assignmentFailure.isCriticalForTaskCreation) {
            String msg = "No node found to start datafeed [" + datafeedId + "], " +
                    "allocation explanation [" + assignmentFailure.reason + "]";
            LOGGER.debug(msg);
            throw ExceptionsHelper.conflictStatusException(msg);
        }
    }

    public PersistentTasksCustomMetadata.Assignment selectNode() {
        if (MlMetadata.getMlMetadata(clusterState).isUpgradeMode()) {
            return AWAITING_UPGRADE;
        }

        AssignmentFailure assignmentFailure = checkAssignment();
        if (assignmentFailure == null) {
            String jobNode = jobTask.getExecutorNode();
            if (jobNode == null) {
                return AWAITING_JOB_ASSIGNMENT;
            }
            return new PersistentTasksCustomMetadata.Assignment(jobNode, "");
        }
        LOGGER.debug(assignmentFailure.reason);
        assert assignmentFailure.reason.isEmpty() == false;
        return new PersistentTasksCustomMetadata.Assignment(null, assignmentFailure.reason);
    }

    @Nullable
    private AssignmentFailure checkAssignment() {
        PriorityFailureCollector priorityFailureCollector = new PriorityFailureCollector();
        priorityFailureCollector.add(verifyIndicesActive());

        JobTaskState jobTaskState = null;
        JobState jobState = JobState.CLOSED;
        if (jobTask != null) {
            jobTaskState = (JobTaskState) jobTask.getState();
            jobState = jobTaskState == null ? JobState.OPENING : jobTaskState.getState();
        }

        if (jobState.isAnyOf(JobState.OPENING, JobState.OPENED) == false) {
            // lets try again later when the job has been opened:
            String reason = "cannot start datafeed [" + datafeedId + "], because the job's [" + jobId
                    + "] state is [" + jobState +  "] while state [" + JobState.OPENED + "] is required";
            priorityFailureCollector.add(new AssignmentFailure(reason, true));
        }

        if (jobTaskState != null && jobTaskState.isStatusStale(jobTask)) {
            String reason = "cannot start datafeed [" + datafeedId + "], because the job's [" + jobId
                    + "] state is stale";
            priorityFailureCollector.add(new AssignmentFailure(reason, true));
        }

        return priorityFailureCollector.get();
    }

    @Nullable
    private AssignmentFailure verifyIndicesActive() {
        for (String index : datafeedIndices) {

            if (RemoteClusterLicenseChecker.isRemoteIndex(index)) {
                // We cannot verify remote indices
                continue;
            }

            String[] concreteIndices;

            try {
                concreteIndices = resolver.concreteIndexNames(clusterState, indicesOptions, index);
                if (concreteIndices.length == 0) {
                    return new AssignmentFailure("cannot start datafeed [" + datafeedId + "] because index ["
                        + index + "] does not exist, is closed, or is still initializing.", true);
                }
            } catch (Exception e) {
                String msg = new ParameterizedMessage("failed resolving indices given [{}] and indices_options [{}]",
                    index,
                    indicesOptions).getFormattedMessage();
                LOGGER.debug("[" + datafeedId + "] " + msg, e);
                return new AssignmentFailure(
                    "cannot start datafeed [" + datafeedId + "] because it " + msg + " with exception [" + e.getMessage() + "]",
                    true);
            }

            for (String concreteIndex : concreteIndices) {
                IndexRoutingTable routingTable = clusterState.getRoutingTable().index(concreteIndex);
                if (routingTable == null || !routingTable.allPrimaryShardsActive()) {
                    return new AssignmentFailure("cannot start datafeed [" + datafeedId + "] because index ["
                        + concreteIndex + "] does not have all primary shards active yet.", false);
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
