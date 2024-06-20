/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.Strings.arrayToCommaDelimitedString;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ml.MlTasks.AWAITING_UPGRADE;
import static org.elasticsearch.xpack.core.ml.MlTasks.RESET_IN_PROGRESS;

public class DatafeedNodeSelector {

    private static final Logger LOGGER = LogManager.getLogger(DatafeedNodeSelector.class);

    public static final PersistentTasksCustomMetadata.Assignment AWAITING_JOB_ASSIGNMENT = new PersistentTasksCustomMetadata.Assignment(
        null,
        "datafeed awaiting job assignment.",
        PersistentTasksCustomMetadata.Explanation.WAITING_FOR_INITIAL_ASSIGNMENT
    );
    public static final PersistentTasksCustomMetadata.Assignment AWAITING_JOB_RELOCATION = new PersistentTasksCustomMetadata.Assignment(
        null,
        "datafeed awaiting job relocation.",
        PersistentTasksCustomMetadata.Explanation.AWAITING_REASSIGNMENT
    );

    private final String datafeedId;
    private final String jobId;
    private final List<String> datafeedIndices;
    private final PersistentTasksCustomMetadata.PersistentTask<?> jobTask;
    private final ClusterState clusterState;
    private final IndexNameExpressionResolver resolver;
    private final IndicesOptions indicesOptions;

    public DatafeedNodeSelector(
        ClusterState clusterState,
        IndexNameExpressionResolver resolver,
        String datafeedId,
        String jobId,
        List<String> datafeedIndices,
        IndicesOptions indicesOptions
    ) {
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
            String msg = "Unable to start datafeed ["
                + datafeedId
                + "] explanation ["
                + AWAITING_UPGRADE.getExplanationCodesAndExplanation()
                + "]";
            LOGGER.debug(msg);
            Exception detail = new IllegalStateException(msg);
            throw new ElasticsearchStatusException(
                "Could not start datafeed [" + datafeedId + "] as indices are being upgraded",
                RestStatus.TOO_MANY_REQUESTS,
                detail
            );
        }
        AssignmentFailure assignmentFailure = checkAssignment();
        if (assignmentFailure != null && assignmentFailure.isCriticalForTaskCreation) {
            String msg = "No node found to start datafeed ["
                + datafeedId
                + "], "
                + "allocation explanation ["
                + assignmentFailure.reason
                + "]";
            LOGGER.debug(msg);
            throw ExceptionsHelper.conflictStatusException(msg);
        }
    }

    /**
     * Select which node to run the datafeed on.  The logic is to always choose the same node that the job
     * is already running on <em>unless</em> this node is not permitted for some reason or there is some
     * problem in the cluster that would stop the datafeed working.
     * @param candidateNodes Only nodes in this collection may be chosen as the executor node.
     * @return The assignment for the datafeed, containing either an executor node or a reason why an
     *         executor node was not returned.
     */
    public PersistentTasksCustomMetadata.Assignment selectNode(Collection<DiscoveryNode> candidateNodes) {
        if (MlMetadata.getMlMetadata(clusterState).isUpgradeMode()) {
            return AWAITING_UPGRADE;
        }
        if (MlMetadata.getMlMetadata(clusterState).isResetMode()) {
            return RESET_IN_PROGRESS;
        }

        AssignmentFailure assignmentFailure = checkAssignment();
        if (assignmentFailure == null) {
            String jobNode = jobTask.getExecutorNode();
            if (jobNode == null) {
                return AWAITING_JOB_ASSIGNMENT;
            }
            // During node shutdown the datafeed will have been unassigned but the job will still be gracefully persisting state.
            // During this time the datafeed will be trying to select the job's node, but we must disallow this. Instead the
            // datafeed must remain in limbo until the job has finished persisting state and can move to a different node.
            // Nodes that are shutting down will have been excluded from the candidate nodes.
            if (candidateNodes.stream().anyMatch(candidateNode -> candidateNode.getId().equals(jobNode)) == false) {
                return AWAITING_JOB_RELOCATION;
            }
            return new PersistentTasksCustomMetadata.Assignment(jobNode, PersistentTasksCustomMetadata.Explanation.ASSIGNMENT_SUCCESSFUL);
        }
        LOGGER.debug(assignmentFailure.reason);
        assert assignmentFailure.reason != null;
        assert assignmentFailure.details.isEmpty() == false;
        return new PersistentTasksCustomMetadata.Assignment(null, assignmentFailure.details, assignmentFailure.reason);
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
            // let's try again later when the job has been opened:
            String reason = "cannot start datafeed ["
                + datafeedId
                + "], because the job's ["
                + jobId
                + "] state is ["
                + jobState
                + "] while state ["
                + JobState.OPENED
                + "] is required";
            priorityFailureCollector.add(
                new AssignmentFailure(PersistentTasksCustomMetadata.Explanation.DATAFEED_JOB_STATE_NOT_OPEN, reason, true)
            );
        }

        if (jobTaskState != null && jobTaskState.isStatusStale(jobTask)) {
            String reason = "cannot start datafeed [" + datafeedId + "], because the job's [" + jobId + "] state is stale";
            priorityFailureCollector.add(new AssignmentFailure(PersistentTasksCustomMetadata.Explanation.DATAFEED_JOB_STALE, reason, true));
        }

        return priorityFailureCollector.get();
    }

    @Nullable
    private AssignmentFailure verifyIndicesActive() {
        boolean hasRemoteIndices = datafeedIndices.stream().anyMatch(RemoteClusterLicenseChecker::isRemoteIndex);
        String[] index = datafeedIndices.stream()
            // We cannot verify remote indices
            .filter(i -> RemoteClusterLicenseChecker.isRemoteIndex(i) == false)
            .toArray(String[]::new);

        final String[] concreteIndices;

        try {
            concreteIndices = resolver.concreteIndexNames(clusterState, indicesOptions, true, index);

            // If we have remote indices we cannot check those. We should not fail as they may contain data.
            if (hasRemoteIndices == false && concreteIndices.length == 0) {
                return new AssignmentFailure(
                    PersistentTasksCustomMetadata.Explanation.DATAFEED_INDEX_NOT_FOUND,
                    "cannot start datafeed ["
                        + datafeedId
                        + "] because index ["
                        + Strings.arrayToCommaDelimitedString(index)
                        + "] does not exist, is closed, or is still initializing.",
                    true
                );
            }
        } catch (Exception e) {
            String msg = format(
                "failed resolving indices given [%s] and indices_options [%s]",
                arrayToCommaDelimitedString(index),
                indicesOptions
            );
            LOGGER.debug("[" + datafeedId + "] " + msg, e);
            return new AssignmentFailure(
                PersistentTasksCustomMetadata.Explanation.DATAFEED_RESOLVING_INDEX_THREW_EXCEPTION,
                "cannot start datafeed [" + datafeedId + "] because it " + msg + " with exception [" + e.getMessage() + "]",
                true
            );
        }

        for (String concreteIndex : concreteIndices) {
            IndexRoutingTable routingTable = clusterState.getRoutingTable().index(concreteIndex);
            if (routingTable == null
                || routingTable.allPrimaryShardsActive() == false
                || routingTable.readyForSearch(clusterState) == false) {
                return new AssignmentFailure(
                    PersistentTasksCustomMetadata.Explanation.PRIMARY_SHARDS_NOT_ACTIVE,
                    "cannot start datafeed ["
                        + datafeedId
                        + "] because index ["
                        + concreteIndex
                        + "] does not have all primary shards active yet.",
                    false
                );
            }
        }
        return null;
    }

    private static class AssignmentFailure {
        private final PersistentTasksCustomMetadata.Explanation reason;
        private final String details;
        private final boolean isCriticalForTaskCreation;

        private AssignmentFailure(PersistentTasksCustomMetadata.Explanation reason, String details, boolean isCriticalForTaskCreation) {
            this.reason = reason;
            this.details = details;
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
