/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.ml.MachineLearning.MAX_OPEN_JOBS_PER_NODE;

/**
 * Class that contains the logic to decide which node to assign each job to.
 *
 * The assignment rules are as follows:
 *
 * 1. Reject nodes that are not ML nodes
 * 2. Reject nodes for which the node filter returns a rejection reason
 * 3. Reject nodes where the new job would result in more than the permitted number of concurrent "opening" jobs
 * 4. Reject nodes where the new job would result in more than the permitted number of assigned jobs
 * 5. If assigning by memory, reject nodes where the new job would result in the permitted amount of memory being exceeded
 * 6. If assigning by memory, pick the node that remains after rejections that has the most remaining memory
 * 7. If assigning by count, pick the node that remains after rejections that has the fewest jobs assigned to it
 *
 * The decision on whether to assign by memory or by count is:
 * - If values are available for every node's memory size and every job's memory requirement then assign by memory
 * - Otherwise assign by count
 */
public class JobNodeSelector {

    private static final Logger logger = LogManager.getLogger(JobNodeSelector.class);

    private final String jobId;
    private final String taskName;
    private final ClusterState clusterState;
    private final MlMemoryTracker memoryTracker;
    private final Function<DiscoveryNode, String> nodeFilter;

    /**
     * @param nodeFilter Optionally a function that returns a reason beyond the general
     *                   reasons why a job cannot be assigned to a particular node.  May
     *                   be <code>null</code> if no such function is needed.
     */
    public JobNodeSelector(ClusterState clusterState, String jobId, String taskName, MlMemoryTracker memoryTracker,
                           Function<DiscoveryNode, String> nodeFilter) {
        this.jobId = Objects.requireNonNull(jobId);
        this.taskName = Objects.requireNonNull(taskName);
        this.clusterState = Objects.requireNonNull(clusterState);
        this.memoryTracker = Objects.requireNonNull(memoryTracker);
        this.nodeFilter = nodeFilter;
    }

    public PersistentTasksCustomMetaData.Assignment selectNode(int dynamicMaxOpenJobs, int maxConcurrentJobAllocations,
                                                               int maxMachineMemoryPercent, boolean isMemoryTrackerRecentlyRefreshed) {
        // TODO: remove in 8.0.0
        boolean allNodesHaveDynamicMaxWorkers = clusterState.getNodes().getMinNodeVersion().onOrAfter(Version.V_7_1_0);

        // Try to allocate jobs according to memory usage, but if that's not possible (maybe due to a mixed version cluster or maybe
        // because of some weird OS problem) then fall back to the old mechanism of only considering numbers of assigned jobs
        boolean allocateByMemory = isMemoryTrackerRecentlyRefreshed;
        if (isMemoryTrackerRecentlyRefreshed == false) {
            logger.warn("Falling back to allocating job [{}] by job counts because a memory requirement refresh could not be scheduled",
                jobId);
        }

        List<String> reasons = new LinkedList<>();
        long maxAvailableCount = Long.MIN_VALUE;
        long maxAvailableMemory = Long.MIN_VALUE;
        DiscoveryNode minLoadedNodeByCount = null;
        DiscoveryNode minLoadedNodeByMemory = null;
        PersistentTasksCustomMetaData persistentTasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (MachineLearning.isMlNode(node) == false) {
                String reason = "Not opening job [" + jobId + "] on node [" + nodeNameOrId(node)
                        + "], because this node isn't a ml node.";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            if (nodeFilter != null) {
                String reason = nodeFilter.apply(node);
                if (reason != null) {
                    logger.trace(reason);
                    reasons.add(reason);
                    continue;
                }
            }

            Tuple<long[], Boolean> currentLoad = calculateCurrentLoadForNode(node, persistentTasks, allocateByMemory);
            long numberOfAssignedJobs = currentLoad.v1()[0];
            long numberOfAllocatingJobs = currentLoad.v1()[1];
            long assignedJobMemory = currentLoad.v1()[2];
            allocateByMemory = currentLoad.v2();

            if (numberOfAllocatingJobs >= maxConcurrentJobAllocations) {
                String reason = "Not opening job [" + jobId + "] on node [" + nodeNameAndMlAttributes(node)
                        + "], because node exceeds [" + numberOfAllocatingJobs +
                        "] the maximum number of jobs [" + maxConcurrentJobAllocations + "] in opening state";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            Map<String, String> nodeAttributes = node.getAttributes();
            int maxNumberOfOpenJobs = dynamicMaxOpenJobs;
            // TODO: remove this in 8.0.0
            if (allNodesHaveDynamicMaxWorkers == false) {
                String maxNumberOfOpenJobsStr = nodeAttributes.get(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR);
                try {
                    maxNumberOfOpenJobs = Integer.parseInt(maxNumberOfOpenJobsStr);
                } catch (NumberFormatException e) {
                    String reason = "Not opening job [" + jobId + "] on node [" + nodeNameAndMlAttributes(node) + "], because " +
                        MachineLearning.MAX_OPEN_JOBS_NODE_ATTR + " attribute [" + maxNumberOfOpenJobsStr + "] is not an integer";
                    logger.trace(reason);
                    reasons.add(reason);
                    continue;
                }
            }
            long availableCount = maxNumberOfOpenJobs - numberOfAssignedJobs;
            if (availableCount == 0) {
                String reason = "Not opening job [" + jobId + "] on node [" + nodeNameAndMlAttributes(node)
                        + "], because this node is full. Number of opened jobs [" + numberOfAssignedJobs
                        + "], " + MAX_OPEN_JOBS_PER_NODE.getKey() + " [" + maxNumberOfOpenJobs + "]";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            if (maxAvailableCount < availableCount) {
                maxAvailableCount = availableCount;
                minLoadedNodeByCount = node;
            }

            String machineMemoryStr = nodeAttributes.get(MachineLearning.MACHINE_MEMORY_NODE_ATTR);
            long machineMemory;
            try {
                machineMemory = Long.parseLong(machineMemoryStr);
            } catch (NumberFormatException e) {
                String reason = "Not opening job [" + jobId + "] on node [" + nodeNameAndMlAttributes(node) + "], because " +
                    MachineLearning.MACHINE_MEMORY_NODE_ATTR + " attribute [" + machineMemoryStr + "] is not a long";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            if (allocateByMemory) {
                if (machineMemory > 0) {
                    long maxMlMemory = machineMemory * maxMachineMemoryPercent / 100;
                    Long estimatedMemoryFootprint = memoryTracker.getJobMemoryRequirement(taskName, jobId);
                    if (estimatedMemoryFootprint != null) {
                        long availableMemory = maxMlMemory - assignedJobMemory;
                        if (estimatedMemoryFootprint > availableMemory) {
                            String reason = "Not opening job [" + jobId + "] on node [" + nodeNameAndMlAttributes(node) +
                                "], because this node has insufficient available memory. Available memory for ML [" + maxMlMemory +
                                "], memory required by existing jobs [" + assignedJobMemory +
                                "], estimated memory required for this job [" + estimatedMemoryFootprint + "]";
                            logger.trace(reason);
                            reasons.add(reason);
                            continue;
                        }

                        if (maxAvailableMemory < availableMemory) {
                            maxAvailableMemory = availableMemory;
                            minLoadedNodeByMemory = node;
                        }
                    } else {
                        // If we cannot get the job memory requirement,
                        // fall back to simply allocating by job count
                        allocateByMemory = false;
                        logger.debug("Falling back to allocating job [{}] by job counts because its memory requirement was not available",
                            jobId);
                    }
                } else {
                    // If we cannot get the available memory on any machine in
                    // the cluster, fall back to simply allocating by job count
                    allocateByMemory = false;
                    logger.debug("Falling back to allocating job [{}] by job counts because machine memory was not available for node [{}]",
                        jobId, nodeNameAndMlAttributes(node));
                }
            }
        }
        DiscoveryNode minLoadedNode = allocateByMemory ? minLoadedNodeByMemory : minLoadedNodeByCount;
        if (minLoadedNode == null) {
            String explanation = String.join("|", reasons);
            logger.debug("no node selected for job [{}], reasons [{}]", jobId, explanation);
            return new PersistentTasksCustomMetaData.Assignment(null, explanation);
        }
        logger.debug("selected node [{}] for job [{}]", minLoadedNode, jobId);
        return new PersistentTasksCustomMetaData.Assignment(minLoadedNode.getId(), "");
    }

    private Tuple<long[], Boolean> calculateCurrentLoadForNode(DiscoveryNode node, PersistentTasksCustomMetaData persistentTasks,
                                                               boolean allocateByMemory) {
        long numberOfAssignedJobs = 0;
        long numberOfAllocatingJobs = 0;
        long assignedJobMemory = 0;

        if (persistentTasks != null) {
            // find all the anomaly detector job tasks assigned to this node
            Collection<PersistentTasksCustomMetaData.PersistentTask<?>> assignedAnomalyDetectorTasks = persistentTasks.findTasks(
                MlTasks.JOB_TASK_NAME, task -> node.getId().equals(task.getExecutorNode()));
            for (PersistentTasksCustomMetaData.PersistentTask<?> assignedTask : assignedAnomalyDetectorTasks) {
                JobState jobState = MlTasks.getJobStateModifiedForReassignments(assignedTask);
                if (jobState.isAnyOf(JobState.CLOSED, JobState.FAILED) == false) {
                    // Don't count CLOSED or FAILED jobs, as they don't consume native memory
                    ++numberOfAssignedJobs;
                    if (jobState == JobState.OPENING) {
                        ++numberOfAllocatingJobs;
                    }
                    OpenJobAction.JobParams params = (OpenJobAction.JobParams) assignedTask.getParams();
                    Long jobMemoryRequirement = memoryTracker.getAnomalyDetectorJobMemoryRequirement(params.getJobId());
                    if (jobMemoryRequirement == null) {
                        allocateByMemory = false;
                        logger.debug("Falling back to allocating job [{}] by job counts because " +
                            "the memory requirement for job [{}] was not available", jobId, params.getJobId());
                    } else {
                        assignedJobMemory += jobMemoryRequirement;
                    }
                }
            }
            // find all the data frame analytics job tasks assigned to this node
            Collection<PersistentTasksCustomMetaData.PersistentTask<?>> assignedAnalyticsTasks = persistentTasks.findTasks(
                MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, task -> node.getId().equals(task.getExecutorNode()));
            for (PersistentTasksCustomMetaData.PersistentTask<?> assignedTask : assignedAnalyticsTasks) {
                DataFrameAnalyticsState dataFrameAnalyticsState = ((DataFrameAnalyticsTaskState) assignedTask.getState()).getState();
                // TODO: skip FAILED here too if such a state is ever added
                if (dataFrameAnalyticsState != DataFrameAnalyticsState.STOPPED) {
                    // The native process is only running in the ANALYZING and STOPPING states, but in the STARTED
                    // and REINDEXING states we're committed to using the memory soon, so account for it here
                    ++numberOfAssignedJobs;
                    StartDataFrameAnalyticsAction.TaskParams params =
                        (StartDataFrameAnalyticsAction.TaskParams) assignedTask.getParams();
                    Long jobMemoryRequirement = memoryTracker.getDataFrameAnalyticsJobMemoryRequirement(params.getId());
                    if (jobMemoryRequirement == null) {
                        allocateByMemory = false;
                        logger.debug("Falling back to allocating job [{}] by job counts because " +
                            "the memory requirement for job [{}] was not available", jobId, params.getId());
                    } else {
                        assignedJobMemory += jobMemoryRequirement;
                    }
                }
            }
        }

        return new Tuple<>(new long[] { numberOfAssignedJobs, numberOfAllocatingJobs, assignedJobMemory }, allocateByMemory);
    }

    static String nodeNameOrId(DiscoveryNode node) {
        String nodeNameOrID = node.getName();
        if (Strings.isNullOrEmpty(nodeNameOrID)) {
            nodeNameOrID = node.getId();
        }
        return nodeNameOrID;
    }

    public static String nodeNameAndVersion(DiscoveryNode node) {
        String nodeNameOrID = nodeNameOrId(node);
        StringBuilder builder = new StringBuilder("{").append(nodeNameOrID).append('}');
        builder.append('{').append("version=").append(node.getVersion()).append('}');
        return builder.toString();
    }

    static String nodeNameAndMlAttributes(DiscoveryNode node) {
        String nodeNameOrID = nodeNameOrId(node);

        StringBuilder builder = new StringBuilder("{").append(nodeNameOrID).append('}');
        for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
            if (entry.getKey().startsWith("ml.") || entry.getKey().equals("node.ml")) {
                builder.append('{').append(entry).append('}');
            }
        }
        return builder.toString();
    }
}
