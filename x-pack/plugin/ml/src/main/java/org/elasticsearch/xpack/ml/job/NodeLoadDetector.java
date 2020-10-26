/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;


public class NodeLoadDetector {
    private static final Logger logger = LogManager.getLogger(NodeLoadDetector.class);

    private final MlMemoryTracker mlMemoryTracker;

    public NodeLoadDetector(MlMemoryTracker memoryTracker) {
        this.mlMemoryTracker = memoryTracker;
    }

    public MlMemoryTracker getMlMemoryTracker() {
        return mlMemoryTracker;
    }

    public NodeLoad detectNodeLoad(ClusterState clusterState,
                                   boolean allNodesHaveDynamicMaxWorkers,
                                   DiscoveryNode node,
                                   int dynamicMaxOpenJobs,
                                   int maxMachineMemoryPercent,
                                   boolean isMemoryTrackerRecentlyRefreshed,
                                   boolean useAutoMachineMemoryCalculation) {
        PersistentTasksCustomMetadata persistentTasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        Map<String, String> nodeAttributes = node.getAttributes();
        List<String> errors = new ArrayList<>();
        int maxNumberOfOpenJobs = dynamicMaxOpenJobs;
        // TODO: remove this in 8.0.0
        if (allNodesHaveDynamicMaxWorkers == false) {
            String maxNumberOfOpenJobsStr = nodeAttributes.get(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR);
            try {
                maxNumberOfOpenJobs = Integer.parseInt(maxNumberOfOpenJobsStr);
            } catch (NumberFormatException e) {
                errors.add(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR + " attribute [" + maxNumberOfOpenJobsStr + "] is not an integer");
                maxNumberOfOpenJobs = -1;
            }
        }
        OptionalLong maxMlMemory = NativeMemoryCalculator.allowedBytesForMl(node,
            maxMachineMemoryPercent,
            useAutoMachineMemoryCalculation);
        if (maxMlMemory.isEmpty()) {
            errors.add(MachineLearning.MACHINE_MEMORY_NODE_ATTR
                + " attribute ["
                + nodeAttributes.get(MachineLearning.MACHINE_MEMORY_NODE_ATTR)
                + "] is not a long");
        }

        NodeLoad nodeLoad = new NodeLoad(node.getId(), maxMlMemory.orElse(-1L), maxNumberOfOpenJobs, isMemoryTrackerRecentlyRefreshed);
        if (errors.isEmpty() == false) {
            nodeLoad.error = Strings.collectionToCommaDelimitedString(errors);
            return nodeLoad;
        }
        updateLoadGivenTasks(nodeLoad, persistentTasks);
        return nodeLoad;
    }

    private void updateLoadGivenTasks(NodeLoad nodeLoad, PersistentTasksCustomMetadata persistentTasks) {
        if (persistentTasks != null) {
            // find all the anomaly detector job tasks assigned to this node
            Collection<PersistentTasksCustomMetadata.PersistentTask<?>> assignedAnomalyDetectorTasks = persistentTasks.findTasks(
                MlTasks.JOB_TASK_NAME, task -> nodeLoad.getNodeId().equals(task.getExecutorNode()));
            for (PersistentTasksCustomMetadata.PersistentTask<?> assignedTask : assignedAnomalyDetectorTasks) {
                JobState jobState = MlTasks.getJobStateModifiedForReassignments(assignedTask);
                if (jobState.isAnyOf(JobState.CLOSED, JobState.FAILED) == false) {
                    // Don't count CLOSED or FAILED jobs, as they don't consume native memory
                    ++nodeLoad.numAssignedJobs;
                    if (jobState == JobState.OPENING) {
                        ++nodeLoad.numAllocatingJobs;
                    }
                    OpenJobAction.JobParams params = (OpenJobAction.JobParams) assignedTask.getParams();
                    Long jobMemoryRequirement = mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(params.getJobId());
                    if (jobMemoryRequirement == null) {
                        nodeLoad.useMemory = false;
                        logger.debug(() -> new ParameterizedMessage(
                            "[{}] memory requirement was not available. Calculating load by number of assigned jobs.",
                            params.getJobId()
                        ));
                    } else {
                        nodeLoad.assignedJobMemory += jobMemoryRequirement;
                    }
                }
            }
            // find all the data frame analytics job tasks assigned to this node
            Collection<PersistentTasksCustomMetadata.PersistentTask<?>> assignedAnalyticsTasks = persistentTasks.findTasks(
                MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, task -> nodeLoad.getNodeId().equals(task.getExecutorNode()));
            for (PersistentTasksCustomMetadata.PersistentTask<?> assignedTask : assignedAnalyticsTasks) {
                DataFrameAnalyticsState dataFrameAnalyticsState = MlTasks.getDataFrameAnalyticsState(assignedTask);

                // Don't count stopped and failed df-analytics tasks as they don't consume native memory
                if (dataFrameAnalyticsState.isAnyOf(DataFrameAnalyticsState.STOPPED, DataFrameAnalyticsState.FAILED) == false) {
                    // The native process is only running in the ANALYZING and STOPPING states, but in the STARTED
                    // and REINDEXING states we're committed to using the memory soon, so account for it here
                    ++nodeLoad.numAssignedJobs;
                    StartDataFrameAnalyticsAction.TaskParams params =
                        (StartDataFrameAnalyticsAction.TaskParams) assignedTask.getParams();
                    Long jobMemoryRequirement = mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(params.getId());
                    if (jobMemoryRequirement == null) {
                        nodeLoad.useMemory = false;
                        logger.debug(() -> new ParameterizedMessage(
                            "[{}] memory requirement was not available. Calculating load by number of assigned jobs.",
                            params.getId()
                        ));
                    } else {
                        nodeLoad.assignedJobMemory += jobMemoryRequirement;
                    }
                }
            }
            // if any jobs are running then the native code will be loaded, but shared between all jobs,
            // so increase the total memory usage of the assigned jobs to account for this
            if (nodeLoad.numAssignedJobs > 0) {
                nodeLoad.assignedJobMemory += MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();
            }
        }
    }

    public static class NodeLoad {
        private final long maxMemory;
        private final int maxJobs;
        private final String nodeId;
        private boolean useMemory;
        private String error;
        private long numAssignedJobs;
        private long assignedJobMemory;
        private long numAllocatingJobs;

        private NodeLoad(String nodeId, long maxMemory, int maxJobs, boolean useMemory) {
            this.maxJobs = maxJobs;
            this.maxMemory = maxMemory;
            this.nodeId = nodeId;
            this.useMemory = useMemory;
        }

        /**
         * @return The total number of assigned jobs
         */
        public long getNumAssignedJobs() {
            return numAssignedJobs;
        }

        /**
         * @return The total memory in bytes used by the assigned jobs.
         */
        public long getAssignedJobMemory() {
            return assignedJobMemory;
        }

        /**
         * @return The maximum memory on this node for jobs
         */
        public long getMaxMlMemory() {
            return maxMemory;
        }

        /**
         * @return The maximum number of jobs allowed on the node
         */
        public int getMaxJobs() {
            return maxJobs;
        }

        /**
         * @return returns `true` if the assignedJobMemory number is accurate
         */
        public boolean isUseMemory() {
            return useMemory;
        }

        /**
         * @return The node ID
         */
        public String getNodeId() {
            return nodeId;
        }

        /**
         * @return Returns a comma delimited string of errors if any were encountered.
         */
        @Nullable
        public String getError() {
            return error;
        }

        /**
         * @return The current number of jobs allocating to the node
         */
        public long getNumAllocatingJobs() {
            return numAllocatingJobs;
        }
    }

}
