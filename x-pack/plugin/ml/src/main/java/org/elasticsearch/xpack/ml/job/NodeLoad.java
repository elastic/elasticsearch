/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.Objects;

public class NodeLoad {

    private static final Logger logger = LogManager.getLogger(NodeLoadDetector.class);

    private final long maxMemory;
    private final int maxJobs;
    private final String nodeId;
    private final boolean useMemory;
    private final String error;
    private final int numAssignedAnomalyDetectorJobs;
    private final int numAssignedDataFrameAnalyticsJobs;
    private final int numAssignedNativeInferenceModels;
    private final long assignedNativeCodeOverheadMemory;
    private final long assignedAnomalyDetectorMemory;
    private final long assignedDataFrameAnalyticsMemory;
    private final long assignedNativeInferenceMemory;
    private final int numAllocatingJobs;

    NodeLoad(
        long maxMemory,
        int maxJobs,
        String nodeId,
        boolean useMemory,
        String error,
        int numAssignedAnomalyDetectorJobs,
        int numAssignedDataFrameAnalyticsJobs,
        int numAssignedNativeInferenceModels,
        long assignedNativeCodeOverheadMemory,
        long assignedAnomalyDetectorMemory,
        long assignedDataFrameAnalyticsMemory,
        long assignedNativeInferenceMemory,
        int numAllocatingJobs
    ) {
        this.maxMemory = maxMemory;
        this.maxJobs = maxJobs;
        this.nodeId = nodeId;
        this.useMemory = useMemory;
        this.error = error;
        this.numAssignedAnomalyDetectorJobs = numAssignedAnomalyDetectorJobs;
        this.numAssignedDataFrameAnalyticsJobs = numAssignedDataFrameAnalyticsJobs;
        this.numAssignedNativeInferenceModels = numAssignedNativeInferenceModels;
        this.assignedNativeCodeOverheadMemory = assignedNativeCodeOverheadMemory;
        this.assignedAnomalyDetectorMemory = assignedAnomalyDetectorMemory;
        this.assignedDataFrameAnalyticsMemory = assignedDataFrameAnalyticsMemory;
        this.assignedNativeInferenceMemory = assignedNativeInferenceMemory;
        this.numAllocatingJobs = numAllocatingJobs;
    }

    /**
     * @return The total number of assigned jobs and models
     */
    public int getNumAssignedJobsAndModels() {
        return numAssignedAnomalyDetectorJobs + numAssignedDataFrameAnalyticsJobs + numAssignedNativeInferenceModels;
    }

    /**
     * @return The total memory in bytes used by all assigned jobs.
     */
    public long getAssignedJobMemory() {
        return assignedNativeCodeOverheadMemory + assignedAnomalyDetectorMemory + assignedDataFrameAnalyticsMemory
            + assignedNativeInferenceMemory;
    }

    /**
     * @return The total memory in bytes used by all assigned jobs excluding the per-node overhead.
     */
    public long getAssignedJobMemoryExcludingPerNodeOverhead() {
        return assignedAnomalyDetectorMemory + assignedDataFrameAnalyticsMemory + assignedNativeInferenceMemory;
    }

    /**
     * @return The native code overhead, if any, for native processes on this node.
     */
    public long getAssignedNativeCodeOverheadMemory() {
        return assignedNativeCodeOverheadMemory;
    }

    /**
     * @return The total memory in bytes used by the assigned anomaly detectors.
     */
    public long getAssignedAnomalyDetectorMemory() {
        return assignedAnomalyDetectorMemory;
    }

    /**
     * @return The total memory in bytes used by the assigned data frame analytics jobs.
     */
    public long getAssignedDataFrameAnalyticsMemory() {
        return assignedDataFrameAnalyticsMemory;
    }

    /**
     * @return The total memory in bytes used by the assigned native inference processes.
     */
    public long getAssignedNativeInferenceMemory() {
        return assignedNativeInferenceMemory;
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
     * @return The available memory on the node
     */
    public long getFreeMemory() {
        return Math.max(maxMemory - getAssignedJobMemory(), 0L);
    }

    /**
     * @return The available memory on the node, taking into account the effect of the per-node overhead that
     *         is required if any ML process is running on the node. Effectively, this means unconditionally
     *         subtracting the per-node overhead from the available memory, regardless of whether any ML process
     *         is currently running.
     */
    public long getFreeMemoryExcludingPerNodeOverhead() {
        return Math.max(
            maxMemory - getAssignedJobMemoryExcludingPerNodeOverhead() - MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            0L
        );
    }

    /**
     * @return The number of jobs that can still be assigned to the node
     */
    public int remainingJobs() {
        // Native inference jobs use their own thread pool so they should not account towards the limit of open jobs.
        return Math.max(maxJobs - (getNumAssignedJobsAndModels() - numAssignedNativeInferenceModels), 0);
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
    public int getNumAllocatingJobs() {
        return numAllocatingJobs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeLoad nodeLoad = (NodeLoad) o;
        return maxMemory == nodeLoad.maxMemory
            && maxJobs == nodeLoad.maxJobs
            && useMemory == nodeLoad.useMemory
            && numAssignedAnomalyDetectorJobs == nodeLoad.numAssignedAnomalyDetectorJobs
            && numAssignedDataFrameAnalyticsJobs == nodeLoad.numAssignedDataFrameAnalyticsJobs
            && numAssignedNativeInferenceModels == nodeLoad.numAssignedNativeInferenceModels
            && assignedNativeCodeOverheadMemory == nodeLoad.assignedNativeCodeOverheadMemory
            && assignedAnomalyDetectorMemory == nodeLoad.assignedAnomalyDetectorMemory
            && assignedDataFrameAnalyticsMemory == nodeLoad.assignedDataFrameAnalyticsMemory
            && assignedNativeInferenceMemory == nodeLoad.assignedNativeInferenceMemory
            && numAllocatingJobs == nodeLoad.numAllocatingJobs
            && Objects.equals(nodeId, nodeLoad.nodeId)
            && Objects.equals(error, nodeLoad.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            maxMemory,
            maxJobs,
            nodeId,
            useMemory,
            error,
            numAssignedAnomalyDetectorJobs,
            numAssignedDataFrameAnalyticsJobs,
            numAssignedNativeInferenceModels,
            assignedNativeCodeOverheadMemory,
            assignedAnomalyDetectorMemory,
            assignedDataFrameAnalyticsMemory,
            assignedNativeInferenceMemory,
            numAllocatingJobs
        );
    }

    public static Builder builder(String nodeId) {
        return new Builder(nodeId);
    }

    public static Builder builder(NodeLoad nodeLoad) {
        return new Builder(nodeLoad);
    }

    public static class Builder {
        private long maxMemory;
        private int maxJobs;
        private final String nodeId;
        private boolean useMemory;
        private String error;
        private int numAssignedAnomalyDetectorJobs;
        private int numAssignedDataFrameAnalyticsJobs;
        private int numAssignedNativeInferenceModels;
        private long assignedNativeCodeOverheadMemory;
        private long assignedAnomalyDetectorMemory;
        private long assignedDataFrameAnalyticsMemory;
        private long assignedNativeInferenceMemory;
        private int numAllocatingJobs;

        public Builder(NodeLoad nodeLoad) {
            this.maxMemory = nodeLoad.maxMemory;
            this.maxJobs = nodeLoad.maxJobs;
            this.nodeId = nodeLoad.nodeId;
            this.useMemory = nodeLoad.useMemory;
            this.error = nodeLoad.error;
            this.numAssignedAnomalyDetectorJobs = nodeLoad.numAssignedAnomalyDetectorJobs;
            this.numAssignedDataFrameAnalyticsJobs = nodeLoad.numAssignedDataFrameAnalyticsJobs;
            this.numAssignedNativeInferenceModels = nodeLoad.numAssignedNativeInferenceModels;
            this.assignedNativeCodeOverheadMemory = nodeLoad.assignedNativeCodeOverheadMemory;
            this.assignedAnomalyDetectorMemory = nodeLoad.assignedAnomalyDetectorMemory;
            this.assignedDataFrameAnalyticsMemory = nodeLoad.assignedDataFrameAnalyticsMemory;
            this.assignedNativeInferenceMemory = nodeLoad.assignedNativeInferenceMemory;
            this.numAllocatingJobs = nodeLoad.numAllocatingJobs;
        }

        public Builder(String nodeId) {
            this.nodeId = nodeId;
        }

        public long getFreeMemory() {
            return Math.max(
                maxMemory - assignedNativeCodeOverheadMemory - assignedAnomalyDetectorMemory - assignedDataFrameAnalyticsMemory
                    - assignedNativeInferenceMemory,
                0L
            );
        }

        public int remainingJobs() {
            // Native inference jobs use their own thread pool so they should not account towards the limit of open jobs.
            return Math.max(maxJobs - (getNumAssignedJobs() - numAssignedNativeInferenceModels), 0);
        }

        public String getNodeId() {
            return nodeId;
        }

        public int getNumAssignedJobs() {
            return numAssignedAnomalyDetectorJobs + numAssignedDataFrameAnalyticsJobs + numAssignedNativeInferenceModels;
        }

        public Builder setMaxMemory(long maxMemory) {
            this.maxMemory = maxMemory;
            return this;
        }

        public long getMaxMemory() {
            return maxMemory;
        }

        public Builder setMaxJobs(int maxJobs) {
            this.maxJobs = maxJobs;
            return this;
        }

        public Builder setUseMemory(boolean useMemory) {
            this.useMemory = useMemory;
            return this;
        }

        public Builder setError(String error) {
            this.error = error;
            return this;
        }

        public Builder incNumAssignedAnomalyDetectorJobs() {
            ++this.numAssignedAnomalyDetectorJobs;
            return this;
        }

        public Builder incNumAssignedDataFrameAnalyticsJobs() {
            ++this.numAssignedDataFrameAnalyticsJobs;
            return this;
        }

        public Builder incNumAssignedNativeInferenceModels() {
            ++this.numAssignedNativeInferenceModels;
            return this;
        }

        public Builder incAssignedNativeCodeOverheadMemory(long assignedNativeCodeOverheadMemory) {
            this.assignedNativeCodeOverheadMemory += assignedNativeCodeOverheadMemory;
            return this;
        }

        public Builder incAssignedAnomalyDetectorMemory(long assignedAnomalyDetectorMemory) {
            this.assignedAnomalyDetectorMemory += assignedAnomalyDetectorMemory;
            return this;
        }

        public Builder incAssignedDataFrameAnalyticsMemory(long assignedDataFrameAnalyticsMemory) {
            this.assignedDataFrameAnalyticsMemory += assignedDataFrameAnalyticsMemory;
            return this;
        }

        public Builder incAssignedNativeInferenceMemory(long assignedNativeInferenceMemory) {
            this.assignedNativeInferenceMemory += assignedNativeInferenceMemory;
            return this;
        }

        public Builder incNumAllocatingJobs() {
            ++this.numAllocatingJobs;
            return this;
        }

        void addTask(String taskName, String taskId, boolean isAllocating, MlMemoryTracker memoryTracker) {
            switch (taskName) {
                case MlTasks.JOB_TASK_NAME, MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME -> incNumAssignedAnomalyDetectorJobs();
                case MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME -> incNumAssignedDataFrameAnalyticsJobs();
                default -> {
                    assert false : "Unexpected task could not be accounted for: " + taskName;
                }
            }

            if (isAllocating) {
                ++numAllocatingJobs;
            }
            Long jobMemoryRequirement = memoryTracker.getJobMemoryRequirement(taskName, taskId);
            if (jobMemoryRequirement == null) {
                useMemory = false;
                logger.debug("[{}] task memory requirement was not available.", taskId);
            } else {
                switch (taskName) {
                    case MlTasks.JOB_TASK_NAME, MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME -> assignedAnomalyDetectorMemory +=
                        jobMemoryRequirement;
                    case MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME -> assignedDataFrameAnalyticsMemory += jobMemoryRequirement;
                    default -> {
                        assert false : "ML memory-requiring task name not handled: " + taskName;
                        // If this ever happens in production then this is better than nothing, but
                        // hopefully the assertion will mean we pick up any omission in testing
                        assignedAnomalyDetectorMemory += jobMemoryRequirement;
                    }
                }
            }
        }

        public NodeLoad build() {
            return new NodeLoad(
                maxMemory,
                maxJobs,
                nodeId,
                useMemory,
                error,
                numAssignedAnomalyDetectorJobs,
                numAssignedDataFrameAnalyticsJobs,
                numAssignedNativeInferenceModels,
                assignedNativeCodeOverheadMemory,
                assignedAnomalyDetectorMemory,
                assignedDataFrameAnalyticsMemory,
                assignedNativeInferenceMemory,
                numAllocatingJobs
            );
        }
    }
}
