/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.Objects;

public class NodeLoad {

    private static final Logger logger = LogManager.getLogger(NodeLoadDetector.class);

    private final long maxMemory;
    private final int maxJobs;
    private final String nodeId;
    private final boolean useMemory;
    private final String error;
    private final long numAssignedJobs;
    private final long assignedJobMemory;
    private final long numAllocatingJobs;

    NodeLoad(long maxMemory,
             int maxJobs,
             String nodeId,
             boolean useMemory,
             String error,
             long numAssignedJobs,
             long assignedJobMemory,
             long numAllocatingJobs) {
        this.maxMemory = maxMemory;
        this.maxJobs = maxJobs;
        this.nodeId = nodeId;
        this.useMemory = useMemory;
        this.error = error;
        this.numAssignedJobs = numAssignedJobs;
        this.assignedJobMemory = assignedJobMemory;
        this.numAllocatingJobs = numAllocatingJobs;
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
     * @return The available memory on the node
     */
    public long getFreeMemory() {
        return Math.max(maxMemory - assignedJobMemory, 0L);
    }

    /**
     * @return The number of jobs that can still be assigned to the node
     */
    public int remainingJobs() {
        return Math.max(maxJobs - (int)numAssignedJobs, 0);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeLoad nodeLoad = (NodeLoad) o;
        return maxMemory == nodeLoad.maxMemory &&
            maxJobs == nodeLoad.maxJobs &&
            useMemory == nodeLoad.useMemory &&
            numAssignedJobs == nodeLoad.numAssignedJobs &&
            assignedJobMemory == nodeLoad.assignedJobMemory &&
            numAllocatingJobs == nodeLoad.numAllocatingJobs &&
            Objects.equals(nodeId, nodeLoad.nodeId) &&
            Objects.equals(error, nodeLoad.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxMemory, maxJobs, nodeId, useMemory, error, numAssignedJobs, assignedJobMemory, numAllocatingJobs);
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
        private long numAssignedJobs;
        private long assignedJobMemory;
        private long numAllocatingJobs;

        public Builder(NodeLoad nodeLoad) {
            this.maxMemory = nodeLoad.maxMemory;
            this.maxJobs = nodeLoad.maxJobs;
            this.nodeId = nodeLoad.nodeId;
            this.useMemory = nodeLoad.useMemory;
            this.error = nodeLoad.error;
            this.numAssignedJobs = nodeLoad.numAssignedJobs;
            this.assignedJobMemory = nodeLoad.assignedJobMemory;
            this.numAllocatingJobs = nodeLoad.numAllocatingJobs;
        }

        public Builder(String nodeId) {
            this.nodeId = nodeId;
        }

        public long getFreeMemory() {
            return Math.max(maxMemory - assignedJobMemory, 0L);
        }

        public int remainingJobs() {
            return Math.max(maxJobs - (int)numAssignedJobs, 0);
        }

        public String getNodeId() {
            return nodeId;
        }

        public long getNumAssignedJobs() {
            return numAssignedJobs;
        }

        public Builder setMaxMemory(long maxMemory) {
            this.maxMemory = maxMemory;
            return this;
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

        public Builder incNumAssignedJobs() {
            ++this.numAssignedJobs;
            return this;
        }

        public Builder incAssignedJobMemory(long assignedJobMemory) {
            this.assignedJobMemory += assignedJobMemory;
            return this;
        }

        public Builder incNumAllocatingJobs() {
            ++this.numAllocatingJobs;
            return this;
        }

        void addTask(String taskName, String taskId, boolean isAllocating, MlMemoryTracker memoryTracker) {
            ++numAssignedJobs;
            if (isAllocating) {
                ++numAllocatingJobs;
            }
            Long jobMemoryRequirement = memoryTracker.getJobMemoryRequirement(taskName, taskId);
            if (jobMemoryRequirement == null) {
                useMemory = false;
                logger.debug(() -> new ParameterizedMessage(
                    "[{}] memory requirement was not available. Calculating load by number of assigned jobs.",
                    taskId
                ));
            } else {
                assignedJobMemory += jobMemoryRequirement;
            }
        }

        public NodeLoad build() {
            return new NodeLoad(maxMemory,
            maxJobs,
            nodeId,
            useMemory,
            error,
            numAssignedJobs,
            assignedJobMemory,
            numAllocatingJobs);
        }
    }
}
