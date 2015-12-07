/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;

/**
 * A task that can update the cluster state.
 */
abstract public class ClusterStateUpdateTask extends ClusterStateTaskExecutor<ClusterStateUpdateTask> implements ClusterStateTaskConfig, ClusterStateTaskListener {

    final private Priority priority;

    public ClusterStateUpdateTask() {
        this(Priority.NORMAL);
    }

    public ClusterStateUpdateTask(Priority priority) {
        this.priority = priority;
    }

    @Override
    final public BatchResult<ClusterStateUpdateTask> execute(ClusterState currentState, List<ClusterStateUpdateTask> tasks) throws Exception {
        ClusterState result = execute(currentState);
        return BatchResult.<ClusterStateUpdateTask>builder().successes(tasks).build(result);
    }

    /**
     * Update the cluster state based on the current state. Return the *same instance* if no state
     * should be changed.
     */
    abstract public ClusterState execute(ClusterState currentState) throws Exception;

    /**
     * A callback called when execute fails.
     */
    abstract public void onFailure(String source, Throwable t);

    @Override
    public void onNoLongerMaster(String source) {
        onFailure(source, new NotMasterException("no longer master. source: [" + source + "]"));
    }

    @Override
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

    }

    /**
     * If the cluster state update task wasn't processed by the provided timeout, call
     * {@link #onFailure(String, Throwable)}. May return null to indicate no timeout is needed (default).
     */
    @Nullable
    public TimeValue timeout() {
        return null;
    }

    @Override
    public Priority priority() {
        return priority;
    }
}
