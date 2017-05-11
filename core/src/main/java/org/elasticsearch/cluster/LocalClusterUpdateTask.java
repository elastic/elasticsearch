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
 * Used to apply state updates on nodes that are not necessarily master
 */
public abstract class LocalClusterUpdateTask implements ClusterStateTaskConfig, ClusterStateTaskExecutor<LocalClusterUpdateTask>,
    ClusterStateTaskListener {

    private final Priority priority;

    public LocalClusterUpdateTask() {
        this(Priority.NORMAL);
    }

    public LocalClusterUpdateTask(Priority priority) {
        this.priority = priority;
    }

    public abstract ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) throws Exception;

    @Override
    public final ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState,
                                                                    List<LocalClusterUpdateTask> tasks) throws Exception {
        assert tasks.size() == 1 && tasks.get(0) == this : "expected one-element task list containing current object but was " + tasks;
        ClusterTasksResult<LocalClusterUpdateTask> result = execute(currentState);
        return ClusterTasksResult.<LocalClusterUpdateTask>builder().successes(tasks).build(result, currentState);
    }

    /**
     * no changes were made to the cluster state. Useful to execute a runnable on the cluster state applier thread
     */
    public static ClusterTasksResult<LocalClusterUpdateTask> unchanged() {
        return new ClusterTasksResult<>(null, null);
    }

    @Override
    public String describeTasks(List<LocalClusterUpdateTask> tasks) {
        return ""; // one of task, source is enough
    }

    @Nullable
    public TimeValue timeout() {
        return null;
    }

    @Override
    public Priority priority() {
        return priority;
    }

    @Override
    public final boolean runOnlyOnMaster() {
        return false;
    }
}
