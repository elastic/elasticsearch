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
package org.elasticsearch.gradle.clusterformation;

import org.gradle.api.Task;
import org.gradle.api.execution.TaskActionListener;
import org.gradle.api.execution.TaskExecutionListener;
import org.gradle.api.tasks.TaskState;

public class ClusterFormationTaskExecutionListener implements TaskExecutionListener, TaskActionListener {
    @Override
    public void afterExecute(Task task, TaskState state) {
        // always unclaim the cluster, even if _this_ task is up-to-date, as others might not have been and caused the
        // cluster to start.
        ClusterFormationTaskExtension.getForTask(task).getClaimedClusters().forEach(ElasticsearchConfiguration::unClaimAndStop);
    }

    @Override
    public void beforeActions(Task task) {
        // we only start the cluster before the actions, so we'll not start it if the task is up-to-date
        ClusterFormationTaskExtension.getForTask(task).getClaimedClusters().forEach(ElasticsearchConfiguration::start);
    }

    @Override
    public void beforeExecute(Task task) {
    }

    @Override
    public void afterActions(Task task) {
    }
}
