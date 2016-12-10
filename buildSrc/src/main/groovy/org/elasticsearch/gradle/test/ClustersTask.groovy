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

package org.elasticsearch.gradle.test

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input

/** task for forming clusters and track cluster sub-task dependency */
class ClustersTask extends DefaultTask {
    List<ClusterTask> clusterTasks = new ArrayList<>()

    ClustersTask() {
        description = "create a list of cluster"
    }

    @Input
    public void cluster(Closure closure) {
        ClusterTask clusterTask = project.tasks.create('cluster'+ clusterTasks.size(), ClusterTask.class)
        clusterTask.cluster(closure)
        clusterTasks.add(clusterTask)
        dependsOn(clusterTask)
    }

    public List<NodeInfo> getNodes(String clusterName) {
        for (ClusterTask clusterTask: clusterTasks) {
            if (clusterTask.clusterConfiguration.clusterName.equals(clusterName)) {
                return clusterTask.getNodes()
            }
        }
        throw new IllegalArgumentException("no cluster named ${clusterName} was configured")
    }

    public String[] getFinalizedTasks() {
        List<String> finalizedTaskNames = new ArrayList<>()
        for (ClusterTask clusterTask: clusterTasks) {
            finalizedTaskNames.addAll(clusterTask.finalizedTaskNames)
        }
        return finalizedTaskNames.stream().toArray();
    }
}

