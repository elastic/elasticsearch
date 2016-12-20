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
import org.gradle.util.ConfigureUtil

/** task for forming a cluster and tracking sub-task dependency */
class ClusterTask extends DefaultTask {
    ClusterConfiguration clusterConfiguration
    List<NodeInfo> nodes

    ClusterTask() {
        clusterConfiguration = new ClusterConfiguration(project)
        project.gradle.projectsEvaluated {
            nodes = ClusterFormationTasks.setup(project, this, clusterConfiguration)
        }
    }

    @Input
    public void cluster(Closure closure) {
        ConfigureUtil.configure(closure, clusterConfiguration)
    }

    public List<NodeInfo> getNodes() {
        return nodes
    }

    public String[] getFinalizedTaskNames() {
        List<String> finalizedTaskNames = new ArrayList<>();
        if (clusterConfiguration.numNodes > 1) {
            for (int i = 0; i < clusterConfiguration.numNodes; i++) {
                finalizedTaskNames.add("${name}#node${i}.stop")
            }
        } else {
            finalizedTaskNames.add("${name}#stop")
        }
        return finalizedTaskNames.stream().toArray();
    }
}

