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
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClusterFormationTaskExtension {

    private final Task task;

    private final List<ElasticsearchConfiguration> claimedClusters = new ArrayList<>();

    private final Logger logger =  Logging.getLogger(ClusterFormationTaskExtension.class);

    public ClusterFormationTaskExtension(Task task) {
        this.task = task;
    }

    public void call(ElasticsearchConfiguration cluster) {
        // not likely to configure the same task from multiple threads as of Gradle 4.7, but it's the right thing to do
        synchronized (claimedClusters) {
            if (claimedClusters.contains(cluster)) {
                logger.warn("{} already claimed cluster {} will not claim it again",
                    task.getPath(), cluster.getName()
                );
                return;
            }
            claimedClusters.add(cluster);
        }
        logger.info("CF: the {} task will use cluster: {}", task.getName(), cluster.getName());
    }

    public List<ElasticsearchConfiguration> getClaimedClusters() {
        synchronized (claimedClusters) {
            return Collections.unmodifiableList(claimedClusters);
        }
    }

    static ClusterFormationTaskExtension getForTask(Task task) {
        return task.getExtensions().getByType(ClusterFormationTaskExtension.class);
    }
}
