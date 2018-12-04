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
package org.elasticsearch.test.disruption;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.InternalTestCluster;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class BusyMasterServiceDisruption extends SingleNodeDisruption {
    private final AtomicBoolean active = new AtomicBoolean();
    private final Priority priority;

    public BusyMasterServiceDisruption(Random random, Priority priority) {
        super(random);
        this.priority = priority;
    }

    @Override
    public void startDisrupting() {
        disruptedNode = cluster.getMasterName();
        final String disruptionNodeCopy = disruptedNode;
        if (disruptionNodeCopy == null) {
            return;
        }
        ClusterService clusterService = cluster.getInstance(ClusterService.class, disruptionNodeCopy);
        if (clusterService == null) {
            return;
        }
        logger.info("making master service busy on node [{}] at priority [{}]", disruptionNodeCopy, priority);
        active.set(true);
        submitTask(clusterService);
    }

    private void submitTask(ClusterService clusterService) {
        clusterService.getMasterService().submitStateUpdateTask(
            "service_disruption_block",
            new ClusterStateUpdateTask(priority) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (active.get()) {
                        submitTask(clusterService);
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("unexpected error during disruption", e);
                }
            }
        );
    }

    @Override
    public void stopDisrupting() {
        active.set(false);
    }

    @Override
    public void removeAndEnsureHealthy(InternalTestCluster cluster) {
        removeFromCluster(cluster);
    }

    @Override
    public TimeValue expectedTimeToHeal() {
        return TimeValue.timeValueMinutes(0);
    }
}
