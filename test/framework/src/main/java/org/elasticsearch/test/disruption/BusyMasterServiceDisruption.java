/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.disruption;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;
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
        clusterService.getMasterService().submitUnbatchedStateUpdateTask("service_disruption_block", new ClusterStateUpdateTask(priority) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                if (active.get()) {
                    submitTask(clusterService);
                }
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("unexpected error during disruption", e);
            }
        });
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
