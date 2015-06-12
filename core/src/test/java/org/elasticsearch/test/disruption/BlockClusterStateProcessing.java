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

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateNonMasterUpdateTask;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class BlockClusterStateProcessing extends SingleNodeDisruption {

    AtomicReference<CountDownLatch> disruptionLatch = new AtomicReference<>();


    public BlockClusterStateProcessing(Random random) {
        this(null, random);
    }

    public BlockClusterStateProcessing(String disruptedNode, Random random) {
        super(random);
        this.disruptedNode = disruptedNode;
    }


    @Override
    public void startDisrupting() {
        final String disruptionNodeCopy = disruptedNode;
        if (disruptionNodeCopy == null) {
            return;
        }
        ClusterService clusterService = cluster.getInstance(ClusterService.class, disruptionNodeCopy);
        if (clusterService == null) {
            return;
        }
        logger.info("delaying cluster state updates on node [{}]", disruptionNodeCopy);
        boolean success = disruptionLatch.compareAndSet(null, new CountDownLatch(1));
        assert success : "startDisrupting called without waiting on stopDistrupting to complete";
        final CountDownLatch started = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("service_disruption_block", Priority.IMMEDIATE, new ClusterStateNonMasterUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                started.countDown();
                CountDownLatch latch = disruptionLatch.get();
                if (latch != null) {
                    latch.await();
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("unexpected error during disruption", t);
            }
        });
        try {
            started.await();
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void stopDisrupting() {
        CountDownLatch latch = disruptionLatch.get();
        if (latch != null) {
            latch.countDown();
        }

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
