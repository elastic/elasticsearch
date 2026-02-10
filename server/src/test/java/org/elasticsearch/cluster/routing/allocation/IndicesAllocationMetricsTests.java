/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;

public class IndicesAllocationMetricsTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private MeterRegistry meterRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("indices-allocation-metrics");
        clusterService = new ClusterService(Settings.EMPTY, ClusterSettings.createBuiltInClusterSettings(), threadPool, null);
        meterRegistry = MeterRegistry.NOOP;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testStartAndStopScheduledTask() {
        final var taskRuns = new CountDownLatch(between(1, 10));
        final var noopMetricsCompute = new IndicesAllocationMetrics(
            clusterService,
            meterRegistry,
            threadPool,
            TimeValue.timeValueMillis(10)
        ) {
            @Override
            Runnable computeAndPublishTask() {
                return taskRuns::countDown;
            }
        };

        final var node = DiscoveryNodeUtils.create(randomIdentifier("node-"));
        noopMetricsCompute.clusterChanged(becomeMaster(node));
        safeAwait(taskRuns);
        noopMetricsCompute.clusterChanged(becomeNonMaster(node));
        assertFalse(noopMetricsCompute.isRunning());
    }

    ClusterState oneMasterNode(DiscoveryNode node) {
        return ClusterStateCreationUtils.state(node, node, new DiscoveryNode[] { node });
    }

    ClusterState oneNonMasterNode(DiscoveryNode node) {
        return ClusterStateCreationUtils.state(node, null, new DiscoveryNode[] { node });
    }

    ClusterChangedEvent becomeMaster(DiscoveryNode node) {
        return new ClusterChangedEvent("become-master", oneMasterNode(node), oneNonMasterNode(node));
    }

    ClusterChangedEvent becomeNonMaster(DiscoveryNode node) {
        return new ClusterChangedEvent("become-non-master", oneNonMasterNode(node), oneMasterNode(node));
    }

}
