/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class NodeUsageStatsForThreadPoolsMonitorTests extends ESAllocationTestCase {

    public void testMonitor() {
        AtomicLong currentTime = new AtomicLong();
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).build();// NOMERGE: make sensible cluster state
        AtomicBoolean reroute = new AtomicBoolean(false);
        var nodeUsageStatsForThreadPoolsMonitor = new NodeUsageStatsForThreadPoolsMonitor(
            // NOMERGE: figure out the appropriate settings.
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            currentTime::get,
            () -> clusterState,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            }
        );

    }

    private static ClusterInfo clusterInfo(Map<String, NodeUsageStatsForThreadPools> statsPerThreadPoolPerNode) {
        return ClusterInfo.builder().nodeUsageStatsForThreadPools(statsPerThreadPoolPerNode).build();
    }
}
