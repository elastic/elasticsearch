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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.cluster.routing.allocation.decider.MockDiskUsagesIT;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;

/**
 * Fake ClusterInfoService class that allows updating the nodes stats disk
 * usage with fake values
 */
public class MockInternalClusterInfoService extends InternalClusterInfoService {

    public static class TestPlugin extends Plugin {
        @Override
        public String name() {
            return "mock-cluster-info-service";
        }
        @Override
        public String description() {
            return "a mock cluster info service for testing";
        }
        public void onModule(ClusterModule module) {
            module.clusterInfoServiceImpl = MockInternalClusterInfoService.class;
        }
    }

    private final ClusterName clusterName;
    private volatile NodeStats[] stats = new NodeStats[3];

    @Inject
    public MockInternalClusterInfoService(Settings settings, NodeSettingsService nodeSettingsService,
                                          TransportNodesStatsAction transportNodesStatsAction,
                                          TransportIndicesStatsAction transportIndicesStatsAction,
                                          ClusterService clusterService, ThreadPool threadPool) {
        super(settings, nodeSettingsService, transportNodesStatsAction, transportIndicesStatsAction, clusterService, threadPool);
        this.clusterName = ClusterName.clusterNameFromSettings(settings);
        stats[0] = MockDiskUsagesIT.makeStats("node_t1", new DiskUsage("node_t1", "n1", 100, 100));
        stats[1] = MockDiskUsagesIT.makeStats("node_t2", new DiskUsage("node_t2", "n2", 100, 100));
        stats[2] = MockDiskUsagesIT.makeStats("node_t3", new DiskUsage("node_t3", "n3", 100, 100));
    }

    public void setN1Usage(String nodeName, DiskUsage newUsage) {
        stats[0] = MockDiskUsagesIT.makeStats(nodeName, newUsage);
    }

    public void setN2Usage(String nodeName, DiskUsage newUsage) {
        stats[1] = MockDiskUsagesIT.makeStats(nodeName, newUsage);
    }

    public void setN3Usage(String nodeName, DiskUsage newUsage) {
        stats[2] = MockDiskUsagesIT.makeStats(nodeName, newUsage);
    }

    @Override
    public CountDownLatch updateNodeStats(final ActionListener<NodesStatsResponse> listener) {
        NodesStatsResponse response = new NodesStatsResponse(clusterName, stats);
        listener.onResponse(response);
        return new CountDownLatch(0);
    }

    @Override
    public CountDownLatch updateIndicesStats(final ActionListener<IndicesStatsResponse> listener) {
        // Not used, so noop
        return new CountDownLatch(0);
    }
}
