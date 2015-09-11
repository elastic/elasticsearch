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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
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

    /** Create a fake NodeStats for the given node and usage */
    public static NodeStats makeStats(String nodeName, DiskUsage usage) {
        FsInfo.Path[] paths = new FsInfo.Path[1];
        FsInfo.Path path = new FsInfo.Path("/dev/null", null,
            usage.getTotalBytes(), usage.getFreeBytes(), usage.getFreeBytes());
        paths[0] = path;
        FsInfo fsInfo = new FsInfo(System.currentTimeMillis(), paths);
        return new NodeStats(new DiscoveryNode(nodeName, DummyTransportAddress.INSTANCE, Version.CURRENT),
            System.currentTimeMillis(),
            null, null, null, null, null,
            fsInfo,
            null, null, null,
            null);
    }

    @Inject
    public MockInternalClusterInfoService(Settings settings, NodeSettingsService nodeSettingsService,
                                          TransportNodesStatsAction transportNodesStatsAction,
                                          TransportIndicesStatsAction transportIndicesStatsAction,
                                          ClusterService clusterService, ThreadPool threadPool) {
        super(settings, nodeSettingsService, transportNodesStatsAction, transportIndicesStatsAction, clusterService, threadPool);
        this.clusterName = ClusterName.clusterNameFromSettings(settings);
        stats[0] = makeStats("node_t1", new DiskUsage("node_t1", "n1", "/dev/null", 100, 100));
        stats[1] = makeStats("node_t2", new DiskUsage("node_t2", "n2", "/dev/null", 100, 100));
        stats[2] = makeStats("node_t3", new DiskUsage("node_t3", "n3", "/dev/null", 100, 100));
    }

    public void setN1Usage(String nodeName, DiskUsage newUsage) {
        stats[0] = makeStats(nodeName, newUsage);
    }

    public void setN2Usage(String nodeName, DiskUsage newUsage) {
        stats[1] = makeStats(nodeName, newUsage);
    }

    public void setN3Usage(String nodeName, DiskUsage newUsage) {
        stats[2] = makeStats(nodeName, newUsage);
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

    public ClusterInfo getClusterInfo() {
        ClusterInfo clusterInfo = super.getClusterInfo();
        return new ClusterInfo(clusterInfo.getNodeLeastAvailableDiskUsages(), clusterInfo.getNodeMostAvailableDiskUsages(), clusterInfo.shardSizes, DEV_NULL_MAP);
    }

    public static final Map<ShardRouting, String> DEV_NULL_MAP = Collections.unmodifiableMap(new StaticValueMap("/dev/null"));

    // a test only map that always returns the same value no matter what key is passed
    private static final class StaticValueMap extends AbstractMap<ShardRouting, String> {

        private final String value;

        private StaticValueMap(String value) {
            this.value = value;
        }

        @Override
        public String get(Object key) {
            return value;
        }

        @Override
        public Set<Entry<ShardRouting, String>> entrySet() {
            throw new UnsupportedOperationException("this is a test-only map that only supports #get(Object key)");
        }
    }
}
