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

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.monitor.fs.FsHealthService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.TransportService;

import java.util.Set;
import java.util.function.Supplier;

public class NodeFsHealthChecker {

    private static final Logger logger = LogManager.getLogger(FollowersChecker.class);

    public static final Setting<TimeValue> FS_HEALTH_CHECK_INTERVAL_SETTING =
        Setting.timeSetting("cluster.fault_detection.fs_health_check.interval",
            TimeValue.timeValueMillis(5000), TimeValue.timeValueMillis(100), Setting.Property.NodeScope);

    private final TimeValue fsHealthCheckInterval;
    private final boolean fsHealthCheckEnabled;
    private final TriConsumer<DiscoveryNode, String, Supplier<Boolean>> failFollower;
    private final TransportService transportService;
    private final Supplier<Set<DiscoveryNode>> followerNodesSupplier;
    private final Client nodeClient;

    public NodeFsHealthChecker(Settings settings, TransportService transportService, Client nodeClient, TriConsumer<DiscoveryNode,
        String, Supplier<Boolean>> failFollower, Supplier<Set<DiscoveryNode>> followerNodesSupplier){
        fsHealthCheckEnabled = FsHealthService.ENABLED_SETTING.get(settings);
        fsHealthCheckInterval = FS_HEALTH_CHECK_INTERVAL_SETTING.get(settings);
        this.followerNodesSupplier = followerNodesSupplier;
        this.failFollower = failFollower;
        this.transportService = transportService;
        this.nodeClient = nodeClient;
    }


    void start() {
        handleWakeUp();
    }


    private void scheduleNextWakeUp() {
        if (followerNodesSupplier.get().isEmpty() == false) {
            transportService.getThreadPool().schedule(new Runnable() {
                @Override
                public void run() {
                    handleWakeUp();
                }

                @Override
                public String toString() {
                    return NodeFsHealthChecker.this + "::handleWakeUp";
                }
            }, fsHealthCheckInterval, ThreadPool.Names.SAME);
        }
    }


    private void handleWakeUp() {
        if (fsHealthCheckEnabled && followerNodesSupplier.get().isEmpty() == false) {
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest().clear().fs(true).timeout(fsHealthCheckInterval);
            NodesStatsResponse nodesStatsResponse = fetchNodeStats(nodesStatsRequest);
            if(nodesStatsResponse == null){
                return;
            }
            for (NodeStats nodeStats : nodesStatsResponse.getNodes()) {
                if (nodeStats.getFs() == null) {
                    logger.warn("Unable to retrieve node FS stats for {}", nodeStats.getNode().getName());
                } else {
                    if (nodeStats.getFs().getTotal().isWritable() == Boolean.FALSE) {
                        failFollower.apply(nodeStats.getNode(), "read-only-file-system", () ->
                            followerNodesSupplier.get().contains(nodeStats.getNode()));
                    }
                }
            }
        }
        scheduleNextWakeUp();
    }

    private NodesStatsResponse fetchNodeStats(NodesStatsRequest nodeStatsRequest) {
        NodesStatsResponse nodesStatsResponse = null;
        try {
            nodesStatsResponse = nodeClient.admin().cluster().nodesStats(nodeStatsRequest).actionGet();
        } catch (Exception e){
            if (e instanceof ReceiveTimeoutTransportException) {
                logger.error("NodeStatsRequest timed out for FollowerChecker", e);
            } else {
                if (e instanceof ClusterBlockException) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Failed to execute NodeStatsRequest for FollowerChecker", e);
                    }
                } else {
                    logger.warn("Failed to execute NodeStatsRequest for FollowerChecker", e);
                }
            }
        }
        return nodesStatsResponse;
    }
}

