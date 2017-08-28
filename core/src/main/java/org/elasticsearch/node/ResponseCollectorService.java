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

package org.elasticsearch.node;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Collects statistics about queue size, response time, and service time of
 * tasks executed on each node, making the EWMA of the values available to the
 * coordinating node.
 */
public final class ResponseCollectorService extends AbstractComponent implements ClusterStateListener {

    private static final double ALPHA = 0.3;

    private final ConcurrentMap<String, NodeStatistics> nodeIdToStats = ConcurrentCollections.newConcurrentMap();

    public ResponseCollectorService(Settings settings, ClusterService clusterService) {
        super(settings);
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                removeNode(removedNode.getId());
            }
        }
    }

    void removeNode(String nodeId) {
        nodeIdToStats.remove(nodeId);
    }

    public void addNodeStatistics(String nodeId, int queueSize, long responseTimeNanos, long avgServiceTimeNanos) {
        NodeStatistics nodeStats = nodeIdToStats.get(nodeId);
        nodeIdToStats.compute(nodeId, (id, ns) -> {
            if (ns == null) {
                ExponentiallyWeightedMovingAverage queueEWMA = new ExponentiallyWeightedMovingAverage(ALPHA, queueSize);
                ExponentiallyWeightedMovingAverage responseEWMA = new ExponentiallyWeightedMovingAverage(ALPHA, responseTimeNanos);
                NodeStatistics newStats = new NodeStatistics(nodeId, queueEWMA, responseEWMA, avgServiceTimeNanos);
                return newStats;
            } else {
                ns.queueSize.addValue((double) queueSize);
                ns.responseTime.addValue((double) responseTimeNanos);
                ns.serviceTime = avgServiceTimeNanos;
                return ns;
            }
        });
    }

    public Map<String, ComputedNodeStats> getAllNodeStatistics() {
        // Transform the mutable object internally used for accounting into the computed version
        Map<String, ComputedNodeStats> nodeStats = new HashMap<>(nodeIdToStats.size());
        nodeIdToStats.forEach((k, v) -> {
            nodeStats.put(k, new ComputedNodeStats(v));
        });
        return nodeStats;
    }

    /**
     * Struct-like class encapsulating a point-in-time snapshot of a particular
     * node's statistics. This includes the EWMA of queue size, response time,
     * and service time.
     */
    public static class ComputedNodeStats {
        public final String nodeId;
        public final double queueSize;
        public final double responseTime;
        public final double serviceTime;

        ComputedNodeStats(NodeStatistics nodeStats) {
            this.nodeId = nodeStats.nodeId;
            this.queueSize = nodeStats.queueSize.getAverage();
            this.responseTime = nodeStats.responseTime.getAverage();
            this.serviceTime = nodeStats.serviceTime;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ComputedNodeStats[");
            sb.append(nodeId).append("](");
            sb.append("queue: ").append(queueSize);
            sb.append(", response time: ").append(responseTime);
            sb.append(", service time: ").append(serviceTime);
            sb.append(")");
            return sb.toString();
        }
    }

    /**
     * Class encapsulating a node's exponentially weighted queue size, response
     * time, and service time, however, this class is private and intended only
     * to be used for the internal accounting of {@code ResponseCollectorService}.
     */
    private static class NodeStatistics {
        final String nodeId;
        final ExponentiallyWeightedMovingAverage queueSize;
        final ExponentiallyWeightedMovingAverage responseTime;
        double serviceTime;

        NodeStatistics(String nodeId,
                       ExponentiallyWeightedMovingAverage queueSizeEWMA,
                       ExponentiallyWeightedMovingAverage responseTimeEWMA,
                       double serviceTimeEWMA) {
            this.nodeId = nodeId;
            this.queueSize = queueSizeEWMA;
            this.responseTime = responseTimeEWMA;
            this.serviceTime = serviceTimeEWMA;
        }
    }
}
